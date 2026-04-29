"""
This module handles all view logic for pod_manager.
Refactored for SRP, DRY, and Multi-Tenant Architecture.
"""
import logging
import warnings
import hashlib
import hmac
import json
import asyncio
import threading
import time
import urllib.parse
import re
import html
import recurly
from email.utils import format_datetime

import requests
from django.conf import settings
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.core.paginator import Paginator
from django.core.signing import TimestampSigner, SignatureExpired, BadSignature
from django.db import transaction
from django.db.models import Q, F, Case, When, CharField, Max, Count
from django.db.models.functions import Substr, Lower
from django.http import JsonResponse, HttpResponse, HttpResponseForbidden, StreamingHttpResponse, Http404, HttpResponseRedirect, HttpResponseNotModified
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from datetime import timedelta
from django.views.decorators.csrf import csrf_exempt

from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media, Person
from lxml import etree

from .models import PatronProfile, NetworkMembership, Podcast, Episode, Network, PatreonTier, UserMix, NetworkMix, EpisodeEditSuggestion
from .tasks import task_ingest_feed, task_rebuild_episode_fragments, task_rebuild_podcast_fragments, task_send_magic_link

warnings.filterwarnings("ignore", message=".*Image URL must end with.*")
warnings.filterwarnings("ignore", message=".*Size is set to 0.*")

logger = logging.getLogger(__name__)

# ==========================================
# 1. DOMAIN SERVICES (SRP)
# ==========================================

def _evaluate_access(user, podcast, network=None):
    if not user.is_authenticated:
        return False, False
        
    net = network or podcast.network
    is_owner = net.owners.filter(id=user.id).exists()
    if is_owner:
        return True, True

    membership = NetworkMembership.objects.filter(user=user, network=net).first()
    if not membership:
        return False, False

    # 1. Base check: Is this a free podcast?
    if not podcast.required_tier:
        return True, False
        
    req_cents = podcast.required_tier.minimum_cents
    if req_cents == 0:
        return True, False

    # --- 2. THE PATREON CHECK ---
    patreon_access = membership.is_active_patron and (membership.patreon_pledge_cents >= req_cents)
    
    # --- 3. THE RECURLY CHECK ---
    recurly_access = False
    required_plan = podcast.required_tier.recurly_plan_code
    
    if required_plan and membership.active_recurly_plans:
        # Check if the required Recurly plan code exists in the user's JSON array
        if required_plan in membership.active_recurly_plans:
            recurly_access = True

    # 4. The Final Verdict
    has_access = patreon_access or recurly_access
    
    return has_access, False

def _build_episode_description(episode, has_access):
    desc = episode.clean_description or episode.raw_description
    footer_parts = []
    
    if has_access:
        if episode.podcast.show_footer_private: footer_parts.append(episode.podcast.show_footer_private)
        if episode.podcast.network.global_footer_private: footer_parts.append(episode.podcast.network.global_footer_private)
    else:
        if episode.podcast.show_footer_public: footer_parts.append(episode.podcast.show_footer_public)
        if episode.podcast.network.global_footer_public: footer_parts.append(episode.podcast.network.global_footer_public)

    if footer_parts:
        desc += "<br><br>" + "<br><br>".join(footer_parts)
    return desc

def parse_duration(duration_str: str) -> timedelta | None:
    if not duration_str: return None
    try:
        parts = duration_str.split(':')
        sec = int(float(parts[-1]))
        if len(parts) == 3: return timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=sec)
        elif len(parts) == 2: return timedelta(minutes=int(parts[0]), seconds=sec)
        return timedelta(seconds=int(float(duration_str)))
    except ValueError:
        return None

def process_mix_image_url(image_url, mix_instance):
    if not image_url: return None
    try:
        res = requests.get(image_url, timeout=5) 
        if res.status_code == 200:
            import os
            temp_name = os.path.basename(image_url).split('?')[0] or "cover.jpg"
            mix_instance.image_upload.save(temp_name, ContentFile(res.content), save=False)
            mix_instance.image_url = "" 
            return None 
        return f"Server returned status {res.status_code}."
    except requests.exceptions.RequestException:
        return "URL invalid or unreachable."

# ==========================================
# 2. DRY RSS FEED GENERATOR
# ==========================================

class RSSFeedBuilder:
    def __init__(self, base_url, title, description, image_url, network, feed_type='private'):
        # Strip trailing slashes to prevent double-slashing when concatenating URLs
        self.base_url = base_url.rstrip('/') 
        self.feed_type = feed_type
        self.network = network
        self.episodes_data = [] 
        
        safe_description = description or network.summary or f"{title} on {network.name}."
        
        self.feed = PodgenPodcast(
            name=title,
            description=safe_description,
            website=network.website_url or self.base_url,
            explicit=True,
            image=image_url or network.default_image_url or "https://example.com/logo.png",
            authors=[Person(name=network.name, email=network.contact_email or "hosts@example.com")],
            owner=Person(name=network.name, email=network.contact_email or "hosts@example.com"),
            withhold_from_itunes=True,
        )

    def add_episode(self, episode, has_access, display_title=None):
        desc = _build_episode_description(episode, has_access)
        self.episodes_data.append(episode)
        
        # Build the URL internally with the universal placeholder
        target_audio_url = f"{self.base_url}{reverse('play_episode', args=[episode.id])}?auth=__VECTO_AUTH_TOKEN__"
        
        self.feed.episodes.append(PodgenEpisode(
            id=episode.guid_public or episode.guid_private or str(episode.id),
            title=display_title or episode.title,
            summary=desc, 
            publication_date=episode.pub_date,
            media=Media(
                url=target_audio_url, size=0, type="audio/mpeg", 
                duration=parse_duration(episode.duration)
            )
        ))

    def render(self, access_map=None):
        raw_xml = self.feed.rss_str()

        if 'xmlns:podcast=' not in raw_xml:
            raw_xml = raw_xml.replace('<rss ', '<rss xmlns:podcast="https://podcastindex.org/namespace/1.0" ', 1)
        
        tag_map = {str(ep.guid_public or ep.guid_private or ep.id): ep for ep in self.episodes_data}
        if not tag_map: return raw_xml

        root = etree.fromstring(raw_xml.encode('utf-8'))
        podcast_ns = "https://podcastindex.org/namespace/1.0"
        etree.register_namespace('podcast', podcast_ns)

        for item in root.findall('.//item'):
            guid_elem = item.find('guid')
            if guid_elem is not None and guid_elem.text in tag_map:
                ep = tag_map[guid_elem.text]
                
                for tag in ep.tags:
                    cat_elem = etree.SubElement(item, 'category')
                    cat_elem.text = etree.CDATA(str(tag))
                
                ep_access = access_map.get(ep.podcast_id, False) if access_map else (self.feed_type == 'private')
                ftype = 'private' if ep_access else 'public'
                
                if ep.chapters_private or ep.chapters_public:
                    # Swap request object for base_url
                    chapter_url = f"{self.base_url}{reverse('episode_chapters', args=[ep.id, ftype])}"
                    chap_elem = etree.SubElement(item, f'{{{podcast_ns}}}chapters')
                    chap_elem.set('url', chapter_url)
                    
                    # Updated to the official Podcasting 2.0 MIME type requirement
                    chap_elem.set('type', 'application/json+chapters')
                    
        # 1. Convert the lxml tree back into a raw string
        final_xml = etree.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
        
        # 2. Strip the inline namespace lxml tried to force onto the child tags
        final_xml = final_xml.replace(f' xmlns:podcast="{podcast_ns}"', '')
        
        # 3. Force it into the root <rss> tag exactly where PocketCasts expects it
        if 'xmlns:podcast' not in final_xml:
            final_xml = final_xml.replace('<rss ', f'<rss xmlns:podcast="{podcast_ns}" ', 1)
            
        return final_xml
    
# ==========================================
# 3. PATREON SYNC ENGINE (MULTI-TENANT)
# ==========================================

def _sync_patron_profile(user, user_data, included_data, current_network=None):
    logger.info(f"[_sync_patron_profile] Starting sync for user: {user.email} (ID: {user.id})")
    
    patreon_id = user_data.get('id')
    attributes = user_data.get('attributes', {})
    logger.debug(f"[_sync_patron_profile] Extracted Patreon ID: {patreon_id}")
    
    # 1. Update Global Profile
    profile, created = PatronProfile.objects.get_or_create(user=user, defaults={'patreon_id': patreon_id})
    logger.debug(f"[_sync_patron_profile] PatronProfile created: {created}")
    
    profile.profile_image_url = attributes.get('image_url')
    socials = attributes.get('social_connections', {}) or {}
    discord_info = socials.get('discord') or {}
    profile.discord_id = discord_info.get('user_id') if discord_info else None
    profile.last_active = timezone.now()
    if profile.patreon_id != patreon_id: 
        profile.patreon_id = patreon_id
    profile.save()
    logger.info("[_sync_patron_profile] Global profile saved successfully.")

    # Even if they pay $0, they are now a registered free listener on this network.
    if current_network:
        _, mem_created = NetworkMembership.objects.get_or_create(user=user, network=current_network)
        logger.info(f"[_sync_patron_profile] Default NetworkMembership for '{current_network.name}' ensured (Created: {mem_created}).")

    # FIX INCLUDED: Exclude empty strings to prevent dictionary overwrite bugs!
    known_campaigns = {str(n.patreon_campaign_id): n for n in Network.objects.exclude(patreon_campaign_id__isnull=True).exclude(patreon_campaign_id__exact='')}
    logger.info(f"[_sync_patron_profile] Known campaigns loaded: {list(known_campaigns.keys())}")
    
    seen_campaigns = set()
    logger.debug(f"[_sync_patron_profile] Scanning {len(included_data)} items in included_data...")
    
    for item in included_data:
        if item.get('type') == 'member':
            attrs = item.get('attributes', {})
            campaign_data = item.get('relationships', {}).get('campaign', {}).get('data', {})
            
            logger.debug(f"[_sync_patron_profile] Found 'member' item. Status: '{attrs.get('patron_status')}', Cents: {attrs.get('currently_entitled_amount_cents')}")
            
            if campaign_data:
                campaign_id = str(campaign_data.get('id'))
                logger.debug(f"[_sync_patron_profile] Member item is attached to Campaign ID: {campaign_id}")
                
                if campaign_id in known_campaigns:
                    seen_campaigns.add(campaign_id)
                    network = known_campaigns[campaign_id]
                    logger.info(f"[_sync_patron_profile] Campaign MATCH! Mapping to Network: '{network.name}'")
                    
                    membership, mem_created = NetworkMembership.objects.get_or_create(user=user, network=network)
                    
                    if attrs.get('patron_status') == 'active_patron':
                        cents = attrs.get('currently_entitled_amount_cents', 0)
                        logger.info(f"[_sync_patron_profile] Applying ACTIVE pledge to '{network.name}': {cents} cents.")
                        membership.patreon_pledge_cents = cents
                        membership.is_active_patron = True
                        
                        start_date_str = attrs.get('pledge_relationship_start')
                        if start_date_str:
                            try:
                                membership.patreon_join_date = timezone.datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                            except Exception as e: 
                                logger.error(f"[_sync_patron_profile] Date parsing failed: {e}")
                    else:
                        logger.info(f"[_sync_patron_profile] Patron status is '{attrs.get('patron_status')}'. Setting {network.name} pledge to 0.")
                        membership.patreon_pledge_cents = 0
                        membership.is_active_patron = False
                        
                    membership.save()
                else:
                    logger.warning(f"[_sync_patron_profile] Ignoring member item: Campaign ID '{campaign_id}' is NOT in our database.")
            else:
                logger.warning("[_sync_patron_profile] Ignoring member item: No campaign relationship data found.")

    logger.info(f"[_sync_patron_profile] Checking for stale memberships. Seen campaigns: {list(seen_campaigns)}")
    
    # Check if the user has any active memberships that were NOT confirmed in this API response
    for mem in NetworkMembership.objects.filter(user=user, is_active_patron=True):
        camp_id = str(mem.network.patreon_campaign_id)
        if camp_id not in seen_campaigns:
            logger.info(f"[_sync_patron_profile] REVOKING stale membership for '{mem.network.name}' (Campaign ID '{camp_id}' was not in API response).")
            mem.is_active_patron = False
            mem.patreon_pledge_cents = 0
            mem.save()

    logger.info("[_sync_patron_profile] Sync complete.")
    return profile

def sync_network_patrons(network):
    logger.debug(f"--- Starting COMPLETE Sync for Network: {network.name} ---")
    if not network.patreon_creator_access_token or not network.patreon_campaign_id:
        return 0, "Network is not properly linked to Patreon."

    campaign_id_str = str(network.patreon_campaign_id)
    base_url = f"https://www.patreon.com/api/oauth2/v2/campaigns/{campaign_id_str}/members"
    params = {"include": "user", "fields[member]": "patron_status,currently_entitled_amount_cents", "fields[user]": "email", "page[count]": 100}
    headers = {'Authorization': f'Bearer {network.patreon_creator_access_token}'}
    
    updated_count, seen_patreon_ids, url = 0, set(), f"{base_url}?{urllib.parse.urlencode(params)}"

    while url:
        res = requests.get(url, headers=headers)
        if res.status_code == 401 or "Unauthorized" in res.text:
            network.patreon_sync_enabled = False
            network.save()
            return updated_count, "Patreon authorization permanently expired."
            
        if res.status_code == 429:
            time.sleep(int(res.headers.get('Retry-After', 5)))
            continue 
            
        if res.status_code != 200: return updated_count, f"API Error: {res.text}"
        
        data = res.json()
        included = {i['id']: i for i in data.get('included', []) if i['type'] == 'user'}

        for member in data.get('data', []):
            rel_user = member.get('relationships', {}).get('user', {}).get('data', {})
            if not rel_user: continue
                
            patreon_id = rel_user['id']
            seen_patreon_ids.add(patreon_id) 
            email = included.get(patreon_id, {}).get('attributes', {}).get('email')

            profile = PatronProfile.objects.filter(patreon_id=patreon_id).first()
            if not profile and email: profile = PatronProfile.objects.filter(user__email=email).first()

            if profile:
                attrs = member.get('attributes', {})
                status = attrs.get('patron_status')
                cents = attrs.get('currently_entitled_amount_cents', 0)
                
                membership, _ = NetworkMembership.objects.get_or_create(user=profile.user, network=network)
                membership.patreon_pledge_cents = cents if status == 'active_patron' else 0
                membership.is_active_patron = (membership.patreon_pledge_cents > 0)
                membership.save()
                updated_count += 1

        url = data.get('links', {}).get('next')
        if url: time.sleep(0.5)

    stale = NetworkMembership.objects.filter(network=network, is_active_patron=True).exclude(user__patron_profile__patreon_id__in=seen_patreon_ids)
    revoked_count = stale.update(is_active_patron=False, patreon_pledge_cents=0)

    logger.info(f"Sync Complete. Updated: {updated_count} | Revoked: {revoked_count}")
    return updated_count, None

@csrf_exempt
def patreon_webhook(request):
    if request.method != 'POST': return HttpResponse("Method not allowed", status=405)
    
    signature = request.headers.get('X-Patreon-Signature')
    secret = settings.PATREON_WEBHOOK_SECRET.encode('utf-8')
    expected_signature = hmac.new(secret, request.body, hashlib.md5).hexdigest()
    if not hmac.compare_digest(expected_signature, signature): return HttpResponseForbidden("Invalid signature")

    try:
        data = json.loads(request.body)
        member_data = data.get('data', {})
        patreon_user_id = member_data.get('relationships', {}).get('user', {}).get('data', {}).get('id')

        with transaction.atomic():
            profile = PatronProfile.objects.select_for_update().get(patreon_id=patreon_user_id)
            attrs = member_data.get('attributes', {})
            cents = attrs.get('currently_entitled_amount_cents', 0)
            status = attrs.get('patron_status') 
            final_amount = cents if status == 'active_patron' else 0
            
            campaign_id = str(member_data.get('relationships', {}).get('campaign', {}).get('data', {}).get('id', ''))
            network = Network.objects.filter(patreon_campaign_id=campaign_id).first()
            
            if network:
                membership, _ = NetworkMembership.objects.get_or_create(user=profile.user, network=network)
                membership.patreon_pledge_cents = final_amount
                membership.is_active_patron = (final_amount > 0)
                membership.save()
                
        return HttpResponse("Success", status=200)
    except PatronProfile.DoesNotExist:
        return HttpResponse("User not found.", status=200)
    except Exception as e:
        logger.error(f"Webhook Error: {str(e)}", exc_info=True)
        return HttpResponse("Error", status=500)

# ==========================================
# 4. CREATOR SETTINGS & POST ACTION DISPATCHER
# ==========================================

def _handle_inbox_action(request, current_network, action):
    edit_id = request.POST.get('edit_id')
    edit = get_object_or_404(EpisodeEditSuggestion, id=edit_id, episode__podcast__network=current_network)
    membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)

    if action == 'approve_edit':
        ep = edit.episode
        points = 0

        # Snapshot the live episode state RIGHT NOW, before any field is touched.
        # This becomes the new edit.original_data after approval, so single-edit
        # rollback restores the actual pre-approval state — not the user's
        # potentially-stale submission-time snapshot. Captured up front so that
        # field-by-field rewrites below don't pollute the snapshot mid-flight.
        pre_approval_snapshot = {
            'title': ep.title,
            'description': ep.clean_description or '',
            'tags': list(ep.tags or []),
            'chapters': ep.chapters_public if ep.chapters_public is not None else [],
        }
        # User's submission-time snapshot, used to compute deltas (what the user
        # *intended* to add/remove). Falls back to current state when missing so
        # delta math degenerates safely to "no-op" for absent sections.
        user_snapshot = edit.original_data or {}

        # 0. PROCESS TITLE
        if request.POST.get('approve_title') == 'on':
            new_title = request.POST.get('edited_title', '').strip()
            if new_title and new_title != pre_approval_snapshot['title']:
                ep.title = new_title
                edit.suggested_data['title'] = new_title
                points += 1
                membership.edits_title += 1

        # 1. PROCESS DESCRIPTION
        # Whole-string replacement. The admin's checked toggle is acknowledgment
        # of any conflict that was visible in the inbox.
        if request.POST.get('approve_description') == 'on':
            new_desc = request.POST.get('edited_description', '').strip()
            if new_desc and new_desc != pre_approval_snapshot['description']:
                ep.clean_description = new_desc
                edit.suggested_data['description'] = new_desc  # Update for Audit Log
                points += 1
                membership.edits_descriptions += 1

        # 2. PROCESS TAGS — set-delta merge.
        # Compute the user's intent as additions/removals relative to *their*
        # original_data snapshot. Apply that delta to the *current* episode tags,
        # preserving any tags added by other edits approved in the interim.
        # Order: keep current order; append new additions in the order the user
        # supplied them; drop removals.
        if request.POST.get('approve_tags') == 'on':
            raw_tags = request.POST.get('edited_tags', '[]')
            try:
                user_intended_tags = json.loads(raw_tags)
                user_baseline_tags = user_snapshot.get('tags') or []

                if isinstance(user_intended_tags, list):
                    added = [t for t in user_intended_tags if t not in user_baseline_tags]
                    removed = set(user_baseline_tags) - set(user_intended_tags)

                    current_list = list(ep.tags or [])
                    merged = [t for t in current_list if t not in removed]
                    for t in added:
                        if t not in merged:
                            merged.append(t)

                    if merged != current_list:
                        ep.tags = merged
                        edit.suggested_data['tags'] = merged  # Update for Audit Log
                        points += 1

                        # Trust score reflects net new additions only.
                        if added:
                            membership.edits_tags += len(added)
            except Exception as e:
                logger.error(f"Failed to parse tags from inbox: {e}")

        # 3. PROCESS CHAPTERS — full replacement (order-sensitive, can't merge cleanly).
        # The admin's checked toggle is acknowledgment of any conflict warning
        # the inbox showed. We compare against the pre-approval snapshot to
        # decide whether anything actually changed.
        if request.POST.get('approve_chapters') == 'on':
            raw_chapters = request.POST.get('edited_chapters', '')
            if raw_chapters:
                try:
                    new_chapters = json.loads(raw_chapters)
                    if new_chapters != pre_approval_snapshot['chapters']:
                        ep.chapters_public = new_chapters
                        edit.suggested_data['chapters'] = new_chapters  # Update for Audit Log
                        points += 1

                        # Tally chapter counts dynamically based on format.
                        if isinstance(new_chapters, dict):
                            membership.edits_chapters += len(new_chapters.get('chapters', []))
                        elif isinstance(new_chapters, list):
                            membership.edits_chapters += len(new_chapters)
                except Exception as e:
                    logger.error(f"Failed to parse chapters from inbox: {e}")

        # --- THE ZERO-APPROVAL TRAP ---
        if points == 0:
            edit.status = 'rejected'
            edit.resolved_at = timezone.now()
            edit.save()
            membership.trust_score = max(0, membership.trust_score - 2)
            membership.save()
            messages.warning(request, "No sections selected for approval. Edit converted to rejection. User penalized -2 Trust.")
            return

        # --- PERFECT SWEEP BONUS ---
        if points == 3:
            points += 2

        # Lock metadata and finalize approval
        ep.is_metadata_locked = True
        ep.save()

        # Rewrite original_data to the pre-approval snapshot we captured above.
        # This makes single-edit rollback restore the state that existed right
        # before this approval, regardless of when the user originally submitted.
        # All three fields are stored even if only some were approved — fields
        # we didn't change still need a faithful pre-approval value so that
        # rollback restores the *whole* episode to the pre-approval state.
        edit.original_data = pre_approval_snapshot
        edit.status = 'approved'
        edit.resolved_at = timezone.now()
        edit.save()

        membership.trust_score += points
        if edit.is_first_responder:
            membership.first_responder_count += 1
        membership.save()

        messages.success(request, f"Partial edit approved! User awarded +{points} Trust Score.")

        base_url = request.build_absolute_uri('/')[:-1]
        task_rebuild_episode_fragments.delay(ep.id, base_url)

    elif action == 'reject_edit':
        edit.status = 'rejected'
        edit.resolved_at = timezone.now()
        edit.save()
        membership.trust_score = max(0, membership.trust_score - 2)
        membership.save()
        messages.warning(request, "Edit rejected. User penalized -2 Trust.")

def _handle_rollback(request, current_network, action):
    if action == 'rollback_single_edit':
        edit = get_object_or_404(EpisodeEditSuggestion, id=request.POST.get('edit_id'), episode__podcast__network=current_network, status='approved')
        membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)

        # Block when newer approved edits exist on the same episode. Rolling back
        # this edit's `original_data` would restore a state that pre-dates those
        # approvals, silently undoing them. The admin must roll those forward
        # first (or accept that they're already obsolete and the rollback isn't
        # needed).
        newer_approved = EpisodeEditSuggestion.objects.filter(
            episode=edit.episode,
            status='approved',
            resolved_at__gt=edit.resolved_at,
        ).select_related('user').order_by('resolved_at')

        if newer_approved.exists():
            blockers = ", ".join(
                f"#{e.id} by {e.user.username}" for e in newer_approved[:5]
            )
            extra = "" if newer_approved.count() <= 5 else f" (and {newer_approved.count() - 5} more)"
            messages.error(
                request,
                f"Cannot roll back edit #{edit.id}: later approved edits exist on this episode "
                f"({blockers}{extra}). Roll those back first, or leave this edit in place."
            )
            return

        ep = edit.episode
        ep.title = edit.original_data.get('title', ep.title)
        ep.clean_description = edit.original_data.get('description', ep.clean_description)
        ep.tags = edit.original_data.get('tags', ep.tags)
        ep.chapters_public = edit.original_data.get('chapters', ep.chapters_public)
        ep.save()

        base_url = request.build_absolute_uri('/')[:-1]
        task_rebuild_episode_fragments.delay(ep.id, base_url)

        edit.status = 'rolled_back'
        edit.resolved_at = timezone.now()
        edit.save()

        membership.trust_score = max(0, membership.trust_score - 5)
        if edit.suggested_data.get('title') != edit.original_data.get('title'): membership.edits_title = max(0, membership.edits_title - 1)
        if edit.suggested_data.get('chapters') != edit.original_data.get('chapters'): membership.edits_chapters = max(0, membership.edits_chapters - len(edit.suggested_data.get('chapters', [])))
        if edit.suggested_data.get('tags') != edit.original_data.get('tags'): membership.edits_tags = max(0, membership.edits_tags - 1)
        if edit.suggested_data.get('description') != edit.original_data.get('description'): membership.edits_descriptions = max(0, membership.edits_descriptions - 1)
        if edit.is_first_responder: membership.first_responder_count = max(0, membership.first_responder_count - 1)
        membership.save()
        messages.success(request, "Edit rolled back and user penalized.")

    elif action == 'bulk_rollback':
        spammer_id = request.POST.get('spammer_id')
        spammer = get_object_or_404(User, id=spammer_id)
        membership, _ = NetworkMembership.objects.get_or_create(user=spammer, network=current_network)
        
        approved_edits = EpisodeEditSuggestion.objects.filter(
            user=spammer, 
            episode__podcast__network=current_network, 
            status='approved'
        )
        
        base_url = request.build_absolute_uri('/')[:-1]
                                                   
        count = 0
        for edit in approved_edits:
            ep = edit.episode
            ep.title = edit.original_data.get('title', ep.title)
            ep.clean_description = edit.original_data.get('description', ep.clean_description)
            ep.tags = edit.original_data.get('tags', ep.tags)
            ep.chapters_public = edit.original_data.get('chapters', ep.chapters_public)
            ep.save()

            task_rebuild_episode_fragments.delay(ep.id, base_url)

            edit.status = 'rolled_back'
            edit.resolved_at = timezone.now()
            edit.save()
            count += 1
            
        # Nuke their stats for this network
        membership.trust_score = 0
        membership.edits_chapters = 0
        membership.edits_tags = 0
        membership.edits_descriptions = 0
        membership.first_responder_count = 0
        membership.save()
        
        messages.success(request, f"Bulk rollback complete. Reverted {count} edits and dropped trust score to 0.")

@login_required(login_url='/login/')
def creator_settings(request):
    allowed_networks = Network.objects.all() if request.user.is_superuser else Network.objects.filter(owners=request.user)
    if not allowed_networks.exists(): return HttpResponseForbidden("No creator access.")

    current_network = allowed_networks.filter(slug=request.GET.get('network')).first() or allowed_networks.first()

    if request.method == 'POST':
        action = request.POST.get('action')
        
        if action in ['approve_edit', 'reject_edit']:
            _handle_inbox_action(request, current_network, action)
        elif action in ['rollback_single_edit', 'bulk_rollback']:
            _handle_rollback(request, current_network, action)
        elif action == 'run_manual_sync':
            count, error = sync_network_patrons(current_network)
            if error: messages.error(request, f"Sync Failed: {error}")
            else: messages.success(request, f"Synced {count} patrons.")
            
        elif action == 'update_network':
            theme_config_str = request.POST.get('theme_config', '{}')
            try:
                current_network.theme_config = json.loads(theme_config_str)
            except json.JSONDecodeError:
                messages.error(request, f"Invalid JSON format for {current_network.name}. Settings not saved.")
                return redirect(f"{reverse('creator_settings')}?network={current_network.slug}")

            current_network.patreon_campaign_id = request.POST.get('patreon_campaign_id', '')
            current_network.website_url = request.POST.get('website_url', '')
            current_network.default_image_url = request.POST.get('default_image_url', '')
            current_network.ignored_title_tags = request.POST.get('ignored_title_tags', '')
            current_network.description_cut_triggers = request.POST.get('description_cut_triggers', '')
            current_network.global_footer_public = request.POST.get('footer_public', '')
            current_network.global_footer_private = request.POST.get('footer_private', '')
            current_network.save()
            messages.success(request, f"{current_network.name} settings saved successfully!")

            base_url = request.build_absolute_uri('/')[:-1]
            for pod in current_network.podcasts.all():
                task_rebuild_podcast_fragments.delay(pod.id, base_url)

        elif action == 'update_show':
            show_id = request.POST.get('show_id')
            show = get_object_or_404(Podcast, id=show_id, network=current_network)
            show.public_feed_url = request.POST.get('public_feed_url', show.public_feed_url)
            show.subscriber_feed_url = request.POST.get('subscriber_feed_url', show.subscriber_feed_url)
            
            tier_id = request.POST.get('tier_id')
            show.required_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None
            show.show_footer_public = request.POST.get('show_footer_public', '')
            show.show_footer_private = request.POST.get('show_footer_private', '')
            show.save()
            messages.success(request, f"{show.title} updated successfully!")

            base_url = request.build_absolute_uri('/')[:-1]
            task_rebuild_podcast_fragments.delay(show.id, base_url)

        elif action == 'add_show':
            title = request.POST.get('title')
            slug = request.POST.get('slug')
            tier_id = request.POST.get('tier_id')
            req_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None
            new_show = Podcast.objects.create(
                network=current_network, title=title, slug=slug,
                public_feed_url=request.POST.get('public_feed_url'), 
                subscriber_feed_url=request.POST.get('subscriber_feed_url'),
                required_tier=req_tier
            )
            messages.success(request, f"Show '{title}' added! Starting live ingestion...")
            return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&auto_import={new_show.id}")

        elif action == 'merge_episodes':
            pub_id = request.POST.get('public_episode_id')
            priv_id = request.POST.get('private_episode_id')
            if pub_id and priv_id:
                pub_ep = Episode.objects.get(id=pub_id, podcast__network=current_network)
                priv_ep = Episode.objects.get(id=priv_id, podcast__network=current_network)
                pub_ep.guid_private = priv_ep.guid_private or priv_ep.guid_public
                pub_ep.audio_url_subscriber = priv_ep.audio_url_subscriber
                if priv_ep.chapters_private: pub_ep.chapters_private = priv_ep.chapters_private
                if priv_ep.tags and not pub_ep.tags: pub_ep.tags = priv_ep.tags
                pub_ep.match_reason = "Manual Merge (Merge Desk)"
                pub_ep.save()
                priv_ep.delete()
                base_url = request.build_absolute_uri('/')[:-1]
                task_rebuild_episode_fragments.delay(pub_ep.id, base_url)
                messages.success(request, f"Successfully merged '{priv_ep.title}' into '{pub_ep.title}'.")

        elif action == 'split_episode':
            ep = Episode.objects.get(id=request.POST.get('episode_id'), podcast__network=current_network)
            new_ep = Episode.objects.create(
                podcast=ep.podcast, title=ep.title, pub_date=ep.pub_date,
                raw_description=ep.raw_description, clean_description=ep.clean_description,
                duration=ep.duration, link=ep.link, tags=ep.tags,
                guid_private=ep.guid_private, audio_url_subscriber=ep.audio_url_subscriber,
                chapters_private=ep.chapters_private, match_reason="Manually Unpaired"
            )
            ep.guid_private = None
            ep.audio_url_subscriber = ""
            ep.chapters_private = None
            ep.match_reason = "Manually Unpaired"
            ep.save()
            base_url = request.build_absolute_uri('/')[:-1]
            task_rebuild_episode_fragments.delay(ep.id, base_url)
            task_rebuild_episode_fragments.delay(new_ep.id, base_url)
            messages.success(request, f"Successfully split '{ep.title}'.")
        
        return redirect(f"{reverse('creator_settings')}?network={current_network.slug}")

    # 1. Manage Podcasts
    manage_podcasts = current_network.podcasts.annotate(
        clean_title=Case(When(title__istartswith='The ', then=Substr('title', 5)), default='title', output_field=CharField()),
        latest_episode_date=Max('episodes__pub_date'),
        episode_count=Count('episodes', distinct=True)
    ).order_by(Lower('clean_title'))

    # 2. Inbox (Pending Edits with Multi-Tenant Trust Scores + Conflict Detection)
    pending_edits = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network=current_network, status='pending'
    ).select_related('episode', 'user')

    user_ids = [e.user_id for e in pending_edits]
    memberships = {m.user_id: m for m in NetworkMembership.objects.filter(user_id__in=user_ids, network=current_network)}

    for edit in pending_edits:
        edit.membership = memberships.get(edit.user_id)

        # Per-section conflict detection: did the live episode field change since
        # this edit was submitted? Computation runs entirely in Python on data
        # already loaded by select_related — no extra DB hits per edit. Each
        # `*_conflict` flag becomes True when the user's `original_data` snapshot
        # disagrees with the current episode field, meaning some other approval
        # has landed in the interim and the admin should review the 3-column view.
        ep = edit.episode
        orig = edit.original_data or {}

        # Title: string equality
        current_title = ep.title or ''
        snapshot_title = orig.get('title') or ''
        edit.title_conflict = current_title != snapshot_title
        edit.current_title = current_title

        # Tags: compare as sets — listing order is not semantically meaningful.
        current_tags = ep.tags or []
        snapshot_tags = orig.get('tags') or []
        edit.tags_conflict = set(current_tags) != set(snapshot_tags)
        edit.current_tags = current_tags

        # Chapters: compare as deserialized Python objects. Both list and
        # {waypoints, chapters: [...]} shapes compare correctly via ==.
        current_chapters = ep.chapters_public or []
        snapshot_chapters = orig.get('chapters') or []
        edit.chapters_conflict = current_chapters != snapshot_chapters
        edit.current_chapters = current_chapters

        # Description: HTML string equality. May produce occasional false
        # positives if the renderer re-serialized whitespace, but never a false
        # negative — when in doubt the admin sees the warning.
        current_desc = ep.clean_description or ''
        snapshot_desc = orig.get('description') or ''
        edit.desc_conflict = current_desc != snapshot_desc
        edit.current_description = current_desc

    # 3. Merge Desk (Unpaired & Matched Episodes)
    merge_view = request.GET.get('merge_view', 'orphans')
    merge_podcast_id = request.GET.get('merge_podcast_id', '')
    merge_q = request.GET.get('merge_q', '').strip()
    merge_reason = request.GET.get('merge_reason', '').strip()

    base_episodes = Episode.objects.filter(podcast__network=current_network).select_related('podcast')

    if merge_podcast_id:
        base_episodes = base_episodes.filter(podcast_id=merge_podcast_id)
    if merge_q:
        base_episodes = base_episodes.filter(
            Q(title__icontains=merge_q) | 
            Q(guid_public__icontains=merge_q) | 
            Q(guid_private__icontains=merge_q)
        )

    public_orphans = None
    private_orphans = None
    matched_episodes = None
    match_reasons = []

    if merge_view == 'orphans':
        pub_qs = base_episodes.filter(Q(guid_private__isnull=True) | Q(guid_private__exact='')).exclude(Q(audio_url_public__isnull=True) | Q(audio_url_public__exact='')).order_by('-pub_date')
        public_orphans = Paginator(pub_qs, 20).get_page(request.GET.get('pub_page', 1))

        priv_qs = base_episodes.filter(Q(guid_public__isnull=True) | Q(guid_public__exact='')).exclude(Q(audio_url_subscriber__isnull=True) | Q(audio_url_subscriber__exact='')).order_by('-pub_date')
        private_orphans = Paginator(priv_qs, 20).get_page(request.GET.get('priv_page', 1))
    
    elif merge_view == 'matched':
        matched_qs = base_episodes.exclude(Q(guid_public__isnull=True) | Q(guid_public__exact='')).exclude(Q(guid_private__isnull=True) | Q(guid_private__exact=''))
        
        match_reasons = Episode.objects.filter(podcast__network=current_network).exclude(match_reason__isnull=True).exclude(match_reason__exact='').values_list('match_reason', flat=True).distinct()
        
        if merge_reason:
            matched_qs = matched_qs.filter(match_reason=merge_reason)
            
        matched_episodes = Paginator(matched_qs.order_by('-pub_date'), 20).get_page(request.GET.get('match_page', 1))

    # 4. Audit Log
    audit_query = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network=current_network
    ).exclude(status='pending').select_related('episode', 'user')

    audit_q = request.GET.get('audit_q', '').strip()
    audit_status = request.GET.get('audit_status', '').strip()
    audit_user = request.GET.get('audit_user', '').strip()

    if audit_q:
        audit_query = audit_query.filter(Q(episode__title__icontains=audit_q) | Q(episode__podcast__title__icontains=audit_q))
    if audit_status:
        audit_query = audit_query.filter(status=audit_status)
    if audit_user:
        audit_query = audit_query.filter(user__username__icontains=audit_user)

    audit_query = audit_query.order_by('-resolved_at')
    audit_paginator = Paginator(audit_query, 20)
    audit_page_obj = audit_paginator.get_page(request.GET.get('audit_page', 1))

    # 5. Final Context Assembly
    context = {
        'networks': allowed_networks,
        'current_network': current_network,
        'manage_podcasts': manage_podcasts,
        'pending_edits': pending_edits,
        'network_podcasts': manage_podcasts,
        'merge_view': merge_view,
        'merge_podcast_id': merge_podcast_id,
        'merge_q': merge_q,
        'merge_reason': merge_reason,
        'public_orphans': public_orphans,
        'private_orphans': private_orphans,
        'matched_episodes': matched_episodes,
        'match_reasons': match_reasons,
        'audit_page_obj': audit_page_obj,
        'theme_config_json': json.dumps(current_network.theme_config, indent=2),
    }
    return render(request, 'pod_manager/creator_settings.html', context)

# ==========================================
# 5. FRONTEND VIEWS (MULTI-TENANT)
# ==========================================

def home(request):
    show_slug = request.GET.get('show')
    search_query = request.GET.get('q', '').strip()
    
    tenant_profile = getattr(request, 'tenant_profile', None)
    
    query = Episode.objects.select_related('podcast', 'podcast__network', 'podcast__required_tier').filter(podcast__network=request.network)
    podcasts = Podcast.objects.filter(network=request.network).order_by('title')

    if show_slug: query = query.filter(podcast__slug=show_slug)
    if search_query: query = query.filter(Q(title__icontains=search_query) | Q(clean_description__icontains=search_query))
        
    page_obj = Paginator(query.order_by('-pub_date'), 20).get_page(request.GET.get('page', 1))
    
    for ep in page_obj:
        ep.user_has_access, is_owner = _evaluate_access(request.user, ep.podcast, request.network)

    context = {
        'episodes': page_obj, 'page_obj': page_obj, 'podcasts': podcasts,          
        'current_filter': show_slug, 'current_network': request.network, 
        'search_query': search_query, 'tenant_profile': tenant_profile
    }
    return render(request, 'pod_manager/home.html', context)

def user_feeds(request):
    tenant_profile = getattr(request, 'tenant_profile', None)
    profile = getattr(request.user, 'patron_profile', None) if request.user.is_authenticated else None
    
    # --- 1. HANDLE POST ACTIONS (CREATE, EDIT, DELETE MIX) ---
    if request.method == 'POST':
        if request.POST.get('create_mix'):
            mix_name = request.POST.get('mix_name', '').strip() or f"{request.user.first_name}'s Custom Mix"
            mix = UserMix.objects.create(
                user=request.user,
                network=request.network,
                name=mix_name,
                image_url=request.POST.get('mix_image', '')
            )
            if 'mix_image_upload' in request.FILES:
                mix.image_upload = request.FILES['mix_image_upload']
                
            mix.selected_podcasts.set(request.POST.getlist('podcasts'))
            mix.save()
            messages.success(request, f"Mix '{mix.name}' created successfully!")

        elif request.POST.get('edit_mix'):
            mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
            mix.name = request.POST.get('mix_name', '').strip() or mix.name
            mix.image_url = request.POST.get('mix_image', '') or mix.image_url
            if 'mix_image_upload' in request.FILES:
                mix.image_upload = request.FILES['mix_image_upload']
            cache.delete(f"shell_user_mix_{mix.id}")   
            mix.selected_podcasts.set(request.POST.getlist('podcasts'))
            mix.save()

            messages.success(request, "Mix updated successfully!")

        elif request.POST.get('delete_mix'):
            mix = get_object_or_404(UserMix, id=request.POST.get('mix_id'), user=request.user, network=request.network)
            cache.delete(f"shell_user_mix_{mix.id}")
            mix.delete()
            messages.warning(request, "Custom mix deleted.")

        return redirect('user_feeds')

    # --- 2. GENERATE GET DATA ---
    feed_data = []
    available_podcasts = []
    
    # PROCESS NETWORK MIXES FIRST (So they group at the top of the UI)
    network_mixes = NetworkMix.objects.filter(network=request.network)
    for mix in network_mixes:
        mix_req_cents = mix.required_tier.minimum_cents if mix.required_tier else 0
        user_cents = tenant_profile.patreon_pledge_cents if tenant_profile else 0
        is_owner = request.network.owners.filter(id=request.user.id).exists() if request.user.is_authenticated else False
        
        mix.has_access = is_owner or (mix_req_cents == 0) or (user_cents >= mix_req_cents)
        mix.feed_url = request.build_absolute_uri(reverse('network_mix_feed', args=[request.network.slug, mix.slug])) + (f"?auth={profile.feed_token}" if profile else "")
        
        # FIX: Append to feed_data so the HTML template can actually render it!
        feed_data.append({
            'is_network_mix': True, 
            'mix': mix, 
            'has_access': mix.has_access, 
            'feed_url': mix.feed_url
        })

    # PROCESS STANDARD PODCASTS
    for podcast in Podcast.objects.filter(network=request.network).select_related('network', 'required_tier'):
        has_access, is_owner = _evaluate_access(request.user, podcast, request.network)
        
        available_podcasts.append({
            'podcast': podcast,
            'has_access': has_access
        })
        
        if profile is not None:
            raw_url = reverse('custom_feed') + f"?auth={profile.feed_token}&show={podcast.slug}"
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': has_access, 'feed_url': request.build_absolute_uri(raw_url)})
        elif not podcast.required_tier or podcast.public_feed_url:
            raw_url = reverse('public_feed', args=[podcast.slug]) 
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': False, 'feed_url': request.build_absolute_uri(raw_url)})

    user_mixes = UserMix.objects.filter(user=request.user, network=request.network, is_active=True).prefetch_related('selected_podcasts') if request.user.is_authenticated else []

    context = {
        'profile': profile, 
        'tenant_profile': tenant_profile, 
        'feed_data': feed_data,
        'user_mixes': user_mixes,
        'current_network': request.network,
        'available_podcasts': available_podcasts
    }
    return render(request, 'pod_manager/user_feeds.html', context)

def episode_detail(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    ep.user_has_access, _ = _evaluate_access(request.user, ep.podcast, ep.podcast.network)
    
    if ep.podcast.network != request.network and not ep.user_has_access:
        raise Http404("No Episode matches the given query.")
    
    ep.display_description = _build_episode_description(ep, ep.user_has_access)
    return render(request, 'pod_manager/episode_detail.html', {'ep': ep})

@login_required(login_url='/login/')
def user_profile(request):
    tenant_profile = getattr(request, 'tenant_profile', None)
    
    if not tenant_profile:
        return render(request, 'pod_manager/user_profile.html', {
            'level': 0, 'title': "Commoner", 'progress_percent': 0,
            'total_approved': 0, 'account_vintage': None, 
            'live_stats': {'playback_hits': 0, 'hours_accessed': 0.0, 'streak_days': 0, 'streak_weeks': 0, 'obsession_title': "Wandering Adventurer"}
        })

    account_vintage = tenant_profile.patreon_join_date
    joined_after_launch_days = None
    account_age_years = None

    if account_vintage:
        account_age_years = (timezone.now() - account_vintage).days / 365.25
        if request.network.patreon_campaign_created_at:
            delta = account_vintage - request.network.patreon_campaign_created_at
            joined_after_launch_days = max(0, delta.days)

    total_approved = EpisodeEditSuggestion.objects.filter(user=request.user, episode__podcast__network=request.network, status='approved').count()
    
    level, title, next_level_goal, progress_percent = 0, "Commoner", 1, 0
    if total_approved >= 1000:
        level, title, next_level_goal, progress_percent = 5, "Keeper of the Tome", 1000, 100
    elif total_approved >= 500:
        level, title, next_level_goal, progress_percent = 4, "Grand Archivist", 1000, (total_approved / 1000) * 100
    elif total_approved >= 100:
        level, title, next_level_goal, progress_percent = 3, "Archivist", 500, (total_approved / 500) * 100
    elif total_approved >= 25:
        level, title, next_level_goal, progress_percent = 2, "Scout", 100, (total_approved / 100) * 100
    elif total_approved >= 1:
        level, title, next_level_goal, progress_percent = 1, "Initiate", 25, (total_approved / 25) * 100

    context = {
        'profile': tenant_profile, 'total_approved': total_approved,
        'level': level, 'title': title, 'next_level_goal': next_level_goal,
        'progress_percent': min(progress_percent, 100),
        'live_stats': get_live_user_stats(tenant_profile),
        'account_vintage': account_vintage,
        'joined_after_launch_days': joined_after_launch_days,
        'account_age_years': account_age_years,
    }
    return render(request, 'pod_manager/user_profile.html', context)

def episode_chapters(request, episode_id, feed_type):
    ep = get_object_or_404(Episode, id=episode_id)
    data = ep.chapters_public or ep.chapters_private if feed_type == 'public' else ep.chapters_private or ep.chapters_public
    if not data: raise Http404("Chapters not found.")
    
    # Check if the DB holds a raw legacy list or the new dict format
    if isinstance(data, list):
        payload = {
            "version": "1.2.0",
            "chapters": data
        }
    elif isinstance(data, dict):
        payload = data
        # Inject the mandatory version string if the database object lacks it
        if "version" not in payload:
            payload["version"] = "1.2.0"
        if "chapters" not in payload:
            payload["chapters"] = []
    else:
        raise Http404("Invalid chapter format in database.")
        
    response = JsonResponse(payload, safe=False)
    response["Access-Control-Allow-Origin"] = "*"
    return response

def get_or_build_feed_shell(podcast, base_url, has_access):
    """Caches the top-level RSS metadata (Header and Footer) without any episodes."""
    feed_type = 'private' if has_access else 'public'
    cache_key = f"feed_shell_{feed_type}_{podcast.id}"
    shell = cache.get(cache_key)
    if shell: return shell

    title = f"{podcast.title} (Private)" if has_access else podcast.title
    builder = RSSFeedBuilder(base_url, title, podcast.description or "", podcast.image_url, podcast.network, feed_type)
    raw_xml = builder.render()
    
    # Split the XML to grab everything before the closing </channel> tag
    header = raw_xml.split('</channel>')[0]
    footer = "</channel></rss>"
    
    shell = (header, footer)
    cache.set(cache_key, shell, timeout=604800) # 7 Days
    return shell

def get_or_build_episode_fragment(episode, base_url, has_access):
    """Caches a single <item>...</item> block."""
    feed_type = 'private' if has_access else 'public'
    cache_key = f"ep_frag_{feed_type}_{episode.id}"
    fragment = cache.get(cache_key)
    
    # FIX 1: Check for None explicitly so we don't infinitely rebuild empty strings
    if fragment is not None: return fragment

    # Build a temporary shell to render just this one episode
    builder = RSSFeedBuilder(base_url, "Temp", "Temp", "", episode.podcast.network, feed_type)
    builder.add_episode(episode, has_access)
    raw_xml = builder.render(access_map={episode.podcast_id: has_access})
    
    # FIX 2: Use robust regex to extract the item block, ignoring whitespace/attributes
    match = re.search(r'(<item.*?>.*?</item>)', raw_xml, re.DOTALL | re.IGNORECASE)
    fragment = match.group(1) if match else ""
        
    cache.set(cache_key, fragment, timeout=604800) # 7 Days
    return fragment

# ==========================================
# 6. RSS FEED ROUTES (FRAGMENT ASSEMBLY)
# ==========================================

def generate_custom_feed(request):
    feed_token = request.GET.get('auth')
    podcast = get_object_or_404(Podcast, slug=request.GET.get('show'), network=request.network)
    profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    has_access, _ = _evaluate_access(profile.user, podcast, podcast.network)

    base_url = request.build_absolute_uri('/')[:-1]
    header, footer = get_or_build_feed_shell(podcast, base_url, has_access)
    
    # Get valid episodes
    episodes = [ep for ep in podcast.episodes.all().order_by('-pub_date')[:1000] if ep.has_public_audio or ep.is_premium]
    if not has_access: episodes = [ep for ep in episodes if ep.has_public_audio]
        
    feed_type = 'private' if has_access else 'public'
    cache_keys = [f"ep_frag_{feed_type}_{ep.id}" for ep in episodes]
    
    # Bulk fetch fragments from Redis
    fragments_dict = cache.get_many(cache_keys)
    
    # Assemble missing fragments inline if cache missed
    items_xml = ""
    for i, ep in enumerate(episodes):
        frag = fragments_dict.get(cache_keys[i])
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, has_access)
        items_xml += frag
        
    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(profile.feed_token))
    
    analytics_key = f"analytics:rss:{profile.id}"
    cache.incr(analytics_key) if cache.get(analytics_key) else cache.set(analytics_key, 1, timeout=172800)
    billing_key = f"billing:active:{podcast.network_id}:{profile.user_id}:{timezone.now().strftime('%Y-%m-%d')}"
    cache.set(billing_key, 1, timeout=172800)

    xml_bytes = final_xml.encode('utf-8')
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        logger.info(f"[ETag MATCH] {request.path} | Served: 0 bytes")
        response = HttpResponseNotModified()
        response['Access-Control-Allow-Origin'] = '*'
        return response

    response = HttpResponse(xml_bytes, content_type='application/xml')
    response['ETag'] = etag
    response['Cache-Control'] = 'public, max-age=0, must-revalidate'
    response['Access-Control-Allow-Origin'] = '*'
    
    size_mb = len(xml_bytes) / (1024 * 1024)
    logger.info(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
    return response

def generate_public_feed(request, podcast_slug):
    podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)
    base_url = request.build_absolute_uri('/')[:-1]
    
    header, footer = get_or_build_feed_shell(podcast, base_url, False)
    episodes = [ep for ep in podcast.episodes.all().order_by('-pub_date')[:500] if ep.has_public_audio]
    
    cache_keys = [f"ep_frag_public_{ep.id}" for ep in episodes]
    fragments_dict = cache.get_many(cache_keys)
    
    items_xml = ""
    for i, ep in enumerate(episodes):
        frag = fragments_dict.get(cache_keys[i])
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, False)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('?auth=__VECTO_AUTH_TOKEN__', '')
    xml_bytes = final_xml.encode('utf-8')
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        logger.info(f"[ETag MATCH] {request.path} | Served: 0 bytes")
        response = HttpResponseNotModified()
        response['Access-Control-Allow-Origin'] = '*'
        return response

    response = HttpResponse(xml_bytes, content_type='application/xml')
    response['ETag'] = etag
    response['Cache-Control'] = 'public, max-age=0, must-revalidate'
    response['Access-Control-Allow-Origin'] = '*'
    
    size_mb = len(xml_bytes) / (1024 * 1024)
    logger.info(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
    return response

def generate_mix_feed(request, unique_id):
    user_mix = get_object_or_404(UserMix.objects.select_related('user__patron_profile'), unique_id=unique_id, is_active=True)
    base_url = request.build_absolute_uri('/')[:-1]
    cache_key = f"shell_user_mix_{user_mix.id}"
    shell = cache.get(cache_key)
    
    if not shell:
        # Generate mix shell on the fly (lightweight)
        builder = RSSFeedBuilder(base_url, user_mix.name, f"Custom blended feed for {user_mix.user.first_name}.", user_mix.display_image or user_mix.network.default_image_url, user_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)
        
    header, footer = shell
    
    episodes = Episode.objects.filter(podcast__in=user_mix.selected_podcasts.all()).select_related('podcast', 'podcast__network').order_by('-pub_date')[:500]
    if episodes:
        # Get the exact RFC-2822 formatted string of the newest episode
        latest_date_str = format_datetime(episodes[0].pub_date)
        # Swap it into the cached header
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    keys_and_eps = []
    for ep in episodes:
        if not ep.has_public_audio and not ep.is_premium: continue
        ep_has_access, _ = _evaluate_access(user_mix.user, ep.podcast, ep.podcast.network)
        feed_type = 'private' if ep_has_access else 'public'
        keys_and_eps.append((f"ep_frag_{feed_type}_{ep.id}", ep, ep_has_access))
        
    fragments_dict = cache.get_many([k[0] for k in keys_and_eps])
    
    items_xml = ""
    for key, ep, ep_has_access in keys_and_eps:
        frag = fragments_dict.get(key)
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, ep_has_access)
        
        # Inject Podcast Title into episode title for mix context
        safe_title = html.escape(ep.podcast.title)
        frag = frag.replace('<title>', f'<title>[{safe_title}] ', 1)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(user_mix.user.patron_profile.feed_token))
    
    # Get all distinct network IDs attached to the podcasts in this mix
    today_str = timezone.now().strftime('%Y-%m-%d')
    network_ids = set(user_mix.selected_podcasts.values_list('network_id', flat=True))
    
    for net_id in network_ids:
        billing_key = f"billing:active:{net_id}:{user_mix.user.id}:{today_str}"
        cache.set(billing_key, 1, timeout=172800)
    # ==========================================

    xml_bytes = final_xml.encode('utf-8')
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        logger.info(f"[ETag MATCH] {request.path} | Served: 0 bytes")
        response = HttpResponseNotModified()
        response['Access-Control-Allow-Origin'] = '*'
        return response

    response = HttpResponse(xml_bytes, content_type='application/xml')
    response['ETag'] = etag
    response['Cache-Control'] = 'public, max-age=0, must-revalidate'
    response['Access-Control-Allow-Origin'] = '*'
    
    size_mb = len(xml_bytes) / (1024 * 1024)
    logger.info(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
    return response

def generate_network_mix_feed(request, network_slug, mix_slug):
    network_mix = get_object_or_404(NetworkMix, slug=mix_slug, network__slug=network_slug)
    feed_token = request.GET.get('auth')
    profile = PatronProfile.objects.filter(feed_token=feed_token).first() if feed_token else None
    user = profile.user if profile else request.user
    
    mix_req_cents = network_mix.required_tier.minimum_cents if network_mix.required_tier else 0
    mix_membership = NetworkMembership.objects.filter(user=user, network=network_mix.network).first() if user.is_authenticated else None
    user_cents = mix_membership.patreon_pledge_cents if mix_membership else 0
    is_owner = network_mix.network.owners.filter(id=user.id).exists() if user.is_authenticated else False
    user_meets_mix_tier = is_owner or (mix_req_cents == 0) or (user_cents >= mix_req_cents)

    base_url = request.build_absolute_uri('/')[:-1]
    cache_key = f"shell_net_mix_{network_mix.id}"
    shell = cache.get(cache_key)
    
    if not shell:
        builder = RSSFeedBuilder(base_url, network_mix.name, f"A curated network mix by {network_mix.network.name}.", network_mix.display_image or network_mix.network.default_image_url, network_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)
        
    header, footer = shell
    
    episodes = Episode.objects.filter(podcast__in=network_mix.selected_podcasts.all()).select_related('podcast', 'podcast__network').order_by('-pub_date')[:5000]
    if episodes:
        # Get the exact RFC-2822 formatted string of the newest episode
        latest_date_str = format_datetime(episodes[0].pub_date)
        # Swap it into the cached header
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    keys_and_eps = []
    for ep in episodes:
        ep_has_access, _ = _evaluate_access(user, ep.podcast, ep.podcast.network)
        total_access = user_meets_mix_tier and ep_has_access
        if not total_access and not ep.audio_url_public: continue
        
        feed_type = 'private' if total_access else 'public'
        keys_and_eps.append((f"ep_frag_{feed_type}_{ep.id}", ep, total_access))

    fragments_dict = cache.get_many([k[0] for k in keys_and_eps])
    
    items_xml = ""
    for key, ep, total_access in keys_and_eps:
        frag = fragments_dict.get(key)
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, total_access)
        safe_title = html.escape(ep.podcast.title)
        frag = frag.replace('<title>', f'<title>[{safe_title}] ', 1)
        items_xml += frag

    final_xml = header + items_xml + footer
    if feed_token: final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(feed_token))
    else: final_xml = final_xml.replace('?auth=__VECTO_AUTH_TOKEN__', '') 
    if user and user.is_authenticated:
        billing_key = f"billing:active:{network_mix.network_id}:{user.id}:{timezone.now().strftime('%Y-%m-%d')}"
        cache.set(billing_key, 1, timeout=172800)
    
    xml_bytes = final_xml.encode('utf-8')

    # --- 2. Generate the ETag ---
    # The HTTP spec requires ETags to be wrapped in double quotes.
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    
    # --- 3. Check the Client's Request ---
    # The client sends their saved ETag in the 'If-None-Match' header
    client_etag = request.META.get('HTTP_IF_NONE_MATCH')
    
    if client_etag == etag:
        # The client has the exact same file. Send a 304 and 0 bytes.
        logger.info(f"[ETag MATCH] {request.path} | Saved Client downloading XML | Served: 0 bytes")
        
        response = HttpResponseNotModified()
        response['Access-Control-Allow-Origin'] = '*'
        return response

    # --- 4. The Client needs the new file (or it's their first time) ---
    response = HttpResponse(xml_bytes, content_type='application/xml')
    response['ETag'] = etag
    response['Cache-Control'] = 'public, max-age=0, must-revalidate'
    response['Access-Control-Allow-Origin'] = '*'
    
    # Calculate the size of the payload we are about to send (in Megabytes)
    size_mb = len(xml_bytes) / (1024 * 1024)
    logger.info(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
    
    return response

def play_episode(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    feed_token = request.GET.get('auth')
    
    has_access = False
    if feed_token:
        profile = PatronProfile.objects.filter(feed_token=feed_token).first()
        if profile:
            has_access, _ = _evaluate_access(profile.user, ep.podcast, ep.podcast.network)
            if has_access:
                ck = f"analytics:play:{profile.id}:{ep.id}:{ep.podcast_id}"
                cache.incr(ck) if cache.get(ck) else cache.set(ck, 1, 172800)
                billing_key = f"billing:active:{ep.podcast.network_id}:{profile.user_id}:{timezone.now().strftime('%Y-%m-%d')}"
                cache.set(billing_key, 1, timeout=172800)
            
    target_url = ep.audio_url_subscriber if (has_access and ep.audio_url_subscriber) else ep.audio_url_public
    if not target_url: raise Http404("Audio file not found.")
        
    response = HttpResponseRedirect(target_url)
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response

# ==========================================
# 7. ACTION VIEWS
# ==========================================

@login_required(login_url='/login/')
def submit_episode_edit(request, episode_id):
    if request.method != 'POST': return HttpResponseForbidden("Only POST allowed")
        
    ep = get_object_or_404(Episode, id=episode_id)
    payload_str = request.POST.get('payload')
    if not payload_str: return redirect('episode_detail', episode_id=ep.id)
        
    try:
        suggested_data = json.loads(payload_str)
        
        # --- STRICT CHAPTER & LOCATION SANITIZATION ---
        raw_chapters_input = suggested_data.get('chapters', [])
        
        # Detect if the payload came in as a dict (waypoints enabled) or list
        is_dict_format = isinstance(raw_chapters_input, dict)
        raw_chap_list = raw_chapters_input.get('chapters', []) if is_dict_format else raw_chapters_input
        waypoints_enabled = raw_chapters_input.get('waypoints', False) if is_dict_format else False

        clean_chapters = []
        for chap in raw_chap_list:
            if 'startTime' in chap and 'title' in chap:
                try:
                    c = {
                        "startTime": float(chap['startTime']),
                        "title": str(chap['title']).strip()
                    }
                    if 'endTime' in chap and chap['endTime'] not in [None, ""]:
                        c['endTime'] = float(chap['endTime'])
                    if 'url' in chap and str(chap['url']).startswith('http'):
                        c['url'] = str(chap['url']).strip()
                    if 'img' in chap and str(chap['img']).startswith('http'):
                        c['img'] = str(chap['img']).strip()
                    
                    # Explicitly store toc if set to false
                    if 'toc' in chap and chap['toc'] is False:
                        c['toc'] = False
                        
                    # Location object tagging
                    if 'location' in chap and isinstance(chap['location'], dict):
                        loc = chap['location']
                        if 'name' in loc and 'geo' in loc:
                            c_loc = {
                                "name": str(loc['name']).strip(),
                                "geo": str(loc['geo']).strip()
                            }
                            if 'osm' in loc and loc['osm']:
                                c_loc['osm'] = str(loc['osm']).strip()
                            c['location'] = c_loc

                    clean_chapters.append(c)
                except ValueError:
                    pass 

        # Package the validated data back into the shape the DB expects
        if waypoints_enabled:
            suggested_data['chapters'] = {"waypoints": True, "chapters": clean_chapters}
        else:
            suggested_data['chapters'] = clean_chapters

        network = ep.podcast.network
        membership, _ = NetworkMembership.objects.get_or_create(user=request.user, network=network)
        
        original_data = {"title": ep.title, "description": ep.clean_description, "tags": ep.tags or [], "chapters": ep.chapters_public or []}
        is_first = not EpisodeEditSuggestion.objects.filter(episode=ep, status='approved').exists()
        
        is_trusted = membership.trust_score >= network.auto_approve_trust_threshold
        final_status = 'approved' if is_trusted else 'pending'
        
        EpisodeEditSuggestion.objects.create(
            episode=ep, user=request.user, suggested_data=suggested_data,
            original_data=original_data, status=final_status, is_first_responder=is_first,
            resolved_at=timezone.now() if is_trusted else None
        )
        
        if is_trusted:
            ep.title = suggested_data.get('title', ep.title)
            ep.clean_description = suggested_data.get('description', ep.clean_description)
            ep.tags = suggested_data.get('tags', ep.tags)
            ep.chapters_public = suggested_data.get('chapters', ep.chapters_public)
            ep.is_metadata_locked = True
            ep.save()
            
            base_url = request.build_absolute_uri('/')[:-1]
            task_rebuild_episode_fragments.delay(ep.id, base_url)

            membership.trust_score = membership.trust_score + 5
            if suggested_data.get('title') != original_data.get('title'): membership.edits_titles += 1
            if suggested_data.get('chapters') != original_data.get('chapters'): membership.edits_chapters += len(suggested_data.get('chapters', []))
            if suggested_data.get('tags') != original_data.get('tags'): membership.edits_tags += 1
            if suggested_data.get('description') != original_data.get('description'): membership.edits_descriptions += 1
            if is_first: membership.first_responder_count += 1
            membership.save()
            messages.success(request, "Edit approved instantly. +5 Trust.")
        else:
            messages.success(request, "Edit submitted for review.")
            
    except Exception as e:
        messages.error(request, "Failed to submit edit.")
        
    return redirect('episode_detail', episode_id=ep.id)

# ==========================================
# 8. OAUTH & AUTHENTICATION
# ==========================================

def _exchange_patreon_token(code, redirect_uri):
    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": settings.PATREON_CLIENT_ID,
        "client_secret": settings.PATREON_CLIENT_SECRET,
        "redirect_uri": redirect_uri,
    }
    res = requests.post(token_url, data=data, timeout=10)
    if res.status_code != 200:
        logger.error(f"Patreon token exchange failed: {res.text}")
        return None, HttpResponse(f"Failed to get token: {res.text}", status=400)
    return res.json(), None

def _link_creator_campaign(request, network_id, access_token, refresh_token):
    """Handles the flow when a creator links their Patreon to a Vecto Network."""
    logger.info(f"Linking Patreon Campaign to Network ID {network_id} for user {request.user.username}")
    network = get_object_or_404(Network, id=network_id)
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # 1. Update the URL to explicitly request tier data alongside the campaign data
    url = (
        "https://www.patreon.com/api/oauth2/v2/campaigns"
        "?include=tiers"
        "&fields[campaign]=created_at,image_url,image_small_url,url,vanity,summary,one_liner,discord_server_id"
        "&fields[tier]=title,amount_cents,url"
    )
    
    camp_res = requests.get(url, headers=headers, timeout=10)
    
    if camp_res.status_code == 200:
        payload = camp_res.json()
        camp_data = payload.get('data', [])
        included_data = payload.get('included', [])
        
        if camp_data:
            campaign_id = camp_data[0]['id']
            attrs = camp_data[0].get('attributes', {})
            
            # Only overwrite these if they are empty, so we don't destroy manual edits
            if not network.logo_url: network.logo_url = attrs.get('image_small_url', '')
            if not network.banner_image_url: network.banner_image_url = attrs.get('image_url', '')
            if not network.patreon_url: network.patreon_url = attrs.get('url', '')
            if not network.summary: network.summary = attrs.get('summary', '')
            if not network.one_liner: network.one_liner = attrs.get('one_liner', '')
            if not network.discord_server_id: network.discord_server_id = attrs.get('discord_server_id', '')

            network.patreon_campaign_id = campaign_id
            created_at_str = attrs.get('created_at')
            if created_at_str:
                network.patreon_campaign_created_at = timezone.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            
            network.patreon_sync_enabled = True
            network.patreon_creator_access_token = access_token
            network.patreon_creator_refresh_token = refresh_token
            network.save()
            
            tiers_created = 0
            for item in included_data:
                if item.get('type') == 'tier':
                    tier_attrs = item.get('attributes', {})
                    title = tier_attrs.get('title', 'Unnamed Tier')
                    amount = tier_attrs.get('amount_cents', 0)
                    checkout_url = tier_attrs.get('url', '')

                    # Prevent making a "$0.00" tier if Patreon sends a ghost tier
                    if amount > 0:
                        # Auto-format the name as requested: "Network Name - Tier Name"
                        formatted_name = f"{network.name} - {title}"
                        
                        from .models import PatreonTier
                        tier, created = PatreonTier.objects.get_or_create(
                            network=network,
                            minimum_cents=amount,
                            defaults={
                                'name': formatted_name,
                                'checkout_url': checkout_url # <-- Save it
                            }
                        )
                        if created: tiers_created += 1

            messages.success(request, f"Successfully linked Campaign! Auto-imported {tiers_created} reward tiers.")
            
            # Kick off the async patron sync
            threading.Thread(target=sync_network_patrons, args=(network,), daemon=True).start()
        else:
            messages.warning(request, "Linked, but no campaigns found on your Patreon account.")
    else:
        logger.error(f"Failed to fetch campaigns during linking: {camp_res.text}")
        messages.error(request, "Failed to fetch your campaigns from Patreon.")
        
    return redirect('creator_settings')

def _fetch_patreon_identity(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    identity_url = (
        "https://www.patreon.com/api/oauth2/v2/identity"
        "?include=memberships.campaign"
        "&fields[user]=email,first_name,last_name,image_url,social_connections"
        "&fields[member]=patron_status,currently_entitled_amount_cents,pledge_relationship_start"
    )
    res = requests.get(identity_url, headers=headers, timeout=10)
    if res.status_code != 200:
        return None, HttpResponse("Failed to fetch user info", status=400)
    return res.json(), None

def patreon_login(request):
    network_id = request.GET.get('network_id')
    dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')
    
    scope = "identity identity[email] identity.memberships campaigns campaigns.members campaigns.members[email]" if network_id else "identity identity[email] identity.memberships"
    
    params = {
        "response_type": "code",
        "client_id": settings.PATREON_CLIENT_ID,
        "redirect_uri": dynamic_redirect_uri,
        "scope": scope,
    }
    if network_id: params["state"] = network_id
    return redirect(f"https://www.patreon.com/oauth2/authorize?{urllib.parse.urlencode(params)}")

def patreon_callback(request):
    code = request.GET.get('code')
    state_network_id = request.GET.get('state')

    if not code: return HttpResponse("No code provided by Patreon", status=400)

    try:
        dynamic_redirect_uri = request.build_absolute_uri('/oauth/patreon/callback')
        token_data, error_response = _exchange_patreon_token(code, dynamic_redirect_uri)
        if error_response: return error_response

        access_token = token_data['access_token']
        refresh_token = token_data['refresh_token']
        
        if state_network_id and request.user.is_authenticated:
            return _link_creator_campaign(request, state_network_id, access_token, refresh_token)

        payload, error_response = _fetch_patreon_identity(access_token)
        if error_response: return error_response
        
        user_data = payload.get('data', {})
        included_data = payload.get('included', [])
        email = user_data.get('attributes', {}).get('email')

        if not email: return HttpResponse("Patreon did not provide an email address.", status=400)

        user, created = User.objects.get_or_create(username=email, defaults={
            'email': email,
            'first_name': user_data.get('attributes', {}).get('first_name', ''),
            'last_name': user_data.get('attributes', {}).get('last_name', '')
        })

        _sync_patron_profile(user, user_data, included_data, current_network=request.network)
        login(request, user)
        return redirect('home')

    except Exception as e:
        logger.error(f"Critical error during Patreon callback: {str(e)}", exc_info=True)
        return HttpResponse(f"Error: {str(e)}", status=500)

def logout_view(request):
    from django.contrib.auth import logout
    logout(request)
    return redirect('home')

def request_magic_link(request):
    if request.method == 'POST':
        email = request.POST.get('email', '').strip()
        client = recurly.Client(settings.RECURLY_API_KEY)
        
        try:
            logger.info(f"[Recurly Auth] Lookup initiated for email: {email}")
            
            # FIX 1: Pass the email inside the params dictionary
            accounts = client.list_accounts(params={'email': email})
            account_id = None
            
            # Safely grab the first account ID that matches the email
            for acc in accounts.items():
                account_id = acc.id
                break
                
            if account_id:
                signer = TimestampSigner()
                payload = f"{email}|{account_id}" 
                token = signer.sign(payload)
                
                base_url = request.build_absolute_uri('/')[:-1]
                magic_link = f"{base_url}{reverse('verify_magic_link', args=[token])}"
                
                task_send_magic_link.delay(email, magic_link)
                logger.info(f"[Recurly Auth] Magic link generated & queued for: {email} (Acc ID: {account_id})")
            else:
                logger.warning(f"[Recurly Auth] Login failed: No Recurly account found for {email}")
                
            messages.success(request, "If an active subscription exists, a login link has been sent to your email.")
            
        # FIX 2: Catch standard Exceptions to prevent module AttributeError crashes
        except Exception as e:
            logger.error(f"[Recurly Auth] Error during account lookup for {email}: {e}")
            messages.error(request, "Unable to verify subscription at this time due to a server error.")
            
    return render(request, 'pod_manager/login_request.html')

def verify_magic_link(request, token):
    signer = TimestampSigner()
    try:
        # Decrypt token (900 seconds = 15 min expiration)
        payload = signer.unsign(token, max_age=900)
        email, account_id = payload.split('|')
        logger.info(f"[Recurly Auth] Magic link clicked and validated for: {email}")
    except SignatureExpired:
        logger.warning("[Recurly Auth] Expired magic link clicked.")
        messages.error(request, "This login link has expired. Please request a new one.")
        return redirect('request_magic_link')
    except BadSignature:
        logger.warning("[Recurly Auth] Invalid/tampered magic link clicked.")
        messages.error(request, "Invalid login link.")
        return redirect('request_magic_link')

    # 1. Identity Link
    user, _ = User.objects.get_or_create(username=email, defaults={'email': email})
    profile, _ = PatronProfile.objects.get_or_create(user=user)
    
    if profile.recurly_account_code != account_id:
        profile.recurly_account_code = account_id
        profile.save()
        logger.info(f"[Recurly Auth] Linked Recurly ID {account_id} to user {email}")

    # 2. Database Sync (The Recurly Bridge)
    client = recurly.Client(settings.RECURLY_API_KEY)
    active_plans = []
    
    try:
        subs = client.list_account_subscriptions(account_id=account_id)
        for sub in subs.items():
            if sub.state in ['active', 'in_trial', 'past_due']:
                active_plans.append(sub.plan.code)
                
        logger.info(f"[Recurly Auth] Sync successful. Active plans for {email}: {active_plans}")
        
        # Sync these plans to the user's NetworkMemberships
        # (For this proof of concept, we map them globally across all networks)
        for network in Network.objects.all():
            membership, _ = NetworkMembership.objects.get_or_create(user=user, network=network)
            membership.active_recurly_plans = active_plans
            membership.save()
            
    except Exception as e:
        logger.error(f"[Recurly Auth] Failed to fetch subscriptions for {email}: {e}")
        messages.warning(request, "Logged in, but could not sync your latest subscription data.")

    # 3. Authenticate
    login(request, user)
    messages.success(request, "Successfully logged in!")
    return redirect('user_feeds') # Adjust to whatever your post-login dashboard URL name is

@staff_member_required
def start_impersonation(request, user_id):
    target_user = get_object_or_404(User, id=user_id)
    if target_user.is_superuser:
        messages.error(request, "Security restriction: You cannot impersonate a superuser.")
        return redirect('admin:auth_user_changelist')

    if target_user == request.user:
        messages.warning(request, "You are already logged in as yourself.")
        return redirect('admin:auth_user_changelist')

    request.session['impersonated_user_id'] = target_user.id
    messages.success(request, f"Now viewing site as {target_user.email}.")
    return redirect('home')

def stop_impersonation(request):
    if 'impersonated_user_id' in request.session:
        del request.session['impersonated_user_id']
        messages.success(request, "Impersonation ended. Welcome back.")
    return redirect('admin:auth_user_changelist')

# ==========================================
# 9. UTILITIES & API
# ==========================================

def get_live_user_stats(tenant_profile):
    import redis
    import logging
    from django.conf import settings
    from django.utils import timezone
    from datetime import timedelta
    from collections import defaultdict
    from .models import Episode, Podcast
    from .tasks import parse_duration_to_hours
    
    logger = logging.getLogger(__name__)

    live_play_hits = tenant_profile.total_playback_hits or 0
    live_hours = tenant_profile.total_hours_accessed or 0.0
    live_streak_days = tenant_profile.streak_days or 0
    live_streak_weeks = tenant_profile.streak_weeks or 0
    live_obsession_title = tenant_profile.current_obsession.title if tenant_profile.current_obsession else "Wandering Adventurer"
    
    today = timezone.now().date()
    current_iso_week = today.isocalendar()[1]

    cache_backend = settings.CACHES['default'].get('BACKEND', '').lower()
    if 'locmem' in cache_backend or 'dummy' in cache_backend:
        return {
            'playback_hits': live_play_hits, 'hours_accessed': round(live_hours, 2),
            'streak_days': live_streak_days, 'streak_weeks': live_streak_weeks,
            'obsession_title': live_obsession_title
        }
        
    try:
        redis_url = settings.CACHES['default']['LOCATION']
        redis_client = redis.from_url(redis_url)
        
        global_user_id = tenant_profile.user.patron_profile.id
        play_keys = redis_client.keys(f"*analytics:play:{global_user_id}:*")
        pending_episode_ids = set()
        podcast_hits = defaultdict(int)
        
        for key_bytes in play_keys:
            hits = redis_client.get(key_bytes)
            if hits:
                key_str = key_bytes.decode('utf-8')
                clean_key = key_str.split('analytics:play:')[-1]
                parts = clean_key.split(':')
                if len(parts) == 3:
                    e_id, pod_id = int(parts[1]), int(parts[2])
                    if Podcast.objects.filter(id=pod_id, network=tenant_profile.network).exists():
                        live_play_hits += int(hits)
                        pending_episode_ids.add(e_id)
                        podcast_hits[pod_id] += int(hits)
                    
        if pending_episode_ids:
            episodes = Episode.objects.filter(id__in=pending_episode_ids)
            for ep in episodes:
                live_hours += parse_duration_to_hours(ep.duration)
                
        if podcast_hits:
            top_pod_id = max(podcast_hits, key=podcast_hits.get)
            obsession_pod = Podcast.objects.filter(id=top_pod_id).first()
            if obsession_pod:
                live_obsession_title = obsession_pod.title

    except Exception as e:
        logger.error(f"Failed to fetch live stats from Redis: {e}")
        
    return {
        'playback_hits': live_play_hits,
        'hours_accessed': round(live_hours, 2),
        'streak_days': live_streak_days, 
        'streak_weeks': live_streak_weeks,
        'obsession_title': live_obsession_title
    }

def invalidate_show_cache(show_id: int):
    version_key = f"podcast_cache_version_{show_id}"
    try:
        cache.incr(version_key)
    except ValueError:
        cache.set(version_key, 1, timeout=None)

def refresh_patreon_token(network):
    if not network.patreon_creator_refresh_token: return False

    token_url = "https://www.patreon.com/api/oauth2/token"
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': network.patreon_creator_refresh_token,
        'client_id': settings.PATREON_CLIENT_ID,
        'client_secret': settings.PATREON_CLIENT_SECRET,
    }
    
    try:
        res = requests.post(token_url, data=data, timeout=10)
        if res.status_code == 200:
            tokens = res.json()
            network.patreon_creator_access_token = tokens['access_token']
            if 'refresh_token' in tokens: network.patreon_creator_refresh_token = tokens['refresh_token']
            network.save()
            return True
        return False
    except Exception:
        return False

def traefik_config_api(request):
    expected_token = getattr(settings, 'TRAEFIK_API_TOKEN', None)
    if request.GET.get('token') != expected_token: return HttpResponseForbidden("Unauthorized access.")

    routers = {}
    networks = Network.objects.exclude(custom_domain__isnull=True).exclude(custom_domain__exact='')

    for network in networks:
        routers[f"custom-domain-{network.id}"] = {
            "rule": f"Host(`{network.custom_domain}`)",
            "entryPoints": ["https"], 
            "service": "vecto-service@file",
            "tls": {"certResolver": "http_resolver"}
        }

    return JsonResponse({"http": {"routers": routers}})

@login_required(login_url='/login/')
def stream_feed_import(request, show_id):
    task_id = f"import_logs_{show_id}"
    
    if not cache.get(task_id):
        cache.set(task_id, "data: [QUEUED] Waiting for Celery worker...\n\n", timeout=3600)
        task_ingest_feed.delay(show_id)

    async def event_stream():
        last_length = 0
        while True:
            logs = await cache.aget(task_id, "")
            if len(logs) > last_length:
                new_logs = logs[last_length:]
                yield new_logs
                last_length = len(logs)
                if "[DONE]" in new_logs:
                    await cache.adelete(task_id)
                    break
            await asyncio.sleep(0.5)

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')