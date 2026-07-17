"""
Security-focused test pass for the helpers and view changes added in the
recent hardening pass: signed OAuth state, SSRF guard, rate limiting, OTP
attempt accounting, and HTML sanitization. Plus a couple of view-level
integration tests for the Recurly login rate limits.

Transcription tests are appended at the bottom of this file, covering:
  - transcript_path() path helper and bucket math
  - Timestamp formatters (_vtt_timestamp, _srt_timestamp)
  - Format converters (_to_vtt, _to_srt, _to_html, _to_podcast_index_json,
    _to_words_json, _plain_text)
  - Response parser (_parse_whisper_response / _parse_srt)
  - Transcript model (get_url, auto_delete_transcript_files signal)
  - queue_transcription_on_episode_save signal
  - run_transcription() service (mocked whisper, option fallback chain)
  - apply_speaker_labels() (file rewrite, graceful no-op on missing data)
  - serve_transcript view (ETag, caching, Content-Disposition, 404)
  - backfill_transcripts_api (auth, eligibility filter, IDE vs Celery path)
  - retranscribe_episode_api (owner gate, state reset, IDE vs Celery path)
  - RSS feed transcript tags (_finalize_xml inserts podcast:transcript elements)
  - apply_approved_edit() speaker_mappings branch (early return, no metadata lock)
  - handle_update_network / handle_update_show whisper field persistence

Run with: python manage.py test pod_manager
"""
import json
import re
import shutil
import datetime
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from unittest import mock

from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import Http404
from django.test import Client, SimpleTestCase, TestCase, RequestFactory, override_settings
from django.urls import reverse
from django.utils import timezone

from pod_manager import views
from pod_manager.models import (
    CalendarEntry, Network, PatronProfile, PatreonTier, Podcast, Episode, EpisodeCrossPublication,
    NetworkMembership, NetworkMix, UserMix, EpisodeEditSuggestion, Transcript,
    R2OrphanedObject, LogEntry, NotFoundEntry,
)
from pod_manager.services.edits import (
    apply_approved_edit, chapter_items, parse_chapter_payload, snapshot_episode,
    update_contribution_stats, REJECT_PENALTY,
)
from pod_manager.views.creator.data import SHOW_PAGE_SIZE
from pod_manager.services.episode_move import move_episodes
from pod_manager.services.release_calendar import (
    ensure_calendar_entry_for_episode,
    link_calendar_entry_for_new_episode,
    match_calendar_entry,
)
from pod_manager.services.transcription import (
    _parse_srt,
    _parse_srt_timestamp,
    _parse_whisper_response,
    _plain_text,
    _srt_timestamp,
    _to_html,
    _to_podcast_index_json,
    _to_srt,
    _to_vtt,
    _to_words_json,
    _vtt_timestamp,
    apply_speaker_labels,
    transcript_path,
    transcript_r2_key,
    write_transcript_formats,
)


# Use locmem so tests don't depend on Redis being up.
TEST_CACHES = {'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'}}


@override_settings(CACHES=TEST_CACHES)
class ValidatePublicUrlTests(TestCase):
    """SSRF guard: only public, http(s) hosts are allowed through."""

    def test_rejects_empty_and_non_string(self):
        self.assertEqual(views.validate_public_url('')[0], False)
        self.assertEqual(views.validate_public_url(None)[0], False)
        self.assertEqual(views.validate_public_url(12345)[0], False)

    def test_rejects_non_http_schemes(self):
        for url in ['javascript:alert(1)', 'file:///etc/passwd', 'ftp://example.com', 'gopher://x']:
            ok, _ = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}")

    def test_rejects_loopback(self):
        for url in ['http://127.0.0.1/x', 'http://localhost/x', 'http://[::1]/x']:
            ok, reason = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}: {reason}")

    def test_rejects_aws_metadata(self):
        # 169.254.169.254 is link-local — primary cloud-metadata SSRF target.
        ok, reason = views.validate_public_url('http://169.254.169.254/latest/meta-data/')
        self.assertFalse(ok)
        self.assertIn('non-public', reason.lower())

    def test_rejects_private_ranges(self):
        for url in ['http://10.0.0.5/', 'http://192.168.1.1/', 'http://172.16.0.1/']:
            ok, _ = views.validate_public_url(url)
            self.assertFalse(ok, f"Expected reject for {url}")

    def test_rejects_unresolvable_host(self):
        ok, reason = views.validate_public_url('http://this-host-should-never-resolve.invalid/')
        self.assertFalse(ok)
        self.assertIn('resolved', reason.lower())

    def test_accepts_public_url(self):
        # Use mock to avoid real DNS during CI.
        with mock.patch('pod_manager.utils.socket.getaddrinfo',
                        return_value=[(2, 1, 6, '', ('93.184.216.34', 0))]):
            ok, _ = views.validate_public_url('https://example.com/path')
            self.assertTrue(ok)

    def test_dns_rebinding_defense(self):
        # If ANY resolved address is private, reject. Defends against a host
        # that resolves to a public address and a private address simultaneously.
        with mock.patch('pod_manager.utils.socket.getaddrinfo',
                        return_value=[
                            (2, 1, 6, '', ('93.184.216.34', 0)),
                            (2, 1, 6, '', ('10.0.0.1', 0)),
                        ]):
            ok, _ = views.validate_public_url('https://shady.example.com/')
            self.assertFalse(ok)


@override_settings(CACHES=TEST_CACHES)
class RateLimitTests(TestCase):

    def setUp(self):
        cache.clear()

    def test_under_limit_returns_false(self):
        for _ in range(3):
            self.assertFalse(views._is_rate_limited('test_bucket', limit=5, window_seconds=60))

    def test_over_limit_returns_true(self):
        for _ in range(5):
            views._is_rate_limited('test_bucket', limit=5, window_seconds=60)
        # 6th call should be over.
        self.assertTrue(views._is_rate_limited('test_bucket', limit=5, window_seconds=60))

    def test_buckets_are_independent(self):
        for _ in range(5):
            views._is_rate_limited('bucket_a', limit=5, window_seconds=60)
        # bucket_a is at limit, bucket_b is fresh.
        self.assertTrue(views._is_rate_limited('bucket_a', limit=5, window_seconds=60))
        self.assertFalse(views._is_rate_limited('bucket_b', limit=5, window_seconds=60))


@override_settings(CACHES=TEST_CACHES)
class OtpAttemptTests(TestCase):

    def setUp(self):
        cache.clear()

    def test_record_failure_increments(self):
        self.assertEqual(views._record_otp_failure('a@b.com'), 1)
        self.assertEqual(views._record_otp_failure('a@b.com'), 2)
        self.assertEqual(views._record_otp_failure('a@b.com'), 3)

    def test_clear_state_burns_all_keys(self):
        cache.set('recurly_otp_a@b.com', 'ABC123|acct_1', timeout=600)
        cache.set('recurly_account_a@b.com', 'acct_1', timeout=600)
        views._record_otp_failure('a@b.com')
        views._record_otp_failure('a@b.com')

        views._clear_otp_state('a@b.com')

        self.assertIsNone(cache.get('recurly_otp_a@b.com'))
        self.assertIsNone(cache.get('recurly_account_a@b.com'))
        self.assertIsNone(cache.get('recurly_otp_attempts:a@b.com'))


class OauthStateSigningTests(TestCase):

    def test_round_trip(self):
        signed = views._sign_oauth_state('link:42:7')
        self.assertEqual(views._unsign_oauth_state(signed), 'link:42:7')

    def test_returns_none_on_missing(self):
        self.assertIsNone(views._unsign_oauth_state(''))
        self.assertIsNone(views._unsign_oauth_state(None))

    def test_returns_none_on_tampered(self):
        signed = views._sign_oauth_state('link:42:7')
        # Flip a payload byte: signature no longer matches.
        tampered = signed.replace('42', '99', 1)
        self.assertIsNone(views._unsign_oauth_state(tampered))

    def test_returns_none_on_expired(self):
        signed = views._sign_oauth_state('link:42:7')
        # max_age=0 means anything older than now is expired.
        # Sleep a hair to ensure the timestamp is "in the past".
        time.sleep(1.01)
        self.assertIsNone(views._unsign_oauth_state(signed, max_age_seconds=1))


class SanitizeUserHtmlTests(TestCase):

    def test_strips_script(self):
        out = views.sanitize_user_html('<p>hi</p><script>alert(1)</script>')
        self.assertNotIn('<script>', out)
        self.assertIn('<p>hi</p>', out)

    def test_strips_event_handlers(self):
        out = views.sanitize_user_html('<img src="x" onerror="alert(1)">')
        self.assertNotIn('onerror', out)

    def test_allows_safe_formatting(self):
        out = views.sanitize_user_html('<p><strong>bold</strong> <em>italic</em></p>')
        self.assertIn('<strong>bold</strong>', out)
        self.assertIn('<em>italic</em>', out)

    def test_links_get_safe_rel(self):
        out = views.sanitize_user_html('<a href="https://example.com">x</a>')
        self.assertIn('rel=', out)
        self.assertIn('noopener', out)

    def test_empty_input_safe(self):
        self.assertEqual(views.sanitize_user_html(''), '')
        self.assertEqual(views.sanitize_user_html(None), '')


@override_settings(CACHES=TEST_CACHES,
                   RECURLY_API_KEY='test-key',
                   PATREON_CLIENT_ID='test-id',
                   PATREON_CLIENT_SECRET='test-secret')
class RecurlyLoginRateLimitTests(TestCase):
    """
    Integration coverage for the rate limits added to /login/legacy/.
    Stubs out the Recurly client and the OTP-email Celery task so the test
    exercises only the rate-limit logic.
    """

    def setUp(self):
        cache.clear()
        # Networks are required because views resolve `request.network` via
        # middleware. We bypass middleware by calling the view directly with
        # a stub request that already has `.network` attached.
        self.network = Network.objects.create(name='Test Net', slug='test')
        self.factory = RequestFactory()

    def _post(self, data, ip='1.2.3.4'):
        req = self.factory.post(reverse('recurly_login'), data=data,
                                HTTP_X_FORWARDED_FOR=ip)
        # Attach what the middlewares would normally provide.
        req.network = self.network
        # Manually wire session + messages so the view doesn't blow up.
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        return req

    @mock.patch('pod_manager.views.recurly.Client')
    @mock.patch('pod_manager.tasks.task_send_otp_email')
    def test_per_ip_rate_limit_blocks_after_10(self, mock_task, mock_client_cls):
        # Recurly returns "no account found" so we don't accidentally trigger
        # an OTP send on every request — the rate limit should fire either way.
        mock_client_cls.return_value.list_accounts.return_value.items.return_value = iter([])

        for i in range(10):
            req = self._post({'email': f'user{i}@example.com'}, ip='9.9.9.9')
            resp = views.recurly_login(req)
            self.assertEqual(resp.status_code, 302)

        # 11th request from same IP, fresh email: rate-limited before the
        # Recurly call. Verify by checking the call count didn't go up.
        before_calls = mock_client_cls.return_value.list_accounts.call_count
        req = self._post({'email': 'user-final@example.com'}, ip='9.9.9.9')
        resp = views.recurly_login(req)
        self.assertEqual(resp.status_code, 302)
        after_calls = mock_client_cls.return_value.list_accounts.call_count
        self.assertEqual(after_calls, before_calls,
                         "Expected the 11th request to be blocked before Recurly was called.")

    @mock.patch('pod_manager.views.recurly.Client')
    def test_per_email_rate_limit_blocks_after_5(self, mock_client_cls):
        mock_client_cls.return_value.list_accounts.return_value.items.return_value = iter([])

        # 5 attempts on the same email from different IPs (so the IP limit
        # never triggers) — limit is 5 so the 6th must be blocked.
        for i in range(5):
            req = self._post({'email': 'victim@example.com'}, ip=f'10.0.0.{i+1}')
            views.recurly_login(req)

        before_calls = mock_client_cls.return_value.list_accounts.call_count
        req = self._post({'email': 'victim@example.com'}, ip='10.0.0.99')
        views.recurly_login(req)
        after_calls = mock_client_cls.return_value.list_accounts.call_count
        self.assertEqual(after_calls, before_calls,
                         "Expected the 6th per-email request to short-circuit before Recurly.")


@override_settings(CACHES=TEST_CACHES, RECURLY_API_KEY='test-key')
class RecurlyOtpAttemptTests(TestCase):
    """The OTP cache entry is burned after MAX_OTP_ATTEMPTS bad guesses."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(name='Test Net', slug='test')
        self.user = User.objects.create_user(username='victim@example.com',
                                             email='victim@example.com')
        # Pre-seed the OTP into the cache as if it were just sent.
        cache.set('recurly_otp_victim@example.com', '111111|acct_1', timeout=600)
        self.factory = RequestFactory()

    def _post(self, otp):
        req = self.factory.post(reverse('recurly_login'), data={'otp': otp})
        req.network = self.network
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        req.session['recurly_login'] = {
            'state': 'awaiting_email',
            'email': 'victim@example.com',
            'account_id': 'acct_1',
            'is_second_factor': False,
        }
        req.session.save()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        return req

    def test_otp_burned_after_max_failures(self):
        # 5 wrong guesses must burn the cache entry.
        for _ in range(views.MAX_OTP_ATTEMPTS):
            req = self._post('000000')
            views.recurly_login(req)
        self.assertIsNone(cache.get('recurly_otp_victim@example.com'),
                          "OTP should be cleared after MAX_OTP_ATTEMPTS failures.")

    def test_correct_otp_still_works_within_attempts(self):
        # Two wrong guesses, then the right one — should still succeed.
        with mock.patch('pod_manager.services.recurly.Client') as mock_client_cls:
            # Stub list_account_subscriptions so the success path doesn't blow up.
            mock_client_cls.return_value.list_account_subscriptions.return_value.items.return_value = iter([])

            req = self._post('999999')
            views.recurly_login(req)
            req = self._post('888888')
            views.recurly_login(req)
            # Cache should still have the OTP at this point.
            self.assertIsNotNone(cache.get('recurly_otp_victim@example.com'))

            # Now the correct code: should clear cache + create profile.
            req = self._post('111111')
            views.recurly_login(req)
            self.assertIsNone(cache.get('recurly_otp_victim@example.com'))
            self.assertTrue(PatronProfile.objects.filter(user=self.user).exists())


# ---------------------------------------------------------------------------
# Helpers used by multiple integration test classes below
# ---------------------------------------------------------------------------

def _make_tenant_request(factory, network, *, method='get', path='/feed/',
                        data=None, user=None):
    """Build a request that already has request.network attached, bypassing
    NetworkMiddleware so view-level tests don't need to install hosts."""
    if method == 'post':
        req = factory.post(path, data=data or {})
    else:
        req = factory.get(path, data=data or {})
    req.network = network
    req.tenant_profile = None
    from django.contrib.sessions.backends.cache import SessionStore
    req.session = SessionStore()
    req.session.create()
    from django.contrib.messages.storage.fallback import FallbackStorage
    setattr(req, '_messages', FallbackStorage(req))
    if user is not None:
        req.user = user
    else:
        from django.contrib.auth.models import AnonymousUser
        req.user = AnonymousUser()
    return req


def _tiny_image_upload(name='pic.png'):
    """A minimal real PNG, so ImageField/Pillow-backed save() paths (which
    open() the upload with Pillow) don't choke on garbage bytes."""
    import io
    from PIL import Image
    buf = io.BytesIO()
    Image.new('RGB', (10, 10), color='red').save(buf, format='PNG')
    return SimpleUploadedFile(name, buf.getvalue(), content_type='image/png')


def _tiny_gif_upload(name='anim.gif', frames=3):
    """A minimal real animated GIF (distinct frames, 100ms apart)."""
    import io
    from PIL import Image
    imgs = [Image.new('RGB', (10, 10), color=c) for c in ('red', 'lime', 'blue')[:frames]]
    buf = io.BytesIO()
    imgs[0].save(buf, format='GIF', save_all=True, append_images=imgs[1:],
                 duration=100, loop=0)
    return SimpleUploadedFile(name, buf.getvalue(), content_type='image/gif')


class PendingApprovalsContextProcessorTests(TestCase):
    """pending_approvals(): badge count aggregated across owned networks, not
    scoped to request.network (which is often None on the admin console)."""

    def setUp(self):
        self.factory = RequestFactory()
        self.owner = User.objects.create_user('owner', password='x')
        self.other_owner = User.objects.create_user('other_owner', password='x')
        self.superuser = User.objects.create_superuser('root', password='x')

        self.network_a = Network.objects.create(name='A', slug='a')
        self.network_b = Network.objects.create(name='B', slug='b')
        self.network_a.owners.add(self.owner)
        self.network_b.owners.add(self.owner)
        self.other_network = Network.objects.create(name='Other', slug='other')
        self.other_network.owners.add(self.other_owner)

        podcast_a = Podcast.objects.create(network=self.network_a, title='A Show', slug='a-show')
        podcast_b = Podcast.objects.create(network=self.network_b, title='B Show', slug='b-show')
        other_podcast = Podcast.objects.create(network=self.other_network, title='O Show', slug='o-show')

        def _ep(podcast, n):
            return Episode.objects.create(
                podcast=podcast, title=f'Ep {n}', pub_date=timezone.now(),
                raw_description='x', clean_description='x',
            )

        for ep, status in (
            (_ep(podcast_a, 1), EpisodeEditSuggestion.Status.PENDING),
            (_ep(podcast_b, 2), EpisodeEditSuggestion.Status.PENDING),
            (_ep(podcast_a, 3), EpisodeEditSuggestion.Status.APPROVED),
            (_ep(other_podcast, 4), EpisodeEditSuggestion.Status.PENDING),
        ):
            EpisodeEditSuggestion.objects.create(episode=ep, user=self.owner, status=status)

    def _ctx(self, user):
        from pod_manager.context_processors import pending_approvals
        req = _make_tenant_request(self.factory, None, user=user)
        return pending_approvals(req)

    def test_anonymous_gets_no_key(self):
        from django.contrib.auth.models import AnonymousUser
        self.assertEqual(self._ctx(AnonymousUser()), {})

    def test_owner_count_aggregates_across_owned_networks_only(self):
        # 2 pending across network_a + network_b; the 4th (other_network) must
        # NOT be counted even though request.network is None here.
        self.assertEqual(self._ctx(self.owner), {'pending_approval_count': 2})

    def test_owner_with_no_networks_gets_no_key(self):
        bystander = User.objects.create_user('bystander', password='x')
        self.assertEqual(self._ctx(bystander), {})

    def test_superuser_counts_every_network(self):
        self.assertEqual(self._ctx(self.superuser), {'pending_approval_count': 3})

    def test_other_owner_only_sees_their_own_network(self):
        self.assertEqual(self._ctx(self.other_owner), {'pending_approval_count': 1})


@override_settings(CACHES=TEST_CACHES)
class FeedEtagStabilityTests(TestCase):
    """generate_custom_feed and generate_public_feed must produce the same
    ETag across successive calls, even when the shell cache is rebuilt
    between them. Otherwise every podcast app downloads the full feed body
    on every poll."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        # Two episodes so there's at least one item to hash.
        for i in range(2):
            Episode.objects.create(
                podcast=self.podcast,
                title=f"Ep {i}",
                pub_date=timezone.now() - timedelta(days=i),
                raw_description='hello',
                clean_description='hello',
                audio_url_public='https://cdn.example.com/audio.mp3',
            )

    def _get_etag(self, view, **kwargs):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        resp = view(req, **kwargs)
        return resp.get('ETag')

    def test_public_feed_etag_stable_across_shell_cache_rebuild(self):
        first = self._get_etag(views.generate_public_feed, podcast_slug='show')
        # Wipe the shell cache to simulate a podcast-update rebuild or
        # autoreload. Episode fragments stay so we're really testing that
        # lastBuildDate is pinned to episode pub_date, not "now".
        cache.delete(f"feed_shell_public_{self.podcast.id}")
        second = self._get_etag(views.generate_public_feed, podcast_slug='show')
        self.assertIsNotNone(first)
        self.assertEqual(first, second,
                         "ETag must not change when the shell cache is rebuilt.")

    def test_custom_feed_etag_stable_across_shell_cache_rebuild(self):
        user = User.objects.create_user(username='listener')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)

        def get_etag():
            req = self.factory.get('/feed/', {
                'auth': str(profile.feed_token),
                'show': 'show',
            })
            req.network = self.network
            return views.generate_custom_feed(req).get('ETag')

        first = get_etag()
        cache.delete(f"feed_shell_public_{self.podcast.id}")
        cache.delete(f"feed_shell_private_{self.podcast.id}")
        second = get_etag()
        self.assertEqual(first, second)

    def test_public_feed_returns_304_on_etag_match(self):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        resp = views.generate_public_feed(req, podcast_slug='show')
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        req2 = _make_tenant_request(self.factory, self.network, path='/feed/')
        req2.META['HTTP_IF_NONE_MATCH'] = etag
        resp2 = views.generate_public_feed(req2, podcast_slug='show')
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    def test_custom_feed_returns_304_on_etag_match(self):
        user = User.objects.create_user(username='listener')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)

        def make_req(etag=None):
            req = self.factory.get('/feed/', {
                'auth': str(profile.feed_token),
                'show': 'show',
            })
            req.network = self.network
            if etag:
                req.META['HTTP_IF_NONE_MATCH'] = etag
            return req

        resp = views.generate_custom_feed(make_req())
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_custom_feed(make_req(etag))
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    # ── UserMix feed ──────────────────────────────────────────────────────

    def _make_user_mix(self):
        user = User.objects.create_user(username='mixowner')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)
        mix = UserMix.objects.create(
            user=user, network=self.network, name='My Mix', is_active=True
        )
        mix.selected_podcasts.add(self.podcast)
        return mix, profile

    def _mix_req(self, mix, profile, etag=None):
        req = self.factory.get(
            f'/feed/mix/{mix.unique_id}',
            {'auth': str(profile.feed_token)},
        )
        if etag:
            req.META['HTTP_IF_NONE_MATCH'] = etag
        return req

    def test_user_mix_feed_etag_stable_across_shell_cache_rebuild(self):
        mix, profile = self._make_user_mix()
        first = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id).get('ETag')
        cache.delete(f"shell_user_mix_{mix.id}")
        second = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id).get('ETag')
        self.assertIsNotNone(first)
        self.assertEqual(first, second, "UserMix ETag must not change when the shell cache is rebuilt.")

    def test_user_mix_feed_returns_304_on_etag_match(self):
        mix, profile = self._make_user_mix()
        resp = views.generate_mix_feed(self._mix_req(mix, profile), unique_id=mix.unique_id)
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_mix_feed(self._mix_req(mix, profile, etag=etag), unique_id=mix.unique_id)
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)

    # ── NetworkMix feed ───────────────────────────────────────────────────

    def _make_network_mix(self):
        mix = NetworkMix.objects.create(
            network=self.network, name='Net Mix', slug='netmix', required_tier=None
        )
        mix.selected_podcasts.add(self.podcast)
        return mix

    def _net_mix_req(self, etag=None):
        req = _make_tenant_request(self.factory, self.network, path='/feed/n/mix/netmix/')
        if etag:
            req.META['HTTP_IF_NONE_MATCH'] = etag
        return req

    def test_network_mix_feed_etag_stable_across_shell_cache_rebuild(self):
        mix = self._make_network_mix()
        first = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix').get('ETag')
        cache.delete(f"shell_net_mix_{mix.id}")
        second = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix').get('ETag')
        self.assertIsNotNone(first)
        self.assertEqual(first, second, "NetworkMix ETag must not change when the shell cache is rebuilt.")

    def test_network_mix_feed_returns_304_on_etag_match(self):
        self._make_network_mix()
        resp = views.generate_network_mix_feed(self._net_mix_req(), network_slug='n', mix_slug='netmix')
        etag = resp.get('ETag')
        self.assertIsNotNone(etag)
        self.assertEqual(resp.status_code, 200)

        resp2 = views.generate_network_mix_feed(self._net_mix_req(etag=etag), network_slug='n', mix_slug='netmix')
        self.assertEqual(resp2.status_code, 304)
        self.assertEqual(len(resp2.content), 0)


@override_settings(CACHES=TEST_CACHES)
class NetworkMixCrudTests(TestCase):
    """The creator_settings dispatcher gained add/edit/delete handlers for
    NetworkMix; verify each branch persists the right state."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner', email='owner@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.p1 = Podcast.objects.create(network=self.network, title='P1', slug='p1')
        self.p2 = Podcast.objects.create(network=self.network, title='P2', slug='p2')

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_add_network_mix_creates_row(self):
        self._post({
            'action': 'add_network_mix',
            'network_id': self.network.id,
            'name': 'My Net Mix',
            'slug': 'my-net-mix',
            'mix_image': '',
            'tier_id': '',
            'podcasts': [str(self.p1.id), str(self.p2.id)],
        })
        mix = NetworkMix.objects.get(network=self.network, slug='my-net-mix')
        self.assertEqual(mix.name, 'My Net Mix')
        self.assertEqual(set(mix.selected_podcasts.values_list('id', flat=True)),
                         {self.p1.id, self.p2.id})

    def test_edit_network_mix_persists_name_and_podcast_set(self):
        mix = NetworkMix.objects.create(network=self.network, name='Old', slug='old')
        mix.selected_podcasts.set([self.p1])

        self._post({
            'action': 'edit_network_mix',
            'mix_id': mix.id,
            'name': 'New Name',
            'slug': 'old',
            'mix_image': '',
            'tier_id': '',
            'podcasts': [str(self.p2.id)],   # swap p1 -> p2
        })
        mix.refresh_from_db()
        self.assertEqual(mix.name, 'New Name')
        self.assertEqual(list(mix.selected_podcasts.values_list('id', flat=True)),
                         [self.p2.id])

    def test_delete_network_mix_removes_row(self):
        mix = NetworkMix.objects.create(network=self.network, name='X', slug='x')
        self._post({'action': 'delete_network_mix', 'mix_id': mix.id})
        self.assertFalse(NetworkMix.objects.filter(id=mix.id).exists())


class NotFoundEntryCrudTests(TestCase):
    """creator_settings add/delete handlers for the 404-page image+caption
    pool: owner-gated (via the standard allowed_networks check) and scoped
    to the acting owner's network."""

    def setUp(self):
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner', email='owner@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)

        self.other_owner = User.objects.create_user(username='other', email='other@example.com')
        self.other_network = Network.objects.create(name='Other', slug='o')
        self.other_network.owners.add(self.other_owner)

    def _post(self, data, user=None):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=user or self.owner)
        return views.creator_settings(req)

    def test_add_notfound_entry_creates_row(self):
        self._post({
            'action': 'add_notfound_entry',
            'caption': "I'm sorry Dave, I'm afraid I can't do that.",
            'image_upload': _tiny_image_upload(),
        })
        entry = NotFoundEntry.objects.get(network=self.network)
        self.assertEqual(entry.caption, "I'm sorry Dave, I'm afraid I can't do that.")
        self.assertTrue(entry.image_upload)
        self.assertEqual(entry.image_version, 1)

    def test_animated_gif_becomes_animated_webp(self):
        from PIL import Image
        self._post({
            'action': 'add_notfound_entry',
            'caption': 'Deal with it.',
            'image_upload': _tiny_gif_upload(frames=3),
        })
        entry = NotFoundEntry.objects.get(network=self.network)
        with entry.image_upload.open('rb') as f:
            img = Image.open(f)
            self.assertEqual(img.format, 'WEBP')
            self.assertTrue(getattr(img, 'is_animated', False))
            self.assertEqual(img.n_frames, 3)

    def test_404_images_keep_full_frame_never_cropped(self):
        # 404 art often has text baked in — a wide upload must keep its aspect
        # ratio (only bounded to 800px), not get centre-cropped square.
        import io
        from PIL import Image
        buf = io.BytesIO()
        Image.new('RGB', (400, 100), color='red').save(buf, format='PNG')
        self._post({
            'action': 'add_notfound_entry',
            'caption': 'Wide boy.',
            'image_upload': SimpleUploadedFile('wide.png', buf.getvalue(), content_type='image/png'),
        })
        entry = NotFoundEntry.objects.get(network=self.network)
        with entry.image_upload.open('rb') as f:
            img = Image.open(f)
            self.assertEqual((img.width, img.height), (400, 100))

    def test_static_upload_stays_single_frame_webp(self):
        from PIL import Image
        self._post({
            'action': 'add_notfound_entry',
            'caption': 'Still here.',
            'image_upload': _tiny_image_upload(),
        })
        entry = NotFoundEntry.objects.get(network=self.network)
        with entry.image_upload.open('rb') as f:
            img = Image.open(f)
            self.assertEqual(img.format, 'WEBP')
            self.assertFalse(getattr(img, 'is_animated', False))

    def test_delete_notfound_entry_removes_row(self):
        entry = NotFoundEntry.objects.create(
            network=self.network, caption='Oh Snap!', image_upload=_tiny_image_upload(),
        )
        self._post({'action': 'delete_notfound_entry', 'entry_id': entry.id})
        self.assertFalse(NotFoundEntry.objects.filter(id=entry.id).exists())

    def test_delete_notfound_entry_scoped_to_network(self):
        foreign_entry = NotFoundEntry.objects.create(
            network=self.other_network, caption='Not yours', image_upload=_tiny_image_upload(),
        )
        with self.assertRaises(Http404):
            self._post({'action': 'delete_notfound_entry', 'entry_id': foreign_entry.id})
        self.assertTrue(NotFoundEntry.objects.filter(id=foreign_entry.id).exists())

    def test_non_owner_cannot_reach_creator_settings(self):
        outsider = User.objects.create_user(username='outsider', email='outsider@example.com')
        req = self.factory.post('/creator/', data={
            'action': 'add_notfound_entry', 'caption': 'x', 'image_upload': _tiny_image_upload(),
        })
        req.user = outsider
        resp = views.creator_settings(req)
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(NotFoundEntry.objects.filter(caption='x').exists())


def _woff2_upload(name='font.woff2', size=64):
    """A minimal fake .woff2: correct magic bytes, rest is padding."""
    return SimpleUploadedFile(name, b'wOF2' + b'\x00' * (size - 4), content_type='font/woff2')


class NetworkCustomFontTests(TestCase):
    """handle_update_network_font: extension/size/magic-byte validation,
    sanitization, overwrite-in-place on re-upload, and removal (Feature 5
    amendments A2/A3/A5)."""

    def setUp(self):
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner', email='owner@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)

    def _post(self, data, user=None):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=user or self.owner)
        return views.creator_settings(req)

    def test_valid_upload_accepted(self):
        self._post({
            'action': 'update_network_font',
            'font_upload': _woff2_upload(),
            'custom_font_family': 'Brand Sans',
        })
        self.network.refresh_from_db()
        self.assertTrue(self.network.custom_font_upload)
        self.assertEqual(self.network.custom_font_family, 'Brand Sans')

    def test_wrong_extension_rejected(self):
        self._post({
            'action': 'update_network_font',
            'font_upload': SimpleUploadedFile('font.ttf', b'wOF2' + b'\x00' * 60, content_type='font/ttf'),
            'custom_font_family': 'Brand Sans',
        })
        self.network.refresh_from_db()
        self.assertFalse(self.network.custom_font_upload)

    def test_oversize_rejected(self):
        self._post({
            'action': 'update_network_font',
            'font_upload': _woff2_upload(size=2 * 1024 * 1024 + 1),
            'custom_font_family': 'Brand Sans',
        })
        self.network.refresh_from_db()
        self.assertFalse(self.network.custom_font_upload)

    def test_bad_magic_bytes_rejected(self):
        self._post({
            'action': 'update_network_font',
            'font_upload': SimpleUploadedFile('font.woff2', b'NOPE' + b'\x00' * 60, content_type='font/woff2'),
            'custom_font_family': 'Brand Sans',
        })
        self.network.refresh_from_db()
        self.assertFalse(self.network.custom_font_upload)

    def test_reupload_overwrites_in_place(self):
        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload(size=64)})
        self.network.refresh_from_db()
        first_name = self.network.custom_font_upload.name

        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload(size=100)})
        self.network.refresh_from_db()
        self.assertEqual(self.network.custom_font_upload.name, first_name)
        self.assertEqual(self.network.custom_font_upload.size, 100)

    def test_upload_bumps_version_for_cache_busting(self):
        # The stable fonts/<slug>.woff2 key is CDN-cached immutable for a year;
        # each upload must bump the version so display_font_url changes.
        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload()})
        self.network.refresh_from_db()
        self.assertEqual(self.network.custom_font_version, 1)
        self.assertTrue(self.network.display_font_url.endswith('?v=1'))

        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload()})
        self.network.refresh_from_db()
        self.assertEqual(self.network.custom_font_version, 2)

    def test_family_only_save_does_not_bump_version(self):
        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload()})
        self.network.refresh_from_db()
        self.assertEqual(self.network.custom_font_version, 1)
        self._post({'action': 'update_network_font', 'custom_font_family': 'Renamed Sans'})
        self.network.refresh_from_db()
        self.assertEqual(self.network.custom_font_version, 1)

    def test_font_name_sanitization_strips_quotes(self):
        self._post({
            'action': 'update_network_font',
            'custom_font_family': '"Evil"; } body { display: none; } /*',
        })
        self.network.refresh_from_db()
        self.assertNotIn('"', self.network.custom_font_family)
        self.assertNotIn(';', self.network.custom_font_family)

    def test_remove_clears_both_fields(self):
        self._post({'action': 'update_network_font', 'font_upload': _woff2_upload(), 'custom_font_family': 'Brand Sans'})
        self.network.refresh_from_db()
        self.assertTrue(self.network.custom_font_upload)

        self._post({'action': 'update_network_font', 'remove': '1'})
        self.network.refresh_from_db()
        self.assertFalse(self.network.custom_font_upload)
        self.assertEqual(self.network.custom_font_family, '')


@override_settings(DEBUG=False, ALLOWED_HOSTS=['*'])
class Custom404ViewTests(TestCase):
    """handler404 -> pod_manager.views.errors.custom_404: themed pool pick,
    imageless themed fallback, and the no-network fallback (Feature 3
    amendment A5: themed cases route through NetworkMiddleware via a real
    HTTP_HOST matching Network.custom_domain)."""

    def setUp(self):
        cache.clear()  # tenant_custom_domains is cached for 60s across requests
        self.network = Network.objects.create(
            name='Themed Net', slug='themed', custom_domain='themed.example.test',
        )

    def test_renders_random_pool_entry_for_known_network(self):
        entry = NotFoundEntry.objects.create(
            network=self.network, caption='Oh Snap!', image_upload=_tiny_image_upload(),
        )
        resp = self.client.get('/this-path-does-not-exist/', HTTP_HOST='themed.example.test')
        self.assertEqual(resp.status_code, 404)
        self.assertContains(resp, 'Oh Snap!', status_code=404)
        self.assertContains(resp, entry.display_image, status_code=404)

    def test_renders_imageless_fallback_when_pool_empty(self):
        resp = self.client.get('/this-path-does-not-exist/', HTTP_HOST='themed.example.test')
        self.assertEqual(resp.status_code, 404)
        self.assertContains(resp, 'Themed Net', status_code=404)
        self.assertContains(resp, 'This episode was never recorded.', status_code=404)

    def test_renders_no_network_fallback_for_unmatched_domain(self):
        resp = self.client.get('/some/bogus/path/', HTTP_HOST='totally-unknown.example.test')
        self.assertEqual(resp.status_code, 404)
        self.assertContains(resp, 'Vecto', status_code=404)
        self.assertNotContains(resp, 'name="q"', status_code=404)


class EpisodeChapterMirrorTests(TestCase):
    """submit_episode_edit must mirror chapter writes to both the public
    and private columns so the private feed is never left null after an
    edit."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='trusted', email='trusted@example.com')
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=0)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast,
            title='Ep',
            pub_date=timezone.now(),
            raw_description='hi',
            clean_description='hi',
            audio_url_public='https://x/audio.mp3',
        )
        # Trust score >= threshold (0) → auto-approval path
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=10)

    def test_chapters_mirror_to_both_columns(self):
        import json
        payload = json.dumps({
            'title': 'Ep',
            'description': '<p>still hi</p>',
            'tags': [],
            'chapters': [{'startTime': 0.0, 'title': 'Intro'}],
        })
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': payload},
        )
        req.user = self.user
        req.network = self.network
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))

        views.submit_episode_edit(req, self.episode.id)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.chapters_public, self.episode.chapters_private,
                         "Chapters must be written to both columns.")
        # Sanity: chapters actually contain our entry.
        chap_list = (self.episode.chapters_public or {}).get('chapters', [])
        self.assertEqual(len(chap_list), 1)
        self.assertEqual(chap_list[0]['title'], 'Intro')


@override_settings(WHISPER_ENABLED=False)
class EpisodeDetailTemplateTests(TestCase):
    """The 'Show Ad-Supported Version' button must NOT render when there's
    no public audio URL — otherwise it loads an empty <audio> element."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='listener', email='listener@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        # Membership grants tier-free access.
        NetworkMembership.objects.create(user=self.user, network=self.network)

    def _render(self, public_url):
        ep = Episode.objects.create(
            podcast=self.podcast,
            title='Ep',
            pub_date=timezone.now(),
            raw_description='hi',
            clean_description='hi',
            audio_url_public=public_url,
            audio_url_subscriber='https://cdn.example.com/private.mp3',
        )
        # Bypass NetworkMiddleware by calling the view directly with a
        # request that already has request.network attached. Avoids the
        # ALLOWED_HOSTS dance that the test client requires.
        req = _make_tenant_request(RequestFactory(), self.network,
                                   path=f'/episode/{ep.id}/', user=self.user)
        resp = views.episode_detail(req, ep.id)
        return resp.content.decode('utf-8')

    def test_button_hidden_when_no_public_audio(self):
        body = self._render(public_url='')
        self.assertNotIn('Show Ad-Supported Version', body)

    def test_button_shown_when_public_audio_present(self):
        body = self._render(public_url='https://cdn.example.com/public.mp3')
        self.assertIn('Show Ad-Supported Version', body)

    def _render_episode(self, **fields):
        ep = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='hi', clean_description='hi', **fields,
        )
        req = _make_tenant_request(RequestFactory(), self.network,
                                   path=f'/episode/{ep.id}/', user=self.user)
        return views.episode_detail(req, ep.id).content.decode('utf-8')

    def test_gdrive_episode_with_r2_streams_inline_from_r2(self):
        """A mirrored GDrive episode plays inline from R2 — no 'cannot be streamed
        inline' warning, and the <audio> source is the R2 URL, not the Drive link."""
        r2 = 'https://audio.test/1/2/ep-abc.mp3'
        body = self._render_episode(
            audio_url_subscriber='https://docs.google.com/uc?export=download&id=xyz',
            r2_url=r2,
        )
        self.assertIn(r2, body)
        self.assertNotIn('cannot be streamed inline', body)
        self.assertNotIn('docs.google.com/uc', body)  # Drive link not used as a source

    def test_gdrive_episode_without_r2_shows_recovery_warning(self):
        body = self._render_episode(
            audio_url_subscriber='https://docs.google.com/uc?export=download&id=xyz',
        )
        self.assertIn('cannot be streamed inline', body)


class AnalyticsSweepTests(TestCase):
    """Drives sweep_analytics_buffer end-to-end against an in-memory
    fakeredis. Verifies that:
      - billing:active and analytics:rss keys both update last_active_date
      - the latest date wins when multiple keys exist for one membership
      - keys are deleted after processing
      - the function still runs the presence sweep when there are no
        analytics:play keys (the bug we fixed)
    """

    def setUp(self):
        from django.conf import settings as django_settings
        self.user = User.objects.create_user(username='listener', email='listener@example.com')
        self.network = Network.objects.create(name='Net', slug='n')
        self.membership = NetworkMembership.objects.create(user=self.user, network=self.network)

        import os
        if django_settings.IS_IDE:
            import fakeredis
            self.fake = fakeredis.FakeRedis()
            redis_location = 'redis://fake:6379/0'
        else:
            import redis as redis_lib
            redis_location = os.getenv('REDIS_URL', 'redis://redis:6379/0')
            self.fake = redis_lib.from_url(redis_location)
            self._clean_test_keys()

        # override_settings(CACHES=...) is required in both branches: the task reads
        # settings.CACHES['default']['BACKEND'] directly and bails early on locmem.
        self._patches = [
            mock.patch('pod_manager.tasks.redis.from_url', return_value=self.fake),
            override_settings(CACHES={
                'default': {
                    'BACKEND': 'django.core.cache.backends.redis.RedisCache',
                    'LOCATION': redis_location,
                }
            }),
        ]

        for p in self._patches:
            p.enable() if hasattr(p, 'enable') else p.__enter__()

    def tearDown(self):
        for p in reversed(self._patches):
            p.disable() if hasattr(p, 'disable') else p.__exit__(None, None, None)
        from django.conf import settings as django_settings
        if not django_settings.IS_IDE:
            self._clean_test_keys()

    def _clean_test_keys(self):
        for pattern in [b'billing:active:*', b'analytics:rss:*']:
            keys = self.fake.keys(pattern)
            if keys:
                self.fake.delete(*keys)

    def test_billing_active_updates_last_active_date(self):
        from pod_manager.tasks import sweep_analytics_buffer
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-05-02", "1")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')
        # And the key was drained.
        self.assertEqual(self.fake.keys('billing:active:*'), [])

    def test_analytics_rss_updates_last_active_date(self):
        """RSS-only listener (no session, no billing middleware) still counts
        as active because the feed views write analytics:rss directly."""
        from pod_manager.tasks import sweep_analytics_buffer
        self.fake.set(f"analytics:rss:{self.network.id}:{self.user.id}:2026-04-30", "3")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-04-30')
        self.assertEqual(self.fake.keys('analytics:rss:*'), [])

    def test_latest_date_wins_for_one_membership(self):
        from pod_manager.tasks import sweep_analytics_buffer
        # Two keys for the same (network, user) — older billing, newer rss.
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-04-01", "1")
        self.fake.set(f"analytics:rss:{self.network.id}:{self.user.id}:2026-05-02", "5")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')

    def test_runs_when_no_play_keys(self):
        """The early-return that used to skip billing when there were no
        analytics:play:* keys is gone. With only billing keys, the sweep
        must still update last_active_date."""
        from pod_manager.tasks import sweep_analytics_buffer
        # No analytics:play keys at all.
        self.fake.set(f"billing:active:{self.network.id}:{self.user.id}:2026-05-02", "1")

        sweep_analytics_buffer()

        self.membership.refresh_from_db()
        self.assertEqual(str(self.membership.last_active_date), '2026-05-02')


class PatreonStateForgeryTests(TestCase):
    """patreon_callback must NOT call _link_creator_campaign when the state
    parameter isn't a valid signed token, even when the rest of the OAuth
    round-trip succeeds. The negative manual test (raw network_id as state)
    couldn't actually prove this because Patreon rejects the fake code first.
    Mock the token exchange so we reach the state validation."""

    def setUp(self):
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Victim', slug='victim')

    def _callback(self, state):
        req = self.factory.get(
            reverse('patreon_callback'),
            {'code': 'fake-code', 'state': state},
        )
        req.network = self.network
        from django.contrib.sessions.backends.cache import SessionStore
        req.session = SessionStore()
        req.session.create()
        from django.contrib.messages.storage.fallback import FallbackStorage
        setattr(req, '_messages', FallbackStorage(req))
        from django.contrib.auth.models import AnonymousUser
        req.user = AnonymousUser()
        return req

    @mock.patch('pod_manager.views._link_creator_campaign')
    @mock.patch('pod_manager.views._fetch_patreon_identity')
    @mock.patch('pod_manager.views._exchange_patreon_token')
    def test_raw_network_id_state_does_not_link_campaign(self, mock_exchange,
                                                         mock_identity, mock_link):
        # Token exchange succeeds.
        mock_exchange.return_value = ({'access_token': 'a', 'refresh_token': 'r'}, None)
        # Identity returns the minimum needed to fall through to listener path.
        mock_identity.return_value = ({
            'data': {'id': '1', 'attributes': {'email': 'x@example.com',
                                                'first_name': 'X',
                                                'last_name': 'Y'}},
            'included': [],
        }, None)
        # Forged state: just the raw network id.
        req = self._callback(state=str(self.network.id))
        views.patreon_callback(req)
        # Critical assertion: link path was NOT taken.
        mock_link.assert_not_called()

    @mock.patch('pod_manager.views._link_creator_campaign')
    @mock.patch('pod_manager.views._exchange_patreon_token')
    def test_signed_state_with_wrong_user_id_is_rejected(self, mock_exchange, mock_link):
        # Even a properly-signed state must include the linking user's id;
        # if request.user.id doesn't match, _link_creator_campaign should
        # not run.
        mock_exchange.return_value = ({'access_token': 'a', 'refresh_token': 'r'}, None)
        # Sign state for user 999, but the callback runs anonymously (user.id=None).
        forged = views._sign_oauth_state(f"link:999:{self.network.id}")
        req = self._callback(state=forged)
        views.patreon_callback(req)
        mock_link.assert_not_called()


# ---------------------------------------------------------------------------
# Creator settings: inbox action handlers (approve / reject)
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorInboxActionTests(TestCase):
    """_handle_inbox_action: approve_edit and reject_edit branches."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.submitter = User.objects.create_user(username='submitter')
        # High threshold so submitter never auto-approves
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=999)
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original Title', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/a.mp3',
            tags=['tag1'], chapters_public=[],
        )
        self.membership = NetworkMembership.objects.create(
            user=self.submitter, network=self.network, trust_score=5,
        )

    def _make_pending_edit(self):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.PENDING,
            original_data={
                'title': self.episode.title,
                'description': self.episode.clean_description,
                'tags': list(self.episode.tags or []),
                'chapters': self.episode.chapters_public or [],
            },
            suggested_data={
                'title': 'New Title',
                'description': '<p>new desc</p>',
                'tags': ['tag2'],
                'chapters': [],
            },
        )

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_approve_title_grants_trust_locks_episode(self):
        edit = self._make_pending_edit()
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_title': 'on',
            'edited_title': 'New Title',
        })
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'New Title')
        self.assertTrue(self.episode.is_metadata_locked)
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'approved')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 6)  # +1

    def test_approve_zero_fields_converts_to_rejection_and_penalizes(self):
        edit = self._make_pending_edit()
        # No approve_* checkboxes → zero points
        self._post({'action': 'approve_edit', 'edit_id': edit.id})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rejected')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 3)  # -2

    def test_unapproved_sections_pruned_from_suggested_data(self):
        """A reviewer can uncheck sections; the unapproved ones must not survive in
        suggested_data (else the audit log shows them as applied + scored)."""
        edit = self._make_pending_edit()  # title + description + tags + chapters
        self._post({
            'action': 'approve_edit', 'edit_id': edit.id,
            'approve_title': 'on', 'edited_title': 'New Title',
            # description / tags / chapters intentionally left unchecked
        })
        edit.refresh_from_db()
        self.assertEqual(set(edit.suggested_data.keys()), {'title'})
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 6)  # +1 title only, no phantom

    def test_approve_three_fields_applies_perfect_sweep_bonus(self):
        import json as _json
        edit = self._make_pending_edit()
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_title': 'on',
            'edited_title': 'New Title',
            'approve_description': 'on',
            'edited_description': '<p>new desc</p>',
            'approve_tags': 'on',
            'edited_tags': _json.dumps(['tag2']),
        })
        self.membership.refresh_from_db()
        # 3 fields approved → 3 points + 2 perfect-sweep bonus = 5
        self.assertEqual(self.membership.trust_score, 10)

    def test_approve_empty_dict_chapters_is_not_a_change(self):
        """An empty v1.2 chapters dict against a legacy empty list must not
        count as a chapter edit (phantom 'Version'/'Chapters' rows bug)."""
        import json as _json
        edit = self._make_pending_edit()
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_chapters': 'on',
            'edited_chapters': _json.dumps({'version': '1.2.0', 'chapters': []}),
        })
        edit.refresh_from_db()
        # No real change anywhere → zero-approval trap converts to rejection.
        self.assertEqual(edit.status, 'rejected')
        self.episode.refresh_from_db()
        # Empty chapters normalize to None ("no chapters"); never a legacy [].
        self.assertIsNone(self.episode.chapters_public)

    def test_reject_edit_penalizes_trust_and_marks_rejected(self):
        edit = self._make_pending_edit()
        self._post({'action': 'reject_edit', 'edit_id': edit.id})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rejected')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 3)  # -2

    # --- Phase 5: speaker per-speaker award (§3.4) -------------------------------

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_approve_speaker_awards_points_and_counter(self, mock_apply):
        """Two first-time namings → +2 trust, +2 edits_speakers, points banked."""
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'speaker_mappings': {}},
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Jim', 'SPEAKER_01': 'Aron'}},
        )
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_speaker_labels': 'on'})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'approved')
        self.assertEqual(edit.points, 2)
        self.assertEqual(edit.counter_deltas, {'edits_speakers': 2})  # banked for rollback
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 7)       # 5 + 2
        self.assertEqual(self.membership.edits_speakers, 2)
        mock_apply.assert_called_once_with(self.episode.id)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_rollback_washes_exact_points(self, mock_apply):
        """Single rollback subtracts exactly the banked points from both
        trust and edits_speakers (§3.4 wash)."""
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'speaker_mappings': {}},
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Jim', 'SPEAKER_01': 'Aron'}},
            points=2, counter_deltas={'edits_speakers': 2}, resolved_at=timezone.now(),
        )
        self.membership.trust_score = 7
        self.membership.edits_speakers = 2
        self.membership.save()
        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rolled_back')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 5)   # -points (trust)
        self.assertEqual(self.membership.edits_speakers, 0)  # -counter_deltas

    # --- Phase 5 / §8a: sequence counter ----------------------------------------

    def test_approve_sequence_fields_increment_counter(self):
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'title': self.episode.title,
                           'description': self.episode.clean_description,
                           'tags': [], 'chapters': []},
            suggested_data={'season_number': 2, 'episode_number': 5},
        )
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_season_number': 'on', 'edited_season_number': '2',
                    'approve_episode_number': 'on', 'edited_episode_number': '5'})
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_sequence, 2)
        self.assertEqual(self.membership.trust_score, 7)  # 5 + 2

    def test_rollback_sequence_restores_values_and_decrements_counter(self):
        # Episode currently holds the approved (edited) sequence values.
        self.episode.season_number = 2
        self.episode.episode_number = 5
        self.episode.episode_type = 'bonus'
        self.episode.save()
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': self.episode.title,
                           'description': self.episode.clean_description,
                           'tags': [], 'chapters': [],
                           'season_number': 1, 'episode_number': 4,
                           'episode_type': 'full'},
            suggested_data={'title': self.episode.title,
                            'season_number': 2, 'episode_number': 5,
                            'episode_type': 'bonus'},
            counter_deltas={'edits_sequence': 3},
            resolved_at=timezone.now(),
        )
        self.membership.edits_sequence = 3
        self.membership.save()
        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        # Field values are restored to the pre-approval snapshot, not just the counter.
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 1)
        self.assertEqual(self.episode.episode_number, 4)
        self.assertEqual(self.episode.episode_type, 'full')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_sequence, 0)

    def test_approve_then_rollback_round_trips_sequence_values(self):
        """End-to-end: approving sequence fields captures the pre-approval values,
        so rollback restores them (here: back to 'no sequence metadata')."""
        self.assertIsNone(self.episode.season_number)
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'title': self.episode.title,
                           'description': self.episode.clean_description,
                           'tags': [], 'chapters': []},
            suggested_data={'season_number': 3, 'episode_number': 7,
                            'episode_type': 'trailer'},
        )
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_season_number': 'on', 'edited_season_number': '3',
                    'approve_episode_number': 'on', 'edited_episode_number': '7',
                    'approve_episode_type': 'on', 'edited_episode_type': 'trailer'})
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 3)
        self.assertEqual(self.episode.episode_type, 'trailer')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_sequence, 3)

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.season_number)
        self.assertIsNone(self.episode.episode_number)
        self.assertEqual(self.episode.episode_type, '')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_sequence, 0)

    def test_rollback_tags_is_exact_wash(self):
        """A multi-tag add credits +N at approval and rollback reverses exactly
        −N (via counter_deltas), not the old flat −1 that left the counter high."""
        import json as _json
        self.episode.tags = ['tag1']
        self.episode.save()
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'title': self.episode.title,
                           'description': self.episode.clean_description,
                           'tags': ['tag1'], 'chapters': []},
            suggested_data={'tags': ['tag1', 'tag2', 'tag3']},
        )
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_tags': 'on', 'edited_tags': _json.dumps(['tag1', 'tag2', 'tag3'])})
        edit.refresh_from_db()
        self.assertEqual(edit.counter_deltas, {'edits_tags': 2})
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_tags, 2)

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_tags, 0)  # exact wash, not 1

    def test_rollback_legacy_edit_skips_counter_decrement(self):
        """A pre-feature edit (no counter_deltas) reverses trust but leaves the
        counters untouched — the agreed behaviour for historical rows."""
        self.membership.edits_title = 5
        self.membership.save()
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': 'Original Title', 'description': self.episode.clean_description,
                           'tags': [], 'chapters': []},
            suggested_data={'title': 'New Title'},  # no counter_deltas banked
            resolved_at=timezone.now(),
        )
        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.edits_title, 5)  # skipped (legacy row)

    def test_rollback_unlocks_metadata_when_no_approved_remain(self):
        edit = self._make_pending_edit()
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_title': 'on', 'edited_title': 'New Title'})
        self.episode.refresh_from_db()
        self.assertTrue(self.episode.is_metadata_locked)
        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.episode.refresh_from_db()
        self.assertFalse(self.episode.is_metadata_locked)  # editable again

    def test_rollback_keeps_lock_when_another_approved_remains(self):
        e1 = self._make_pending_edit()
        self._post({'action': 'approve_edit', 'edit_id': e1.id,
                    'approve_title': 'on', 'edited_title': 'New Title'})
        e2 = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'description': 'x'}, suggested_data={'description': '<p>z</p>'},
            points=1, counter_deltas={'edits_descriptions': 1}, resolved_at=timezone.now(),
        )
        self._post({'action': 'rollback_single_edit', 'edit_id': e1.id})
        self.episode.refresh_from_db()
        self.assertTrue(self.episode.is_metadata_locked)  # e2 still approved

    def test_rollback_restores_private_chapters_via_effective_snapshot(self):
        import json as _json
        orig_priv = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'Orig'}]}
        self.episode.chapters_private = orig_priv
        self.episode.chapters_public = None   # typical: blank public, private has chapters
        self.episode.save()
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'title': self.episode.title, 'description': self.episode.clean_description,
                           'tags': [], 'chapters': []},
            suggested_data={},
        )
        new_ch = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'A'}, {'startTime': 60, 'title': 'B'}]}
        self._post({'action': 'approve_edit', 'edit_id': edit.id,
                    'approve_chapters': 'on', 'edited_chapters': _json.dumps(new_ch)})
        self.episode.refresh_from_db()
        self.assertEqual(len(chapter_items(self.episode.chapters_private)), 2)

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.episode.refresh_from_db()
        # Private chapters restored to the pre-edit value (not wiped to blank public).
        self.assertEqual(chapter_items(self.episode.chapters_private), chapter_items(orig_priv))

    def test_rollback_chapters_restores_both_columns_and_counts_inner(self):
        """Approve mirrors new chapters onto BOTH columns, so rollback must
        restore both — and decrement edits_chapters by the inner chapter count
        (chapter_items), not the v1.2 dict's key count."""
        chapters_dict = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'Intro'}]}
        self.episode.chapters_public = chapters_dict
        self.episode.chapters_private = chapters_dict
        self.episode.save()
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': self.episode.title,
                           'description': self.episode.clean_description,
                           'tags': [], 'chapters': []},
            suggested_data={'chapters': chapters_dict},
            counter_deltas={'edits_chapters': 1},
            resolved_at=timezone.now(),
        )
        # Pre-existing chapter credit + this edit's single chapter.
        self.membership.edits_chapters = 6
        self.membership.save()

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.episode.refresh_from_db()
        # BOTH columns reverted to "no chapters" (private would otherwise keep the
        # edited dict). Empty chapters normalize to None on save.
        self.assertEqual(chapter_items(self.episode.chapters_public), [])
        self.assertEqual(chapter_items(self.episode.chapters_private), [])
        self.assertEqual(self.episode.chapters_private, self.episode.chapters_public)
        self.membership.refresh_from_db()
        # -1 (one inner chapter), not -2 (dict keys 'version'+'chapters').
        self.assertEqual(self.membership.edits_chapters, 5)


# ---------------------------------------------------------------------------
# Creator settings: rollback handlers
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorRollbackTests(TestCase):
    """rollback_single_edit and bulk_rollback branches of _handle_rollback."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.spammer = User.objects.create_user(username='spammer')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Vandalized', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3',
        )
        self.membership = NetworkMembership.objects.create(
            user=self.spammer, network=self.network, trust_score=20, edits_title=1,
        )

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def _approved_edit(self, episode, original_title, resolved_at=None):
        return EpisodeEditSuggestion.objects.create(
            episode=episode, user=self.spammer, status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': original_title, 'description': 'x', 'tags': [], 'chapters': []},
            suggested_data={'title': 'Vandalized', 'description': 'x', 'tags': [], 'chapters': []},
            points=1, counter_deltas={'edits_title': 1},  # a title edit's exact award
            resolved_at=resolved_at or timezone.now(),
        )

    def test_rollback_blocked_when_newer_approved_edit_exists(self):
        base = timezone.now()
        older = self._approved_edit(self.episode, 'Original', resolved_at=base)
        self._approved_edit(self.episode, 'Middle',
                            resolved_at=base + timedelta(seconds=30))

        self._post({'action': 'rollback_single_edit', 'edit_id': older.id})

        older.refresh_from_db()
        self.assertEqual(older.status, 'approved')  # Must not change

    def test_rollback_single_restores_episode_and_penalizes_trust(self):
        edit = self._approved_edit(self.episode, 'Original Title')

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})

        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original Title')
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'rolled_back')
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 19)   # -1 (exact banked points)
        self.assertEqual(self.membership.edits_title, 0)    # -1 (exact banked counter)

    def test_bulk_rollback_reverses_exact_awards(self):
        for i in range(3):
            ep = Episode.objects.create(
                podcast=self.podcast, title=f'Ep{i}', pub_date=timezone.now(),
                raw_description='x', clean_description='x',
                audio_url_public='https://cdn.example.com/a.mp3',
            )
            self._approved_edit(ep, f'Clean {i}')
        # Membership reflecting exactly what those 3 title edits awarded.
        self.membership.trust_score = 3
        self.membership.edits_title = 3
        self.membership.save()

        self._post({'action': 'bulk_rollback', 'spammer_id': self.spammer.id})

        self.membership.refresh_from_db()
        self.assertEqual(self.membership.trust_score, 0)   # exact reversal, not blanket zero
        self.assertEqual(self.membership.edits_title, 0)
        reverted = EpisodeEditSuggestion.objects.filter(
            user=self.spammer, status=EpisodeEditSuggestion.Status.ROLLED_BACK
        ).count()
        self.assertEqual(reverted, 3)

    def _speaker_edit(self, mappings, resolved_at=None):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.spammer,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'speaker_mappings': {}},
            suggested_data={'speaker_mappings': mappings},
            resolved_at=resolved_at or timezone.now(),
        )

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_rollback_not_blocked_by_newer_approved(self, mock_apply):
        """Speaker edits replay order-correct, so the newer-approved blocker that
        applies to metadata edits must NOT apply to them (§3.4)."""
        base = timezone.now()
        older = self._speaker_edit({'SPEAKER_00': 'Jim'}, resolved_at=base)
        self._speaker_edit({'SPEAKER_01': 'A.Ron'},
                           resolved_at=base + timedelta(seconds=30))

        self._post({'action': 'rollback_single_edit', 'edit_id': older.id})

        older.refresh_from_db()
        self.assertEqual(older.status, 'rolled_back')
        mock_apply.assert_called_once_with(self.episode.id)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_bulk_rollback_replays_speaker_episode_once(self, mock_apply):
        """Many speaker edits on one episode → one replay after all are flipped."""
        self._speaker_edit({'SPEAKER_00': 'Jim'})
        self._speaker_edit({'SPEAKER_01': 'A.Ron'})

        self._post({'action': 'bulk_rollback', 'spammer_id': self.spammer.id})

        self.assertEqual(
            EpisodeEditSuggestion.objects.filter(
                user=self.spammer, status=EpisodeEditSuggestion.Status.ROLLED_BACK
            ).count(), 2)
        mock_apply.assert_called_once_with(self.episode.id)


@override_settings(CACHES=TEST_CACHES)
class BackfillEditPointsTests(TestCase):
    """backfill_edit_points: reconstruct points + counter_deltas onto legacy
    APPROVED edits so they become exactly reversible."""

    def setUp(self):
        self.net = Network.objects.create(name='N', slug='bep')
        self.pod = Podcast.objects.create(network=self.net, title='S', slug='s')
        self.ep = Episode.objects.create(
            podcast=self.pod, title='New T', pub_date=timezone.now(),
            raw_description='x', clean_description='x', audio_url_public='https://x/a.mp3',
        )
        self.user = User.objects.create_user(username='bep-u')

    def _run(self, *args):
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command('backfill_edit_points', '--network=bep', *args, stdout=out, stderr=StringIO())
        return out.getvalue()

    def _legacy_metadata(self):
        return EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user, status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': 'Old T', 'description': 'x', 'tags': [], 'chapters': []},
            suggested_data={'title': 'New T'}, resolved_at=timezone.now(),
        )  # points=0, counter_deltas={} (legacy)

    def test_metadata_edit_gets_points_and_counter_deltas(self):
        edit = self._legacy_metadata()
        self._run('--apply')
        edit.refresh_from_db()
        self.assertEqual(edit.points, 1)
        self.assertEqual(edit.counter_deltas, {'edits_title': 1})

    def test_preview_changes_nothing(self):
        edit = self._legacy_metadata()
        out = self._run()
        self.assertIn('edit #', out)
        edit.refresh_from_db()
        self.assertEqual(edit.points, 0)
        self.assertEqual(edit.counter_deltas, {})

    def test_idempotent_second_run_is_noop(self):
        self._legacy_metadata()
        self._run('--apply')
        out = self._run('--apply')
        self.assertIn('updated 0 edit', out)

    def test_speaker_chain_points_folded_in_resolved_order(self):
        t0 = timezone.now()
        e1 = EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user, status=EpisodeEditSuggestion.Status.APPROVED,
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Jim'}},
            original_data={'speaker_mappings': {}}, resolved_at=t0)
        e2 = EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user, status=EpisodeEditSuggestion.Status.APPROVED,
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'A.Ron'}},
            original_data={'speaker_mappings': {}}, resolved_at=t0 + timedelta(minutes=1))
        self._run('--apply')
        e1.refresh_from_db(); e2.refresh_from_db()
        self.assertEqual(e1.points, 1)                          # first-time naming
        self.assertEqual(e1.counter_deltas, {'edits_speakers': 1})
        self.assertEqual(e2.points, 1)                          # correction
        self.assertEqual(e2.counter_deltas, {'edits_speakers': 1})


# ---------------------------------------------------------------------------
# Creator settings: network and show management
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CreatorNetworkAndShowTests(TestCase):
    """update_network, add_show, update_show action handlers."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_update_network_invalid_json_does_not_save(self):
        self.network.website_url = 'https://before.example.com'
        self.network.save()
        self._post({
            'action': 'update_network',
            'theme_config': '{invalid json}',
            'website_url': 'https://after.example.com',
            'patreon_campaign_id': '', 'default_image_url': '',
            'ignored_title_tags': '', 'description_cut_triggers': '',
            'footer_public': '', 'footer_private': '',
        })
        self.network.refresh_from_db()
        self.assertEqual(self.network.website_url, 'https://before.example.com')

    def test_update_network_valid_persists_fields(self):
        import json as _json
        self._post({
            'action': 'update_network',
            'theme_config': _json.dumps({'primary_color': '#abc'}),
            'website_url': 'https://example.com',
            'patreon_campaign_id': 'camp999',
            'default_image_url': '', 'ignored_title_tags': '',
            'description_cut_triggers': '',
            'footer_public': 'Pub footer', 'footer_private': 'Priv footer',
        })
        self.network.refresh_from_db()
        self.assertEqual(self.network.website_url, 'https://example.com')
        self.assertEqual(self.network.patreon_campaign_id, 'camp999')
        self.assertEqual(self.network.global_footer_public, 'Pub footer')
        self.assertEqual(self.network.theme_config, {'primary_color': '#abc'})

    def test_add_show_creates_podcast_and_redirects_to_auto_import(self):
        resp = self._post({
            'action': 'add_show',
            'title': 'Brand New Show', 'slug': 'brand-new',
            'tier_id': '',
            'public_feed_url': 'https://feeds.example.com/show.rss',
            'subscriber_feed_url': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('auto_import=', resp['Location'])
        self.assertTrue(Podcast.objects.filter(network=self.network, slug='brand-new').exists())

    def test_update_show_persists_feed_url(self):
        show = Podcast.objects.create(
            network=self.network, title='S', slug='s',
            public_feed_url='https://old.example.com/feed.rss',
        )
        self._post({
            'action': 'update_show',
            'show_id': show.id,
            'public_feed_url': 'https://new.example.com/feed.rss',
            'subscriber_feed_url': '',
            'tier_id': '', 'show_footer_public': '', 'show_footer_private': '',
        })
        show.refresh_from_db()
        self.assertEqual(show.public_feed_url, 'https://new.example.com/feed.rss')

    def test_update_show_persists_allow_public_transcripts(self):
        show = Podcast.objects.create(network=self.network, title='S3', slug='s3')
        self.assertTrue(show.allow_public_transcripts)  # default True
        base = {
            'action': 'update_show', 'show_id': show.id,
            'public_feed_url': '', 'subscriber_feed_url': '',
            'tier_id': '', 'show_footer_public': '', 'show_footer_private': '',
        }
        # Checkbox absent in POST => unchecked => False (serve gate applies live).
        self._post(base)
        show.refresh_from_db()
        self.assertFalse(show.allow_public_transcripts)
        # Checkbox present => True.
        self._post({**base, 'allow_public_transcripts': 'on'})
        show.refresh_from_db()
        self.assertTrue(show.allow_public_transcripts)


@override_settings(CACHES=TEST_CACHES)
class CreatorShowVisibilityCrossPublishTests(TestCase):
    """update_show handling of is_hidden + feed-level auto cross-publish
    (rollout steps 5-6): field persistence, M2M diff -> backfill/teardown
    tasks, access-mode re-sync, and the hidden-but-not-cross-published
    warning."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='vis')
        self.network.owners.add(self.owner)
        self.show = Podcast.objects.create(network=self.network, title='Service', slug='vis-service')
        self.dest1 = Podcast.objects.create(network=self.network, title='Dest1', slug='vis-dest1')
        self.dest2 = Podcast.objects.create(network=self.network, title='Dest2', slug='vis-dest2')

    def _base(self, **overrides):
        base = {
            'action': 'update_show', 'show_id': self.show.id,
            'public_feed_url': '', 'subscriber_feed_url': '',
            'tier_id': '', 'show_footer_public': '', 'show_footer_private': '',
        }
        base.update(overrides)
        return base

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def _post_req(self, data):
        """Like _post, but calls the handler directly and returns the request
        so messages can be inspected (creator_settings redirects afterward,
        and a redirect response has no wsgi_request to read them off)."""
        from pod_manager.views.creator.actions import handle_update_show
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        handle_update_show(req, self.network)
        return req

    def _ep(self, podcast=None, **kwargs):
        defaults = dict(
            podcast=podcast or self.show, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def test_is_hidden_persists_from_checkbox(self):
        self.assertFalse(self.show.is_hidden)
        self._post(self._base(is_hidden='on'))
        self.show.refresh_from_db()
        self.assertTrue(self.show.is_hidden)
        # Checkbox absent => unchecked => False.
        self._post(self._base())
        self.show.refresh_from_db()
        self.assertFalse(self.show.is_hidden)

    def test_adding_destinations_backfills_existing_episodes_as_auto(self):
        ep = self._ep()
        self._post(self._base(auto_crosspublish_target_ids=[str(self.dest1.id), str(self.dest2.id)]))
        self.show.refresh_from_db()
        self.assertEqual(
            set(self.show.auto_crosspublish_targets.values_list('id', flat=True)),
            {self.dest1.id, self.dest2.id})
        for dest in (self.dest1, self.dest2):
            link = ep.cross_publications.get(podcast=dest)
            self.assertTrue(link.auto_created)
            self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.INHERIT)

    def test_target_multiselect_excludes_self_and_foreign_network(self):
        other_network = Network.objects.create(name='Other', slug='vis-other')
        foreign = Podcast.objects.create(network=other_network, title='F', slug='vis-f')
        self._post(self._base(auto_crosspublish_target_ids=[
            str(self.show.id), str(self.dest1.id), str(foreign.id),
        ]))
        self.show.refresh_from_db()
        self.assertEqual(
            set(self.show.auto_crosspublish_targets.values_list('id', flat=True)),
            {self.dest1.id})

    def test_removing_one_destination_tears_down_only_that_ones_auto_links(self):
        ep = self._ep()
        self.show.auto_crosspublish_targets.set([self.dest1, self.dest2])
        self._post(self._base())  # backfill both first
        manual = EpisodeCrossPublication.objects.create(
            episode=self._ep(title='Manual host'), podcast=self.dest1)

        self._post(self._base(auto_crosspublish_target_ids=[str(self.dest2.id)]))

        self.show.refresh_from_db()
        self.assertEqual(
            set(self.show.auto_crosspublish_targets.values_list('id', flat=True)),
            {self.dest2.id})
        self.assertFalse(ep.cross_publications.filter(podcast=self.dest1).exists())
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest2, auto_created=True).exists())
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=manual.id).exists())

    def test_changing_access_mode_resyncs_existing_auto_links_only(self):
        ep = self._ep()
        self.show.auto_crosspublish_targets.set([self.dest1])
        self._post(self._base())  # backfill at default INHERIT
        manual = EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest2)

        self._post(self._base(
            auto_crosspublish_target_ids=[str(self.dest1.id)],
            auto_crosspublish_access_mode='target'))

        self.show.refresh_from_db()
        self.assertEqual(self.show.auto_crosspublish_access_mode, 'target')
        link = ep.cross_publications.get(podcast=self.dest1)
        self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.TARGET)
        manual.refresh_from_db()
        self.assertEqual(manual.access_mode, EpisodeCrossPublication.AccessMode.INHERIT)

    def test_hiding_non_cross_published_feed_warns_but_still_saves(self):
        req = self._post_req(self._base(is_hidden='on'))
        self.show.refresh_from_db()
        self.assertTrue(self.show.is_hidden)
        msgs = [str(m) for m in get_messages(req)]
        self.assertTrue(any('not cross-published anywhere' in m for m in msgs))

    def test_hiding_feed_with_auto_targets_does_not_warn(self):
        req = self._post_req(self._base(
            is_hidden='on', auto_crosspublish_target_ids=[str(self.dest1.id)]))
        msgs = [str(m) for m in get_messages(req)]
        self.assertFalse(any('not cross-published anywhere' in m for m in msgs))

    def test_hiding_feed_with_existing_manual_cross_publication_does_not_warn(self):
        ep = self._ep()
        EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest1)
        req = self._post_req(self._base(is_hidden='on'))
        msgs = [str(m) for m in get_messages(req)]
        self.assertFalse(any('not cross-published anywhere' in m for m in msgs))

    def test_unhiding_feed_emits_no_warning(self):
        self.show.is_hidden = True
        self.show.save()
        req = self._post_req(self._base())
        msgs = [str(m) for m in get_messages(req)]
        self.assertFalse(any('not cross-published anywhere' in m for m in msgs))


# ---------------------------------------------------------------------------
# Creator settings: episode merge, split, move
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES, WHISPER_ENABLED=False)
class CreatorMergeAndMoveTests(TestCase):
    """merge_episodes, split_episode, move_episodes handlers."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(network=self.network, title='P1', slug='p1')

    def _ep(self, **kwargs):
        defaults = dict(
            podcast=self.podcast, pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_merge_transfers_private_data_and_deletes_orphan(self):
        pub_ep = self._ep(title='Public', guid_public='pub-guid')
        priv_ep = self._ep(
            title='Private', guid_private='priv-guid',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self._post({
            'action': 'merge_episodes',
            'public_episode_id': pub_ep.id,
            'private_episode_id': priv_ep.id,
        })
        pub_ep.refresh_from_db()
        self.assertEqual(pub_ep.guid_private, 'priv-guid')
        self.assertEqual(pub_ep.audio_url_subscriber, 'https://cdn.example.com/priv.mp3')
        self.assertFalse(Episode.objects.filter(id=priv_ep.id).exists())

    def test_split_creates_new_episode_and_clears_private_data(self):
        ep = self._ep(
            title='Paired', guid_private='priv-guid',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self._post({'action': 'split_episode', 'episode_id': ep.id})

        ep.refresh_from_db()
        self.assertIsNone(ep.guid_private)
        self.assertEqual(ep.audio_url_subscriber, '')
        self.assertEqual(ep.match_reason, 'Manually Unpaired')
        # A new episode with the same title must exist
        self.assertEqual(Episode.objects.filter(podcast=self.podcast, title='Paired').count(), 2)

    def _merge_partial(self, data, *, hx=True, user=None):
        # S2.4 folded merge_desk_partial into the creator_tab_partial registry —
        # that endpoint was this router with the tab hardcoded (same owner gate,
        # same non-HX redirect, same context build). These tests are unchanged
        # otherwise: they still pin the fragment/redirect contract, now via the
        # router.
        req = _make_tenant_request(self.factory, self.network,
                                   method='get', path='/creator/tab/merge/',
                                   data=data, user=user or self.owner)
        if hx:
            req.META['HTTP_HX_REQUEST'] = 'true'
        return views.creator_tab_partial(req, 'merge')

    def test_merge_partial_hx_returns_body_fragment_only(self):
        self._ep(title='LonelyPublic', guid_public='pub-guid')  # a public orphan
        resp = self._merge_partial({'network': self.network.slug, 'merge_view': 'orphans'})
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        self.assertIn('Data Reconciliation', html)
        self.assertIn('LonelyPublic', html)
        # The fragment must be ONLY the merge body — not the whole settings page.
        self.assertNotIn('id="list-tab"', html)
        self.assertNotIn('id="networkSettingsForm"', html)

    def test_merge_partial_direct_get_redirects_to_full_page(self):
        resp = self._merge_partial(
            {'network': self.network.slug, 'merge_view': 'matched', 'merge_q': 'x'},
            hx=False,
        )
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/creator/?', resp['Location'])
        self.assertIn('tab=merge', resp['Location'])
        self.assertIn('merge_view=matched', resp['Location'])

    def test_merge_partial_forbidden_for_non_owner(self):
        stranger = User.objects.create_user(username='stranger')
        resp = self._merge_partial({'network': self.network.slug}, user=stranger)
        self.assertEqual(resp.status_code, 403)

    def _show_form(self, show, *, user=None):
        req = _make_tenant_request(self.factory, self.network,
                                   method='get', path='/creator/show/%d/form/' % show.id,
                                   data={'network': self.network.slug}, user=user or self.owner)
        return views.creator_show_form(req, show.id)

    def test_show_form_renders_for_owner(self):
        resp = self._show_form(self.podcast)
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        self.assertIn('value="update_show"', html)
        self.assertIn('name="show_id"', html)
        self.assertIn('Transcription Overrides', html)
        # It's a fragment, not the whole page.
        self.assertNotIn('id="list-tab"', html)

    def test_show_form_404_for_show_outside_network(self):
        from django.http import Http404
        other = Network.objects.create(name='Other', slug='o2')
        other.owners.add(self.owner)
        stray = Podcast.objects.create(network=other, title='Stray', slug='stray')
        # Requested with ?network=n (self.network), so the stray show must 404.
        with self.assertRaises(Http404):
            self._show_form(stray)

    def test_show_form_forbidden_for_non_owner(self):
        stranger = User.objects.create_user(username='stranger2')
        resp = self._show_form(self.podcast, user=stranger)
        self.assertEqual(resp.status_code, 403)

    def _tab_partial(self, tab, *, hx=True, user=None):
        req = _make_tenant_request(self.factory, self.network,
                                   method='get', path='/creator/tab/%s/' % tab,
                                   data={'network': self.network.slug}, user=user or self.owner)
        if hx:
            req.META['HTTP_HX_REQUEST'] = 'true'
        return views.creator_tab_partial(req, tab)

    def test_tab_partial_hx_renders_fragment(self):
        resp = self._tab_partial('audit')
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn('id="list-tab"', resp.content.decode())

    def test_tab_partial_direct_get_redirects_to_full_page(self):
        resp = self._tab_partial('audit', hx=False)
        self.assertEqual(resp.status_code, 302)
        self.assertIn('tab=audit', resp['Location'])

    def test_tab_partial_unknown_tab_404(self):
        from django.http import Http404
        with self.assertRaises(Http404):
            self._tab_partial('bogus')

    def test_tab_partial_renders_every_registered_tab(self):
        from pod_manager.views.creator.tabs import TAB_CONTENT
        for name in TAB_CONTENT:
            resp = self._tab_partial(name)
            self.assertEqual(resp.status_code, 200, "tab %r failed to render" % name)
            # Body-only fragment: the tab-pane wrapper belongs to the shell.
            self.assertNotIn('tab-pane fade', resp.content.decode(),
                             "tab %r leaked its pane wrapper" % name)

    def test_tab_partial_forbidden_for_non_owner(self):
        stranger = User.objects.create_user(username='stranger3')
        resp = self._tab_partial('audit', user=stranger)
        self.assertEqual(resp.status_code, 403)

    def test_move_episodes_to_existing_podcast(self):
        target = Podcast.objects.create(network=self.network, title='Target', slug='target')
        ep = self._ep(title='Traveller')
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': target.id,
            'new_podcast_title': '', 'new_podcast_slug': '', 'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, target.id)

    def test_move_episodes_creates_new_podcast(self):
        ep = self._ep(title='Traveller')
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'Brand New', 'new_podcast_slug': 'brand-new',
            'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        new_pod = Podcast.objects.get(network=self.network, slug='brand-new')
        self.assertEqual(ep.podcast_id, new_pod.id)

    def test_move_episodes_missing_slug_redirects_with_error(self):
        ep = self._ep(title='Traveller')
        resp = self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'Show Without Slug', 'new_podcast_slug': '',
            'new_podcast_tier_id': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('tab=move', resp['Location'])
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.podcast.id)  # Unchanged

    def test_move_episodes_duplicate_slug_redirects_with_error(self):
        Podcast.objects.create(network=self.network, title='Existing', slug='exists')
        ep = self._ep(title='Traveller')
        resp = self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': '',
            'new_podcast_title': 'New Show', 'new_podcast_slug': 'exists',
            'new_podcast_tier_id': '',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('tab=move', resp['Location'])
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.podcast.id)  # Unchanged

    def test_move_episodes_stamps_pin_fields(self):
        target = Podcast.objects.create(network=self.network, title='Pin Target', slug='pin-target')
        ep = self._ep(title='Pinned Traveller')
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': target.id,
            'new_podcast_title': '', 'new_podcast_slug': '', 'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        self.assertIsNotNone(ep.podcast_pinned_at)
        self.assertEqual(ep.podcast_pinned_by, self.owner)


@override_settings(CACHES=TEST_CACHES, WHISPER_ENABLED=False)
class EpisodeMoveServiceTests(TestCase):
    """services/episode_move.move_episodes(): parent reassignment, pin
    stamping, cross-pub cleanup, rekey gating, fragment dispatch, base_url
    normalization."""

    def setUp(self):
        self.user = User.objects.create_user(username='mover')
        self.network = Network.objects.create(name='Net', slug='n')
        self.source = Podcast.objects.create(network=self.network, title='Source', slug='src')
        self.target = Podcast.objects.create(network=self.network, title='Target', slug='tgt')

    def _ep(self, **kwargs):
        defaults = dict(
            podcast=self.source, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def test_moves_episodes_and_returns_count_and_target(self):
        eps = [self._ep(title='A'), self._ep(title='B')]
        result = move_episodes([e.id for e in eps], self.target, base_url='http://n.test')
        self.assertEqual(result, {'count': 2, 'target': self.target})
        for e in eps:
            e.refresh_from_db()
            self.assertEqual(e.podcast_id, self.target.id)

    def test_drops_only_self_referencing_cross_publications(self):
        other = Podcast.objects.create(network=self.network, title='Other', slug='other')
        ep = self._ep()
        EpisodeCrossPublication.objects.create(episode=ep, podcast=self.target)
        keep = EpisodeCrossPublication.objects.create(episode=ep, podcast=other)
        move_episodes([ep.id], self.target, base_url='http://n.test', moved_by=self.user)
        self.assertFalse(
            EpisodeCrossPublication.objects.filter(episode=ep, podcast=self.target).exists())
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=keep.id).exists())

    @override_settings(R2_MIRROR_ENABLED=True)
    def test_rekey_dispatched_for_mirrored_episodes_only(self):
        mirrored = self._ep(title='m', r2_url='https://audio.test/1/1/m-aaaaaaaaaaaaaaaa.mp3')
        plain = self._ep(title='p')
        with mock.patch('pod_manager.tasks.task_rekey_episode_audio.delay') as rekey:
            move_episodes([mirrored.id, plain.id], self.target,
                          base_url='http://n.test', moved_by=self.user)
        self.assertEqual({c.args[0] for c in rekey.call_args_list}, {mirrored.id})

    @override_settings(R2_MIRROR_ENABLED=False)
    def test_rekey_skipped_when_mirror_disabled(self):
        mirrored = self._ep(title='m', r2_url='https://audio.test/1/1/m-aaaaaaaaaaaaaaaa.mp3')
        with mock.patch('pod_manager.tasks.task_rekey_episode_audio.delay') as rekey:
            move_episodes([mirrored.id], self.target,
                          base_url='http://n.test', moved_by=self.user)
        rekey.assert_not_called()

    def test_pin_stamps_fields_with_moved_by(self):
        ep = self._ep()
        move_episodes([ep.id], self.target, base_url='http://n.test', moved_by=self.user)
        ep.refresh_from_db()
        self.assertIsNotNone(ep.podcast_pinned_at)
        self.assertEqual(ep.podcast_pinned_by, self.user)

    def test_pin_false_stamps_neither_field(self):
        ep = self._ep()
        move_episodes([ep.id], self.target, base_url='http://n.test',
                      moved_by=self.user, pin=False)
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.target.id)
        self.assertIsNone(ep.podcast_pinned_at)
        self.assertIsNone(ep.podcast_pinned_by)

    def test_rebuild_fragments_false_dispatches_no_fragment_task(self):
        ep = self._ep()
        with mock.patch('pod_manager.tasks.task_rebuild_episode_fragments.delay') as rebuild:
            move_episodes([ep.id], self.target, base_url='http://n.test',
                          moved_by=self.user, rebuild_fragments=False)
        rebuild.assert_not_called()

    def test_base_url_normalized_regardless_of_caller_format(self):
        for raw in ('http://n.test/', 'http://n.test'):
            ep = self._ep(title=f'norm {raw}')
            with mock.patch('pod_manager.tasks.task_rebuild_episode_fragments.delay') as rebuild:
                move_episodes([ep.id], self.target, base_url=raw, moved_by=self.user)
            rebuild.assert_called_once_with(ep.id, 'http://n.test')


# ---------------------------------------------------------------------------
# gather_inbox conflict flags (tested directly, not through the full view)
# ---------------------------------------------------------------------------

class GatherInboxConflictFlagTests(TestCase):
    """gather_inbox annotates each pending edit with conflict flags when the
    live episode has changed since the user submitted their snapshot."""

    def setUp(self):
        from pod_manager.views.creator.data import gather_inbox as _gather_inbox
        self._gather_inbox = _gather_inbox
        self.submitter = User.objects.create_user(username='submitter')
        self.network = Network.objects.create(name='Net', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Current Title', pub_date=timezone.now(),
            raw_description='x', clean_description='<p>current</p>',
            audio_url_public='https://cdn.example.com/a.mp3',
            tags=['tag-live'], chapters_public=[],
        )

    def _make_pending_edit(self, original_data):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter, status=EpisodeEditSuggestion.Status.PENDING,
            original_data=original_data,
            suggested_data={'title': 'X', 'description': 'X', 'tags': [], 'chapters': []},
        )

    def _pending_list(self):
        return list(self._gather_inbox(self.network)['pending_edits'])

    def test_no_conflict_when_episode_matches_snapshot(self):
        self._make_pending_edit({
            'title': 'Current Title', 'description': '<p>current</p>',
            'tags': ['tag-live'], 'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertFalse(edit.title_conflict)
        self.assertFalse(edit.desc_conflict)
        self.assertFalse(edit.tags_conflict)
        self.assertFalse(edit.chapters_conflict)

    def test_title_conflict_when_episode_changed_since_submission(self):
        self._make_pending_edit({
            'title': 'Old Title',  # episode is now 'Current Title'
            'description': '<p>current</p>', 'tags': ['tag-live'], 'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertTrue(edit.title_conflict)
        self.assertFalse(edit.desc_conflict)

    def test_tags_conflict_when_live_tags_differ_from_snapshot(self):
        self._make_pending_edit({
            'title': 'Current Title', 'description': '<p>current</p>',
            'tags': ['tag-old'],  # episode has ['tag-live']
            'chapters': [],
        })
        edit = self._pending_list()[0]
        self.assertTrue(edit.tags_conflict)
        self.assertFalse(edit.title_conflict)


# ---------------------------------------------------------------------------
# submit_episode_edit: untrusted (pending) path
# ---------------------------------------------------------------------------

class SubmitEpisodeEditPendingPathTests(TestCase):
    """Untrusted users (trust_score < threshold) get 'pending' status.
    The episode must NOT be mutated immediately."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='untrusted')
        # threshold=100, user has score=0 → always pending
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=100)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=0)

    def _submit(self, payload):
        import json as _json
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': _json.dumps(payload)},
        )
        req.user = self.user
        req.network = self.network
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))
        return views.submit_episode_edit(req, self.episode.id)

    def test_untrusted_edit_is_pending_and_episode_unchanged(self):
        self._submit({
            'title': 'Vandalized', 'description': '<p>bad</p>',
            'tags': [], 'chapters': [],
        })
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(suggestion.status, 'pending')
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original')  # NOT mutated

    def test_noop_submission_creates_no_suggestion(self):
        """A payload identical to the current episode state (the edit form
        always posts every field) must not create a suggestion at all."""
        self._submit({
            'title': 'Original', 'description': '<p>hi</p>',
            'tags': [], 'chapters': [],
        })
        self.assertFalse(EpisodeEditSuggestion.objects.filter(episode=self.episode).exists())

    def test_unchanged_fields_dropped_from_suggestion(self):
        """Only actual deltas are stored — untouched fields would otherwise
        show in the inbox with Approve pre-toggled."""
        self._submit({
            'title': 'Renamed', 'description': '<p>hi</p>',
            'tags': [], 'chapters': [],
        })
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(set(suggestion.suggested_data.keys()), {'title'})

    def test_chapter_sanitization_skips_invalid_start_time(self):
        """Chapters with non-numeric startTime must be silently dropped."""
        self._submit({
            'title': 'Ep', 'description': '<p>x</p>', 'tags': [],
            'chapters': [
                {'startTime': 0.0, 'title': 'Valid'},
                {'startTime': 'not-a-number', 'title': 'Bad'},
            ],
        })
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        chaps = suggestion.suggested_data.get('chapters', {}).get('chapters', [])
        self.assertEqual(len(chaps), 1)
        self.assertEqual(chaps[0]['title'], 'Valid')


# ---------------------------------------------------------------------------
# services/edits.py unit tests
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# RSSFeedBuilder._finalize_xml unit tests
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class FinalizeXmlTests(TestCase):
    """_finalize_xml handles namespace injection, lxml pollution stripping,
    category attachment, and chapter URL insertion independently of podgen."""

    PODCAST_NS = "https://podcastindex.org/namespace/1.0"

    def setUp(self):
        # _finalize_xml now queries Transcript for completed transcripts.
        # These unit tests use mock episodes (no real DB rows), so suppress
        # the query to keep tests focused on the XML manipulation behaviour.
        # Transcript is locally imported inside _finalize_xml, so patch at models level.
        patcher = mock.patch('pod_manager.models.Transcript')
        self._mock_transcript = patcher.start()
        self._mock_transcript.objects.filter.return_value = []
        self._mock_transcript.Status.COMPLETED = 'completed'
        self.addCleanup(patcher.stop)

    # Minimal RSS skeleton with one item whose <guid> we can control.
    RSS_TMPL = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        '<rss version="2.0"{ns_attr}>'
        "<channel><title>T</title>"
        "<item><guid>{guid}</guid><title>Ep</title></item>"
        "</channel></rss>"
    )

    def _builder(self):
        from pod_manager.views.feeds import RSSFeedBuilder
        from unittest.mock import MagicMock
        net = MagicMock()
        net.name = "TestNet"
        net.summary = ""
        net.website_url = "https://example.com"
        net.default_image_url = ""
        net.contact_email = "test@example.com"
        return RSSFeedBuilder("https://example.com", "T", "D", "", net, feed_type='public')

    def _rss(self, guid="ep-1", with_ns=False):
        ns_attr = f' xmlns:podcast="{self.PODCAST_NS}"' if with_ns else ""
        return self.RSS_TMPL.format(guid=guid, ns_attr=ns_attr)

    def test_namespace_injected_when_absent(self):
        builder = self._builder()
        result = builder._finalize_xml(self._rss(), {}, None)
        self.assertIn(f'xmlns:podcast="{self.PODCAST_NS}"', result)

    def test_namespace_not_duplicated_when_already_present(self):
        builder = self._builder()
        result = builder._finalize_xml(self._rss(with_ns=True), {}, None)
        self.assertEqual(result.count(f'xmlns:podcast="{self.PODCAST_NS}"'), 1)

    def test_empty_tag_map_returns_without_lxml_parse(self):
        builder = self._builder()
        # Deliberately malformed inner XML that lxml would choke on —
        # empty tag_map must short-circuit before any parsing.
        raw = self._rss()
        result = builder._finalize_xml(raw, {}, None)
        self.assertIn('<rss', result)

    def test_inline_namespace_pollution_stripped(self):
        # Simulate what lxml produces when a child element uses the namespace:
        # it re-declares xmlns:podcast on the child. We must strip those.
        # A non-empty tag_map is required so that the lxml roundtrip runs.
        from unittest.mock import MagicMock
        raw = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            f'<rss version="2.0" xmlns:podcast="{self.PODCAST_NS}">'
            "<channel><title>T</title>"
            f'<item xmlns:podcast="{self.PODCAST_NS}"><guid>ep-1</guid></item>'
            "</channel></rss>"
        )
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'
        builder = self._builder()
        result = builder._finalize_xml(raw, {'ep-1': ep}, None)
        # The declaration should appear exactly once, on the root <rss> tag.
        self.assertEqual(result.count(f'xmlns:podcast="{self.PODCAST_NS}"'), 1)

    def test_category_tags_added_for_episode(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = ['comedy', 'tech']
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertEqual(result.count('<category>'), 2)
        self.assertIn('comedy', result)
        self.assertIn('tech', result)

    def test_no_category_tags_when_episode_has_none(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertNotIn('<category>', result)

    def _explicit_ep(self, value):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'
        ep.season_number = None
        ep.episode_number = None
        ep.explicit = value
        return ep

    def test_explicit_true_emitted(self):
        result = self._builder()._finalize_xml(self._rss(guid='ep-1'), {'ep-1': self._explicit_ep(True)}, None)
        self.assertIn('<itunes:explicit>true</itunes:explicit>', result)

    def test_explicit_false_emitted(self):
        result = self._builder()._finalize_xml(self._rss(guid='ep-1'), {'ep-1': self._explicit_ep(False)}, None)
        self.assertIn('<itunes:explicit>false</itunes:explicit>', result)

    def test_explicit_none_omits_item_tag(self):
        result = self._builder()._finalize_xml(self._rss(guid='ep-1'), {'ep-1': self._explicit_ep(None)}, None)
        self.assertNotIn('itunes:explicit', result)

    def test_chapter_url_added_when_chapters_exist(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.id = 42
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = {'version': '1.2.0', 'chapters': []}
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertIn('podcast:chapters', result)
        self.assertIn('/42/', result)
        self.assertIn('application/json+chapters', result)

    def test_no_chapter_elem_when_no_chapters(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = []
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'ep-1': ep}, None)
        self.assertNotIn('podcast:chapters', result)

    def test_access_map_controls_chapter_feed_type(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.id = 99
        ep.tags = []
        ep.podcast_id = 7
        ep.chapters_public = [{'startTime': 0, 'title': 'A'}]
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        result = builder._finalize_xml(
            self._rss(guid='ep-1'), {'ep-1': ep},
            access_map={7: True},
        )
        self.assertIn('/chapters/private.json', result)

    def test_unknown_guid_item_left_untouched(self):
        from unittest.mock import MagicMock
        ep = MagicMock()
        ep.tags = ['x']
        ep.podcast_id = 1
        ep.chapters_public = None
        ep.chapters_private = None
        ep.episode_type = 'full'

        builder = self._builder()
        # tag_map has 'other-guid', XML has 'ep-1' — no match, no mutation
        result = builder._finalize_xml(self._rss(guid='ep-1'), {'other-guid': ep}, None)
        self.assertNotIn('<category>', result)


class ParseChapterPayloadTests(TestCase):
    """parse_chapter_payload normalises list and dict inputs."""

    def test_empty_list_returns_empty_chapters(self):
        result = parse_chapter_payload([])
        self.assertEqual(result, {'version': '1.2.0', 'chapters': []})

    def test_empty_dict_returns_empty_chapters(self):
        result = parse_chapter_payload({'chapters': [], 'version': '1.2.0'})
        self.assertEqual(result, {'version': '1.2.0', 'chapters': []})

    def test_valid_list_chapter_parsed(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'Intro'}])
        self.assertEqual(result['chapters'][0], {'startTime': 0, 'title': 'Intro'})

    def test_float_start_time_preserved_when_not_integer(self):
        result = parse_chapter_payload([{'startTime': 1.5, 'title': 'Mid'}])
        self.assertEqual(result['chapters'][0]['startTime'], 1.5)

    def test_integer_start_time_stored_as_int(self):
        result = parse_chapter_payload([{'startTime': 60.0, 'title': 'A'}])
        self.assertIsInstance(result['chapters'][0]['startTime'], int)

    def test_invalid_start_time_chapter_dropped(self):
        result = parse_chapter_payload([
            {'startTime': 0.0, 'title': 'Good'},
            {'startTime': 'bad', 'title': 'Drop me'},
        ])
        self.assertEqual(len(result['chapters']), 1)
        self.assertEqual(result['chapters'][0]['title'], 'Good')

    def test_chapter_missing_title_dropped(self):
        result = parse_chapter_payload([{'startTime': 0.0}])
        self.assertEqual(result['chapters'], [])

    def test_chapter_missing_start_time_dropped(self):
        result = parse_chapter_payload([{'title': 'No time'}])
        self.assertEqual(result['chapters'], [])

    def test_url_accepted_when_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'url': 'https://example.com'}])
        self.assertEqual(result['chapters'][0]['url'], 'https://example.com')

    def test_url_rejected_when_non_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'url': 'javascript:alert(1)'}])
        self.assertNotIn('url', result['chapters'][0])

    def test_img_accepted_when_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'img': 'https://cdn.example.com/art.jpg'}])
        self.assertEqual(result['chapters'][0]['img'], 'https://cdn.example.com/art.jpg')

    def test_img_rejected_when_non_http(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'img': 'data:image/png;base64,abc'}])
        self.assertNotIn('img', result['chapters'][0])

    def test_toc_false_preserved(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'toc': False}])
        self.assertFalse(result['chapters'][0]['toc'])

    def test_toc_true_not_included(self):
        # Only toc=False is meaningful per the spec; toc=True is the default.
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'toc': True}])
        self.assertNotIn('toc', result['chapters'][0])

    def test_location_included_when_name_and_geo_present(self):
        chap = {'startTime': 0.0, 'title': 'A', 'location': {'name': 'Berlin', 'geo': 'geo:52,13', 'osm': 'R62422'}}
        result = parse_chapter_payload([chap])
        loc = result['chapters'][0]['location']
        self.assertEqual(loc['name'], 'Berlin')
        self.assertEqual(loc['geo'], 'geo:52,13')
        self.assertEqual(loc['osm'], 'R62422')

    def test_location_omitted_when_geo_missing(self):
        chap = {'startTime': 0.0, 'title': 'A', 'location': {'name': 'Berlin'}}
        result = parse_chapter_payload([chap])
        self.assertNotIn('location', result['chapters'][0])

    def test_dict_format_waypoints_flag_propagated(self):
        payload = {'version': '1.2.0', 'chapters': [{'startTime': 0.0, 'title': 'A'}], 'waypoints': True}
        result = parse_chapter_payload(payload)
        self.assertTrue(result.get('waypoints'))

    def test_dict_format_without_waypoints_flag_omits_key(self):
        payload = {'version': '1.2.0', 'chapters': [{'startTime': 0.0, 'title': 'A'}]}
        result = parse_chapter_payload(payload)
        self.assertNotIn('waypoints', result)

    def test_end_time_included_when_valid(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'endTime': 30.0}])
        self.assertEqual(result['chapters'][0]['endTime'], 30)

    def test_end_time_omitted_when_none(self):
        result = parse_chapter_payload([{'startTime': 0.0, 'title': 'A', 'endTime': None}])
        self.assertNotIn('endTime', result['chapters'][0])


@override_settings(CACHES=TEST_CACHES)
class ApplyApprovedEditTests(TestCase):
    """apply_approved_edit writes all fields and locks metadata."""

    def setUp(self):
        self.network = Network.objects.create(name='N', slug='n')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='s')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
            tags=['old'], chapters_public=[], chapters_private=[],
        )

    def test_fields_updated_and_saved(self):
        new_chapters = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'Intro'}]}
        apply_approved_edit(self.episode, {
            'title': 'New Title',
            'description': '<p>new</p>',
            'tags': ['new'],
            'chapters': new_chapters,
        })
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'New Title')
        self.assertEqual(self.episode.clean_description, '<p>new</p>')
        self.assertEqual(self.episode.tags, ['new'])
        self.assertEqual(self.episode.chapters_public, new_chapters)
        self.assertEqual(self.episode.chapters_private, new_chapters)
        self.assertTrue(self.episode.is_metadata_locked)

    def test_missing_key_leaves_field_unchanged(self):
        apply_approved_edit(self.episode, {})
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Original')


@override_settings(CACHES=TEST_CACHES)
class UpdateContributionStatsTests(TestCase):
    """update_contribution_stats increments counters and awards trust correctly."""

    def setUp(self):
        self.user = User.objects.create_user(username='contrib')
        self.network = Network.objects.create(name='N', slug='n2')
        self.membership = NetworkMembership.objects.create(
            user=self.user, network=self.network,
            trust_score=10, edits_title=0, edits_tags=0,
            edits_chapters=0, edits_descriptions=0, first_responder_count=0,
        )
        self.original = {'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}

    def _call(self, suggested, *, is_first=False):
        update_contribution_stats(self.membership, suggested, self.original, is_first=is_first)
        self.membership.refresh_from_db()

    def test_no_change_awards_nothing(self):
        # No flat +5 anymore — an all-unchanged payload scores zero.
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.trust_score, 10)

    def test_title_change_awards_one_point_and_counter(self):
        self._call({'title': 'New', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_title, 1)
        self.assertEqual(self.membership.trust_score, 11)  # +1

    def test_no_title_change_leaves_edits_title_unchanged(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_title, 0)

    def test_tag_added_counts_one(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a', 'b'], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 1)

    def test_tag_removal_scores_point_but_no_counter(self):
        # Counter credits tags ADDED only; a pure removal adds 0 to the counter.
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': [], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 0)

    def test_two_tags_added_one_removed_counts_added_only(self):
        # original=['a'], new=['b','c'] → added={'b','c'} → counter 2 (removal not counted)
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['b', 'c'], 'chapters': []})
        self.assertEqual(self.membership.edits_tags, 2)

    def test_three_fields_award_sweep_bonus(self):
        # title + description + tags = 3 core fields → +1+1+1 +2 bonus = 5.
        points, _ = update_contribution_stats(
            self.membership, {'title': 'New', 'description': '<p>new</p>', 'tags': ['a', 'b']},
            self.original, is_first=False)
        self.assertEqual(points, 5)

    def test_chapter_change_counts_chapter_items(self):
        new_chaps = {'version': '1.2.0', 'chapters': [{'startTime': 0, 'title': 'A'}, {'startTime': 60, 'title': 'B'}]}
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': new_chaps})
        self.assertEqual(self.membership.edits_chapters, 2)

    def test_description_change_increments_edits_descriptions(self):
        self._call({'title': 'Old', 'description': '<p>new</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_descriptions, 1)

    def test_sequence_fields_credit_edits_sequence(self):
        # Trusted-path sequence edits must credit edits_sequence (one per present
        # field) so a later rollback's decrement is a wash, not a clamp at 0.
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'],
                    'chapters': [], 'season_number': 2, 'episode_type': 'bonus'})
        self.assertEqual(self.membership.edits_sequence, 2)

    def test_no_sequence_fields_leaves_edits_sequence_unchanged(self):
        self._call({'title': 'New', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []})
        self.assertEqual(self.membership.edits_sequence, 0)

    def test_first_responder_flag_increments_count(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}, is_first=True)
        self.assertEqual(self.membership.first_responder_count, 1)

    def test_not_first_responder_leaves_count_unchanged(self):
        self._call({'title': 'Old', 'description': '<p>old</p>', 'tags': ['a'], 'chapters': []}, is_first=False)
        self.assertEqual(self.membership.first_responder_count, 0)


@override_settings(CACHES=TEST_CACHES)
class SubmitEpisodeEditTrustedPathTests(TestCase):
    """Trusted users (trust_score >= threshold) get instant approval, episode
    mutation, +5 trust, and correct edit counters."""

    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='trusted')
        self.network = Network.objects.create(name='Net', slug='nt', auto_approve_trust_threshold=10)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='st')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Original', pub_date=timezone.now(),
            raw_description='hi', clean_description='<p>hi</p>',
            audio_url_public='https://cdn.example.com/audio.mp3',
            tags=['old'],
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=50)

    def _submit(self, payload):
        import json as _json
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': _json.dumps(payload)},
        )
        req.user = self.user
        req.network = self.network
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))
        with mock.patch('pod_manager.views.creator.main.task_rebuild_episode_fragments') as m:
            resp = views.submit_episode_edit(req, self.episode.id)
        return resp, m

    def test_trusted_edit_approved_and_episode_mutated(self):
        resp, _ = self._submit({'title': 'Updated', 'description': '<p>new</p>', 'tags': ['new'], 'chapters': []})
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.title, 'Updated')
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(suggestion.status, 'approved')

    def test_trusted_edit_awards_trust_and_increments_title_counter(self):
        self._submit({'title': 'Updated', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        mem = NetworkMembership.objects.get(user=self.user, network=self.network)
        # Only title changed (desc/tags/chapters match) on a first edit:
        # +1 title +1 first-responder = +2 → 52.
        self.assertEqual(mem.trust_score, 52)
        self.assertEqual(mem.edits_title, 1)

    def test_trusted_edit_counts_tag_delta_not_just_one(self):
        # original=['old'], new=['old','extra'] → 1 tag added
        self._submit({'title': 'Original', 'description': '<p>hi</p>', 'tags': ['old', 'extra'], 'chapters': []})
        mem = NetworkMembership.objects.get(user=self.user, network=self.network)
        self.assertEqual(mem.edits_tags, 1)

    def test_trusted_edit_triggers_fragment_rebuild(self):
        _, mock_task = self._submit({'title': 'X', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        mock_task.delay.assert_called_once_with(self.episode.id, mock.ANY)

    def test_first_responder_flag_set_on_suggestion(self):
        resp, _ = self._submit({'title': 'X', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertTrue(suggestion.is_first_responder)

    def test_second_edit_is_not_first_responder(self):
        self._submit({'title': 'First', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        self._submit({'title': 'Second', 'description': '<p>hi</p>', 'tags': ['old'], 'chapters': []})
        suggestions = list(EpisodeEditSuggestion.objects.filter(episode=self.episode).order_by('id'))
        self.assertTrue(suggestions[0].is_first_responder)
        self.assertFalse(suggestions[1].is_first_responder)


# ─────────────────────────────────────────────────────────────────────────────
# TRANSCRIPTION TESTS
# ─────────────────────────────────────────────────────────────────────────────

# Common settings override applied to all transcription tests.
# WHISPER_ENABLED=False suppresses the post_save signal during fixture creation
# so individual tests can opt into it deliberately.
TRANSCRIPTION_SETTINGS = dict(
    CACHES=TEST_CACHES,
    WHISPER_ENABLED=False,
    WHISPER_URL='http://whisper-test:9000',
    WHISPER_MODEL='medium.en',
    WHISPER_LANGUAGE='en',
    WHISPER_TIMEOUT=30,
    WHISPER_KEEP_SOURCE_AUDIO=False,
    IS_IDE=False,
    SITE_URL='http://testserver',
)

# Sample ASR response (JSON format, two speakers).
_MOCK_ASR_JSON = json.dumps({
    'language': 'en',
    'segments': [
        {'start': 0.0, 'end': 2.0, 'text': 'Hello', 'speaker': 'SPEAKER_00', 'words': []},
        {'start': 2.0, 'end': 4.0, 'text': 'World', 'speaker': 'SPEAKER_01', 'words': []},
    ],
})

_SAMPLE_SEGMENTS = [
    {'start': 0.0, 'end': 2.5, 'text': 'Hello world', 'speaker': 'SPEAKER_00'},
    {'start': 2.5, 'end': 5.0, 'text': 'Goodbye',     'speaker': None},
]

_SAMPLE_SEGMENTS_WITH_WORDS = [
    {
        'start': 0.0, 'end': 2.5, 'text': 'Hello world', 'speaker': 'SPEAKER_00',
        'words': [
            {'word': 'Hello', 'start': 0.0, 'end': 1.0, 'score': 0.9,  'speaker': 'SPEAKER_00'},
            {'word': 'world', 'start': 1.0, 'end': 2.5, 'score': 0.85, 'speaker': 'SPEAKER_00'},
        ],
    },
]


_fixture_counter = 0


def _make_fixture(*, subscriber=True):
    """Return (network, podcast, episode) with minimal required fields.
    Uses a module counter to keep slugs unique across repeated calls."""
    global _fixture_counter
    _fixture_counter += 1
    n = _fixture_counter
    net = Network.objects.create(name=f'TestNet{n}', slug=f'testnet-tx{n}')
    pod = Podcast.objects.create(network=net, title=f'Show{n}', slug=f'show-tx{n}')
    ep = Episode.objects.create(
        podcast=pod,
        title='Episode 1',
        pub_date=timezone.now(),
        raw_description='desc',
        clean_description='<p>desc</p>',
        audio_url_public='https://cdn.example.com/pub.mp3',
        audio_url_subscriber='https://cdn.example.com/sub.mp3' if subscriber else '',
    )
    return net, pod, ep


# ── 1. transcript_path() ─────────────────────────────────────────────────────

class TranscriptPathTests(TestCase):

    def test_bucket_and_filename(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            p = transcript_path(6354, 'vtt')
        self.assertEqual(p, Path('/tmp/media/transcriptions/6/6354.vtt'))

    def test_bucket_boundaries(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            self.assertEqual(transcript_path(0,    'srt').parent.name, '0')
            self.assertEqual(transcript_path(999,  'srt').parent.name, '0')
            self.assertEqual(transcript_path(1000, 'srt').parent.name, '1')
            self.assertEqual(transcript_path(5999, 'srt').parent.name, '5')

    def test_invalid_extension_raises(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            with self.assertRaises(ValueError):
                transcript_path(1, 'exe')

    def test_all_valid_extensions_accepted(self):
        with override_settings(MEDIA_ROOT='/tmp/media'):
            for ext in ('vtt', 'json', 'srt', 'html', 'words'):
                self.assertIsNotNone(transcript_path(1, ext))


# ── 2. Timestamp formatters ──────────────────────────────────────────────────

class VttTimestampTests(TestCase):

    def test_zero(self):
        self.assertEqual(_vtt_timestamp(0), '00:00:00.000')

    def test_sub_second(self):
        self.assertEqual(_vtt_timestamp(0.5), '00:00:00.500')

    def test_minutes(self):
        self.assertEqual(_vtt_timestamp(90.0), '00:01:30.000')

    def test_hours(self):
        self.assertEqual(_vtt_timestamp(3661.25), '01:01:01.250')


class SrtTimestampTests(TestCase):

    def test_zero(self):
        self.assertEqual(_srt_timestamp(0), '00:00:00,000')

    def test_milliseconds(self):
        self.assertEqual(_srt_timestamp(1.5), '00:00:01,500')

    def test_hours(self):
        self.assertEqual(_srt_timestamp(3661.25), '01:01:01,250')


# ── 3. Format converters ─────────────────────────────────────────────────────

class ToVttTests(TestCase):

    def test_starts_with_webvtt(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertTrue(out.startswith('WEBVTT'))

    def test_speaker_voice_tag(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('<v SPEAKER_00>', out)

    def test_no_voice_tag_when_speaker_is_none(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertNotIn('<v None>', out)
        self.assertIn('Goodbye', out)

    def test_timestamp_format(self):
        out = _to_vtt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('00:00:00.000 --> 00:00:02.500', out)


class ToSrtTests(TestCase):

    def test_index_starts_at_one(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertTrue(out.startswith('1\n'))

    def test_speaker_in_brackets(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('[SPEAKER_00]:', out)

    def test_no_bracket_label_when_none(self):
        out = _to_srt(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertNotIn('[None]:', out)
        self.assertIn('Goodbye', out)


class ToHtmlTests(TestCase):

    def test_article_wrapper(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('<article class="transcript">', out)

    def test_data_attributes(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('data-start="0.0"', out)
        self.assertIn('data-end="2.5"', out)

    def test_speaker_attribute(self):
        out = _to_html(_SAMPLE_SEGMENTS).decode('utf-8')
        self.assertIn('data-speaker="SPEAKER_00"', out)

    def test_no_speaker_attribute_when_none(self):
        out = _to_html([{'start': 0, 'end': 1, 'text': 'Hi'}]).decode('utf-8')
        self.assertNotIn('data-speaker', out)


class ToPodcastIndexJsonTests(TestCase):

    def test_structure(self):
        doc = json.loads(_to_podcast_index_json(_SAMPLE_SEGMENTS).decode('utf-8'))
        self.assertEqual(doc['version'], '1.0.0')
        seg = doc['segments'][0]
        self.assertEqual(seg['startTime'], 0.0)
        self.assertEqual(seg['body'], 'Hello world')
        self.assertEqual(seg['speaker'], 'SPEAKER_00')

    def test_no_speaker_key_when_absent(self):
        doc = json.loads(_to_podcast_index_json([{'start': 0, 'end': 1, 'text': 'Hi'}]).decode('utf-8'))
        self.assertNotIn('speaker', doc['segments'][0])


class ToWordsJsonTests(TestCase):

    def test_metadata_embedded(self):
        meta = {'episode_id': 42, 'audio_url': 'https://ex.com/ep.mp3', 'language': 'en',
                'model': 'medium.en', 'transcribed_at': '2026-01-01T00:00:00'}
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS_WITH_WORDS, metadata=meta).decode('utf-8'))
        self.assertEqual(doc['episode_id'], 42)
        self.assertEqual(doc['audio_url'], 'https://ex.com/ep.mp3')

    def test_word_level_data_included(self):
        meta = {'episode_id': 1}
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS_WITH_WORDS, metadata=meta).decode('utf-8'))
        words = doc['segments'][0]['words']
        self.assertEqual(len(words), 2)
        self.assertEqual(words[0]['word'], 'Hello')

    def test_no_metadata_still_valid(self):
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS).decode('utf-8'))
        self.assertIn('segments', doc)

    def test_schema_version_is_1_1_0(self):
        doc = json.loads(_to_words_json(_SAMPLE_SEGMENTS).decode('utf-8'))
        self.assertEqual(doc['version'], '1.1.0')

    def test_speaker_id_emitted_at_segment_and_word_level(self):
        segs = [{
            'start': 0.0, 'end': 2.5, 'text': 'Hello', 'speaker': 'Aron',
            'speaker_id': 'SPEAKER_00',
            'words': [{'word': 'Hello', 'start': 0.0, 'end': 1.0, 'score': 0.9,
                       'speaker': 'Aron', 'speaker_id': 'SPEAKER_00'}],
        }]
        doc = json.loads(_to_words_json(segs).decode('utf-8'))
        seg = doc['segments'][0]
        self.assertEqual(seg['speaker_id'], 'SPEAKER_00')
        self.assertEqual(seg['speaker'], 'Aron')
        self.assertEqual(seg['words'][0]['speaker_id'], 'SPEAKER_00')
        self.assertEqual(seg['words'][0]['speaker'], 'Aron')

    def test_no_speaker_id_key_when_absent(self):
        doc = json.loads(_to_words_json(
            [{'start': 0, 'end': 1, 'text': 'Hi'}]).decode('utf-8'))
        self.assertNotIn('speaker_id', doc['segments'][0])


class PlainTextTests(TestCase):

    def test_joins_segment_text(self):
        self.assertEqual(_plain_text(_SAMPLE_SEGMENTS), 'Hello world Goodbye')

    def test_empty_list(self):
        self.assertEqual(_plain_text([]), '')


# ── 4. Response parser ───────────────────────────────────────────────────────

class ParseWhisperResponseTests(TestCase):

    def test_json_format_parsed(self):
        payload = json.dumps({'segments': [{'start': 0, 'end': 1, 'text': 'hi'}], 'language': 'en'})
        segs, lang = _parse_whisper_response(payload, 'en')
        self.assertEqual(len(segs), 1)
        self.assertEqual(lang, 'en')

    def test_json_language_field_overrides_fallback(self):
        payload = json.dumps({'segments': [], 'language': 'fr'})
        _, lang = _parse_whisper_response(payload, 'en')
        self.assertEqual(lang, 'fr')

    def test_srt_fallback(self):
        srt = "1\n00:00:00,000 --> 00:00:02,000\nHello world\n\n2\n00:00:02,000 --> 00:00:04,000\nGoodbye\n"
        segs, lang = _parse_whisper_response(srt, 'en')
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]['text'], 'Hello world')
        self.assertAlmostEqual(segs[0]['start'], 0.0)
        self.assertAlmostEqual(segs[0]['end'], 2.0)
        self.assertEqual(lang, 'en')

    def test_prefixed_json_skips_leading_line(self):
        prefix = "transcribing...\n"
        payload = prefix + json.dumps({'segments': [{'start': 0, 'end': 1, 'text': 'x'}], 'language': 'en'})
        segs, _ = _parse_whisper_response(payload, 'en')
        self.assertEqual(len(segs), 1)

    def test_completely_unparseable_raises_value_error(self):
        with self.assertRaises(ValueError):
            _parse_whisper_response('not json not srt at all', 'en')

    def test_speaker_id_stamped_from_diarization_label(self):
        payload = json.dumps({'language': 'en', 'segments': [{
            'start': 0, 'end': 1, 'text': 'hi', 'speaker': 'SPEAKER_03',
            'words': [{'word': 'hi', 'start': 0, 'end': 1, 'speaker': 'SPEAKER_03'}],
        }]})
        segs, _ = _parse_whisper_response(payload, 'en')
        # At initial transcription speaker == speaker_id == the raw label.
        self.assertEqual(segs[0]['speaker_id'], 'SPEAKER_03')
        self.assertEqual(segs[0]['speaker'], 'SPEAKER_03')
        self.assertEqual(segs[0]['words'][0]['speaker_id'], 'SPEAKER_03')

    def test_no_speaker_id_when_no_diarization(self):
        srt = "1\n00:00:00,000 --> 00:00:02,000\nHello world\n"
        segs, _ = _parse_whisper_response(srt, 'en')
        self.assertNotIn('speaker_id', segs[0])


class ParseSrtTests(TestCase):

    def test_basic_block(self):
        srt = "1\n00:00:01,000 --> 00:00:02,500\nLine one\n\n"
        segs = _parse_srt(srt)
        self.assertEqual(len(segs), 1)
        self.assertAlmostEqual(segs[0]['start'], 1.0)
        self.assertAlmostEqual(segs[0]['end'], 2.5)
        self.assertEqual(segs[0]['text'], 'Line one')

    def test_srt_timestamp_parsing(self):
        self.assertAlmostEqual(_parse_srt_timestamp('01:02:03,456'), 3723.456)

    def test_empty_input_returns_empty(self):
        self.assertEqual(_parse_srt(''), [])


# ── 5. Transcript model ──────────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class TranscriptModelTests(TestCase):

    def setUp(self):
        _, _, self.ep = _make_fixture()
        self.transcript = Transcript.objects.create(
            episode=self.ep,
            status=Transcript.Status.COMPLETED,
            vtt_file='transcriptions/0/1.vtt',
            json_file='transcriptions/0/1.json',
            srt_file='transcriptions/0/1.srt',
            html_file='transcriptions/0/1.html',
            words_json_file='transcriptions/0/1.words',
        )

    def test_get_url_returns_url_with_episode_id(self):
        url = self.transcript.get_url('vtt')
        self.assertIsNotNone(url)
        self.assertIn(str(self.ep.id), url)

    def test_get_url_words_maps_to_words_json_file(self):
        url = self.transcript.get_url('words')
        self.assertIsNotNone(url)

    def test_get_url_returns_none_when_file_field_empty(self):
        self.transcript.vtt_file = None
        self.transcript.save()
        self.assertIsNone(self.transcript.get_url('vtt'))


# ── 6. queue_transcription_on_episode_save signal ────────────────────────────

@override_settings(**{**TRANSCRIPTION_SETTINGS, 'WHISPER_ENABLED': True})
class QueueTranscriptionSignalTests(TestCase):

    def setUp(self):
        cache.clear()
        self.net = Network.objects.create(name='SigNet', slug='signet')
        self.pod = Podcast.objects.create(network=self.net, title='SigPod', slug='sigpod')

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_queues_new_episode_with_subscriber_audio(self, mock_dispatch):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep.mp3',
        )
        mock_dispatch.assert_called_once_with(ep.id)
        self.assertTrue(Transcript.objects.filter(episode=ep, status=Transcript.Status.PENDING).exists())

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_no_queue_without_subscriber_audio(self, mock_dispatch):
        Episode.objects.create(
            podcast=self.pod, title='Ep2', pub_date=timezone.now(),
            raw_description='x', audio_url_public='https://cdn.example.com/pub.mp3',
        )
        mock_dispatch.assert_not_called()

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_no_requeue_when_already_pending(self, mock_dispatch):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep3', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep3.mp3',
        )
        mock_dispatch.reset_mock()
        ep.title = 'Updated'
        ep.save()
        mock_dispatch.assert_not_called()

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_no_requeue_on_update_after_failure(self, mock_dispatch):
        # Auto-queue only fires on creation. Failed transcripts must be manually
        # re-queued by a network owner via the retranscribe API.
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep4', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep4.mp3',
        )
        Transcript.objects.filter(episode=ep).update(status=Transcript.Status.FAILED)
        mock_dispatch.reset_mock()
        ep.title = 'Retried'
        ep.save()
        mock_dispatch.assert_not_called()

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_no_queue_when_whisper_disabled(self, mock_dispatch):
        with override_settings(WHISPER_ENABLED=False):
            Episode.objects.create(
                podcast=self.pod, title='Ep5', pub_date=timezone.now(),
                raw_description='x', audio_url_subscriber='https://cdn.example.com/ep5.mp3',
            )
        mock_dispatch.assert_not_called()

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_no_queue_on_episode_update(self, mock_dispatch):
        ep = Episode.objects.create(
            podcast=self.pod, title='Ep6', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep6.mp3',
        )
        mock_dispatch.reset_mock()
        # Simulates what the feed ingester does: update an existing episode
        ep.title = 'Ep6 Updated'
        ep.save()
        mock_dispatch.assert_not_called()

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_awaiting_recovery_requeues_when_audio_url_changes(self, mock_dispatch):
        ep = Episode.objects.create(
            podcast=self.pod, title='EpAR', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://dead.example.com/a.mp3',
        )
        # Park it awaiting recovery against the dead source it last attempted.
        Transcript.objects.filter(episode=ep).update(
            status=Transcript.Status.AWAITING_RECOVERY,
            source_audio_url='https://dead.example.com/a.mp3',
        )
        mock_dispatch.reset_mock()
        ep.audio_url_subscriber = 'https://live.example.com/a.mp3'
        ep.save()
        mock_dispatch.assert_called_once_with(ep.id)
        self.assertTrue(
            Transcript.objects.filter(episode=ep, status=Transcript.Status.PENDING).exists()
        )

    @mock.patch('pod_manager.services.transcription.dispatch_transcription')
    def test_awaiting_recovery_no_requeue_when_url_unchanged(self, mock_dispatch):
        ep = Episode.objects.create(
            podcast=self.pod, title='EpAR2', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://dead.example.com/b.mp3',
        )
        Transcript.objects.filter(episode=ep).update(
            status=Transcript.Status.AWAITING_RECOVERY,
            source_audio_url='https://dead.example.com/b.mp3',
        )
        mock_dispatch.reset_mock()
        ep.title = 'Touched but same URL'
        ep.save()
        mock_dispatch.assert_not_called()
        self.assertEqual(
            Transcript.objects.get(episode=ep).status, Transcript.Status.AWAITING_RECOVERY
        )

    @mock.patch('pod_manager.services.transcription.run_transcription')
    def test_eager_dispatch_defers_to_thread_after_commit(self, mock_run):
        """Under eager Celery the signal must NOT run whisper inline inside
        Episode.save() — it schedules a post-commit thread instead. In a
        TestCase the transaction never commits, so nothing may have run."""
        from pod_manager.tasks import transcribe_episode
        with mock.patch.object(transcribe_episode, 'delay') as mock_delay:
            Episode.objects.create(
                podcast=self.pod, title='Ep7', pub_date=timezone.now(),
                raw_description='x', audio_url_subscriber='https://cdn.example.com/ep7.mp3',
            )
            mock_delay.assert_not_called()
        mock_run.assert_not_called()


# ── 7. run_transcription() service ───────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class RunTranscriptionTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.net, self.pod, self.ep = _make_fixture()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _settings(self, **extra):
        return {**TRANSCRIPTION_SETTINGS, 'MEDIA_ROOT': self.tmp, **extra}

    def _mock_dl(self, mock_get):
        mock_get.return_value.raise_for_status = mock.MagicMock()
        # is_audio_file() magic-byte-sniffs the download (audio_sniff.py) to
        # reject HTML interstitials — an ID3 tag prefix satisfies that check.
        mock_get.return_value.iter_content.return_value = [b'ID3' + b'\x00' * 9 + b'mp3data']

    def _session_get(self, mock_session_cls):
        """_download_audio_to_temp downloads via a requests.Session() instance
        (added for GDrive cookie/User-Agent support), not the bare requests.get
        function — so tests must patch requests.Session and pull .get off the
        instance mock it returns, not requests.get itself."""
        return mock_session_cls.return_value.get

    def _mock_asr(self, mock_post, text=_MOCK_ASR_JSON):
        mock_post.return_value.raise_for_status = mock.MagicMock()
        mock_post.return_value.text = text

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_happy_path_status_and_fields(self, mock_session_cls, mock_post, mock_rebuild):
        self._mock_dl(self._session_get(mock_session_cls))
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.COMPLETED)
        self.assertEqual(t.transcript_text, 'Hello World')
        self.assertEqual(t.whisper_model_used, 'medium.en')
        self.assertEqual(t.language, 'en')
        mock_rebuild.delay.assert_called_once()

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_all_five_files_written_to_disk(self, mock_session_cls, mock_post, mock_rebuild):
        self._mock_dl(self._session_get(mock_session_cls))
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        for field in ('vtt_file', 'srt_file', 'json_file', 'html_file', 'words_json_file'):
            p = Path(self.tmp) / getattr(t, field)
            self.assertTrue(p.exists(), f"{field} not written to disk")

    def test_skips_when_whisper_disabled(self):
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=False)):
            run_transcription(self.ep.id)
        self.assertFalse(Transcript.objects.filter(episode=self.ep).exists())

    def test_skips_episode_without_subscriber_audio(self):
        _, _, ep_pub = _make_fixture(subscriber=False)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(ep_pub.id)
        self.assertFalse(Transcript.objects.filter(episode=ep_pub).exists())

    def test_missing_episode_returns_gracefully(self):
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(99999)
        self.assertFalse(Transcript.objects.filter(episode_id=99999).exists())

    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_failure_marks_failed_and_increments_retry(self, mock_session_cls, mock_post):
        self._mock_dl(self._session_get(mock_session_cls))
        mock_post.side_effect = Exception('ASR down')
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            with self.assertRaises(Exception):
                run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.FAILED)
        self.assertEqual(t.retry_count, 1)
        self.assertIn('ASR down', t.error_message)

    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_permanent_source_error_parks_awaiting_recovery(self, mock_session_cls, mock_post):
        import requests as _rq
        mock_get = self._session_get(mock_session_cls)
        resp = mock.MagicMock(); resp.status_code = 404
        mock_get.return_value.raise_for_status.side_effect = _rq.exceptions.HTTPError(response=resp)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)  # must NOT raise → Celery task won't retry
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.AWAITING_RECOVERY)
        self.assertEqual(t.retry_count, 0)
        mock_post.assert_not_called()

    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_transient_source_error_marks_failed_and_raises(self, mock_session_cls, mock_post):
        import requests as _rq
        mock_get = self._session_get(mock_session_cls)
        resp = mock.MagicMock(); resp.status_code = 503
        mock_get.return_value.raise_for_status.side_effect = _rq.exceptions.HTTPError(response=resp)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            with self.assertRaises(Exception):
                run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.FAILED)
        self.assertEqual(t.retry_count, 1)

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_podcast_override_wins_over_network(self, mock_session_cls, mock_post, mock_rebuild):
        """Podcast-level whisper_model takes precedence over network default."""
        self.net.whisper_model = 'large'
        self.net.save()
        self.pod.whisper_model = 'base'
        self.pod.save()
        self._mock_dl(self._session_get(mock_session_cls))
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'base')

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_call_kwarg_wins_over_podcast_and_network(self, mock_session_cls, mock_post, mock_rebuild):
        """Per-call model kwarg overrides all other levels."""
        self.pod.whisper_model = 'base'
        self.pod.save()
        self._mock_dl(self._session_get(mock_session_cls))
        self._mock_asr(mock_post)
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True)):
            run_transcription(self.ep.id, model='small')
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'small')

    @mock.patch('pod_manager.tasks.task_rebuild_episode_fragments')
    @mock.patch('pod_manager.services.transcription.requests.post')
    @mock.patch('pod_manager.services.transcription.requests.Session')
    def test_global_settings_used_when_no_overrides(self, mock_session_cls, mock_post, mock_rebuild):
        """Falls back to settings.WHISPER_MODEL when podcast and network have no overrides."""
        self._mock_dl(self._session_get(mock_session_cls))
        self._mock_asr(mock_post)
        # Clear all model overrides so the settings fallback is actually reached.
        self.net.whisper_model = ''
        self.net.save()
        from pod_manager.services.transcription import run_transcription
        with override_settings(**self._settings(WHISPER_ENABLED=True, WHISPER_MODEL='tiny')):
            run_transcription(self.ep.id)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.whisper_model_used, 'tiny')


# ── 7b. Idempotent R2 transcript writes (§4) ─────────────────────────────────

class MediaObjectEtagTests(TestCase):
    """media_object_etag(): unquoted ETag from a HEAD, None on 404."""

    def test_returns_unquoted_etag(self):
        from pod_manager.services import r2_storage
        client = mock.MagicMock()
        client.head_object.return_value = {'ETag': '"abc123"'}
        with mock.patch.object(r2_storage, 'get_r2_client', return_value=client):
            self.assertEqual(r2_storage.media_object_etag('transcripts/0/1.words'), 'abc123')

    def test_returns_none_on_404(self):
        from pod_manager.services import r2_storage
        client = mock.MagicMock()
        client.head_object.side_effect = _not_found_error()
        with mock.patch.object(r2_storage, 'get_r2_client', return_value=client):
            self.assertIsNone(r2_storage.media_object_etag('transcripts/0/1.words'))


@override_settings(R2_MEDIA_ENABLED=True)
class WriteTranscriptFormatsR2Tests(TestCase):
    """write_transcript_formats(): hash-before-PUT skips unchanged formats and
    reports only changed exts (drives the version bump)."""

    def _rendered(self):
        return [
            ('vtt',   b'WEBVTT vtt-bytes'),
            ('json',  b'{"json": true}'),
            ('srt',   b'1 srt-bytes'),
            ('html',  b'<article></article>'),
            ('words', b'{"words": true}'),
        ]

    def test_only_changed_formats_are_put(self):
        from pod_manager.services import transcription as tx
        rendered = self._rendered()
        # 'words' differs (md5 mismatch); every other format already matches.
        etags = {
            tx.transcript_r2_key(7, ext): _hashlib.md5(content).hexdigest()
            for ext, content in rendered
        }
        etags[tx.transcript_r2_key(7, 'words')] = 'deadbeef' * 4  # 32-char md5 that won't match
        put_calls = []
        with mock.patch('pod_manager.services.r2_storage.media_object_etag',
                        side_effect=lambda key: etags.get(key)), \
             mock.patch('pod_manager.services.r2_storage.put_media_object',
                        side_effect=lambda key, content, ct: put_calls.append(key)):
            markers, changed = write_transcript_formats(7, rendered)
        self.assertEqual(changed, ['words'])
        self.assertEqual(put_calls, [tx.transcript_r2_key(7, 'words')])
        self.assertEqual(set(markers), {'vtt', 'json', 'srt', 'html', 'words'})

    def test_missing_object_is_put(self):
        put_calls = []
        with mock.patch('pod_manager.services.r2_storage.media_object_etag',
                        side_effect=lambda key: None), \
             mock.patch('pod_manager.services.r2_storage.put_media_object',
                        side_effect=lambda key, content, ct: put_calls.append(key)):
            markers, changed = write_transcript_formats(7, self._rendered())
        self.assertEqual(len(changed), 5)
        self.assertEqual(len(put_calls), 5)

    def test_multipart_etag_falls_back_to_get_and_hash(self):
        rendered = [('words', b'{"words": true}')]
        with mock.patch('pod_manager.services.r2_storage.media_object_etag',
                        side_effect=lambda key: 'abc-2'), \
             mock.patch('pod_manager.services.r2_storage.get_media_object',
                        side_effect=lambda key: (b'{"words": true}', 'application/json')), \
             mock.patch('pod_manager.services.r2_storage.put_media_object') as put:
            markers, changed = write_transcript_formats(7, rendered)
        self.assertEqual(changed, [])  # bytes matched via GET fallback
        put.assert_not_called()


# ── 8. apply_speaker_labels() ────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ApplySpeakerLabelsTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _, _, self.ep = _make_fixture()
        self.user = User.objects.create_user(username=f'labeller-{self.ep.id}')
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _approve(self, mappings, *, resolved_at=None):
        """Create an APPROVED speaker edit — one delta in the replay chain. Replay
        folds these (no mappings argument), so a test sets state by approving edits."""
        return EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': mappings},
            original_data={'speaker_mappings': {}},
            status=EpisodeEditSuggestion.Status.APPROVED,
            resolved_at=resolved_at or timezone.now(),
        )

    def _write_files(self, segments):
        """Write a .words file (plus stub files for other formats) into tmp."""
        with override_settings(MEDIA_ROOT=self.tmp):
            for ext in ('vtt', 'srt', 'json', 'html'):
                p = transcript_path(self.ep.id, ext)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b'stub')
            meta = {
                'episode_id': self.ep.id,
                'audio_url': 'https://x.com/a.mp3',
                'language': 'en',
                'model': 'medium.en',
                'transcribed_at': '2026-01-01T00:00:00',
            }
            words_p = transcript_path(self.ep.id, 'words')
            words_p.write_bytes(_to_words_json(segments, metadata=meta))
            rel_words = str(words_p.relative_to(self.tmp))
            rel_vtt   = str(transcript_path(self.ep.id, 'vtt').relative_to(self.tmp))
        self.transcript.words_json_file = rel_words
        self.transcript.vtt_file        = rel_vtt
        self.transcript.save()

    def test_speaker_label_applied_in_vtt(self):
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00',
                 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        self._approve({'SPEAKER_00': 'Jim'})
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            vtt = transcript_path(self.ep.id, 'vtt').read_bytes().decode('utf-8')
        self.assertIn('Jim', vtt)
        self.assertNotIn('SPEAKER_00', vtt)

    def test_words_json_updated_with_mapping_record(self):
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00',
                 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        self._approve({'SPEAKER_00': 'Jim'})
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['speaker_mappings'], {'SPEAKER_00': 'Jim'})
        self.assertEqual(doc['segments'][0]['speaker'], 'Jim')
        # speaker_id is the immutable base — never rewritten by replay.
        self.assertEqual(doc['segments'][0]['speaker_id'], 'SPEAKER_00')

    def test_unmapped_speaker_preserved_unchanged(self):
        segs = [
            {'start': 0, 'end': 1, 'text': 'Hi',    'speaker_id': 'SPEAKER_00', 'speaker': 'SPEAKER_00', 'words': []},
            {'start': 1, 'end': 2, 'text': 'There',  'speaker_id': 'SPEAKER_01', 'speaker': 'SPEAKER_01', 'words': []},
        ]
        self._write_files(segs)
        self._approve({'SPEAKER_00': 'Jim'})
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['segments'][1]['speaker'], 'SPEAKER_01')

    def test_chain_is_last_writer_wins_by_resolved_at(self):
        """A later approved edit overrides an earlier one per speaker_id (§3.2)."""
        from datetime import timedelta
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00',
                 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        t0 = timezone.now()
        self._approve({'SPEAKER_00': 'Jim'}, resolved_at=t0)
        self._approve({'SPEAKER_00': 'A.Ron'}, resolved_at=t0 + timedelta(minutes=1))
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['segments'][0]['speaker'], 'A.Ron')

    def test_rolled_back_edit_excluded_from_replay(self):
        """Removing an edit (ROLLED_BACK) and replaying restores the prior name —
        the griefing-recovery / rollback guarantee (§3.4)."""
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00',
                 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        edit = self._approve({'SPEAKER_00': 'Jim'})
        edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        edit.save()
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        # Empty chain → name falls back to the immutable speaker_id base.
        self.assertEqual(doc['segments'][0]['speaker'], 'SPEAKER_00')

    def test_no_speaker_id_falls_back_to_speaker(self):
        """Pre-backfill .words (no speaker_id) still labels via the speaker fallback."""
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_files(segs)
        self._approve({'SPEAKER_00': 'Jim'})
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)
            doc = json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))
        self.assertEqual(doc['segments'][0]['speaker'], 'Jim')

    def test_missing_words_file_returns_gracefully(self):
        # No files written — should not raise
        self._approve({'SPEAKER_00': 'Jim'})
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)

    def test_non_completed_transcript_returns_gracefully(self):
        self.transcript.status = Transcript.Status.PENDING
        self.transcript.save()
        with override_settings(MEDIA_ROOT=self.tmp):
            apply_speaker_labels(self.ep.id)


# ── Phase 7: listener context, §8b review diff, submit key validation ─────────

class EpisodeDetailSpeakerContextTests(TestCase):
    """The episode page exposes, per speaker_id, the CURRENT resolved name derived
    from fold_speaker_mappings over the .words speaker_id base (not seg.speaker)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='spk-ctx')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='spk-ctx-show')
        self.user = User.objects.create_user(username='spk-ctx-listener')
        NetworkMembership.objects.create(user=self.user, network=self.network)
        self.ep = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='hi', clean_description='hi',
            audio_url_public='https://cdn.example.com/a.mp3', is_published=True,
        )
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=1,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_words(self, segments):
        with override_settings(MEDIA_ROOT=self.tmp):
            wp = transcript_path(self.ep.id, 'words')
            wp.parent.mkdir(parents=True, exist_ok=True)
            wp.write_bytes(_to_words_json(segments, metadata={'episode_id': self.ep.id}))
        self.transcript.words_json_file = str(wp.relative_to(self.tmp))
        self.transcript.html_file = ''
        self.transcript.save()

    def _render(self):
        req = _make_tenant_request(self.factory, self.network,
                                   path=f'/episode/{self.ep.id}/', user=self.user)
        with override_settings(MEDIA_ROOT=self.tmp):
            return views.episode_detail(req, self.ep.id).content.decode('utf-8')

    def _speaker_data(self, body):
        m = re.search(r'const speakerData = (\[.*?\]);', body)
        self.assertIsNotNone(m, "speakerData JSON not found in page")
        return json.loads(m.group(1))

    def test_resolved_name_comes_from_fold_not_seg_speaker(self):
        # .words still carries the raw label in seg.speaker (apply not run), but the
        # context resolves names from the APPROVED fold → Aron.
        segs = [
            {'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00', 'speaker': 'SPEAKER_00', 'words': []},
            {'start': 1, 'end': 2, 'text': 'Yo', 'speaker_id': 'SPEAKER_01', 'speaker': 'SPEAKER_01', 'words': []},
        ]
        self._write_words(segs)
        EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Aron'}},
            status=EpisodeEditSuggestion.Status.APPROVED, resolved_at=timezone.now(),
        )
        names = {d['id']: d['name'] for d in self._speaker_data(self._render())}
        self.assertEqual(names['SPEAKER_00'], 'Aron')
        self.assertEqual(names['SPEAKER_01'], 'SPEAKER_01')  # unmapped → raw label

    def test_pre_backfill_falls_back_to_speaker(self):
        segs = [{'start': 0, 'end': 1, 'text': 'Hi', 'speaker': 'SPEAKER_00', 'words': []}]
        self._write_words(segs)
        data = self._speaker_data(self._render())
        self.assertEqual(data[0]['id'], 'SPEAKER_00')
        self.assertEqual(data[0]['name'], 'SPEAKER_00')


class SpeakerDiffAnnotationTests(TestCase):
    """§8b: _annotate_edit_changes / gather_inbox build a structured per-speaker
    before→after diff (and drive has_changes via speaker_changed)."""

    def setUp(self):
        from pod_manager.views.creator.data import _annotate_edit_changes, gather_inbox
        self._annotate = _annotate_edit_changes
        self._gather_inbox = gather_inbox
        self.user = User.objects.create_user(username='diff-user')
        self.network = Network.objects.create(name='Net', slug='diff-net')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='diff-show')
        self.ep = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3',
        )

    def _edit(self, suggested, original, **kw):
        return EpisodeEditSuggestion(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': suggested},
            original_data={'speaker_mappings': original}, **kw,
        )

    def test_correction_before_after_from_original(self):
        edit = self._edit({'SPEAKER_00': 'Jim'}, {'SPEAKER_00': 'Aron'})
        self._annotate(edit)
        self.assertEqual(edit.speaker_diff, [('SPEAKER_00', 'Aron', 'Jim')])
        self.assertTrue(edit.speaker_changed)
        self.assertTrue(edit.has_changes)

    def test_first_time_naming_before_is_raw_label(self):
        edit = self._edit({'SPEAKER_02': 'Aron'}, {})
        self._annotate(edit)
        self.assertEqual(edit.speaker_diff, [('SPEAKER_02', 'SPEAKER_02', 'Aron')])
        self.assertTrue(edit.speaker_changed)

    def test_noop_rename_is_not_a_change(self):
        edit = self._edit({'SPEAKER_00': 'Aron'}, {'SPEAKER_00': 'Aron'})
        self._annotate(edit)
        self.assertFalse(edit.speaker_changed)
        self.assertFalse(edit.has_changes)

    def test_gather_inbox_diffs_against_current_fold(self):
        # APPROVED edit made the live name Aron; the pending edit (with a stale,
        # empty snapshot) renames to Jim. gather_inbox must show Aron → Jim.
        EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Aron'}},
            status=EpisodeEditSuggestion.Status.APPROVED, resolved_at=timezone.now(),
        )
        EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': {'SPEAKER_00': 'Jim'}},
            original_data={'speaker_mappings': {}},
            status=EpisodeEditSuggestion.Status.PENDING,
        )
        edit = list(self._gather_inbox(self.network)['pending_edits'])[0]
        self.assertEqual(edit.speaker_diff, [('SPEAKER_00', 'Aron', 'Jim')])
        self.assertTrue(edit.speaker_changed)


class SubmitSpeakerLabelsValidationTests(TestCase):
    """submit_speaker_labels rejects keys that aren't known speaker_ids for the
    episode (§5.1), before any scoring/banking."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='val-net', auto_approve_trust_threshold=100)
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='val-show')
        self.user = User.objects.create_user(username='val-user')
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=0)
        self.ep = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3',
        )
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=1,
        )
        with override_settings(MEDIA_ROOT=self.tmp):
            wp = transcript_path(self.ep.id, 'words')
            wp.parent.mkdir(parents=True, exist_ok=True)
            wp.write_bytes(_to_words_json([
                {'start': 0, 'end': 1, 'text': 'Hi', 'speaker_id': 'SPEAKER_00', 'speaker': 'SPEAKER_00', 'words': []},
                {'start': 1, 'end': 2, 'text': 'Yo', 'speaker_id': 'SPEAKER_01', 'speaker': 'SPEAKER_01', 'words': []},
            ], metadata={'episode_id': self.ep.id}))
        self.transcript.words_json_file = str(wp.relative_to(self.tmp))
        self.transcript.save()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _post(self, mappings):
        req = self.factory.post(
            reverse('submit_speaker_labels', args=[self.ep.id]),
            data=json.dumps({'speaker_mappings': mappings}),
            content_type='application/json',
        )
        req.user = self.user
        req.network = self.network
        with override_settings(MEDIA_ROOT=self.tmp):
            return views.submit_speaker_labels(req, self.ep.id)

    def test_unknown_key_rejected(self):
        resp = self._post({'SPEAKER_99': 'Ghost'})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(EpisodeEditSuggestion.objects.filter(episode=self.ep).exists())

    def test_known_key_accepted(self):
        resp = self._post({'SPEAKER_00': 'Aron'})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(EpisodeEditSuggestion.objects.filter(episode=self.ep).exists())

    def test_mixed_known_and_unknown_rejected_atomically(self):
        resp = self._post({'SPEAKER_00': 'Aron', 'SPEAKER_77': 'X'})
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(EpisodeEditSuggestion.objects.filter(episode=self.ep).exists())


class TrustBreakdownTallyTests(TestCase):
    """The approve-desk preview and the audit-log tally both split the award into
    Sections + Sweep + First-responder; the parts must reconcile to the total the
    Revert button reverses (edit.points for banked rows)."""

    def setUp(self):
        from pod_manager.views.creator.data import (
            gather_inbox, gather_audit_log, annotate_audit_edit,
        )
        self._gather_inbox = gather_inbox
        self._gather_audit_log = gather_audit_log
        # The audit breakdown moved out of gather_audit_log with S2.2: that
        # gather now emits collapsed summaries and this tally is computed per
        # expanded edit, by the lazy diff endpoint.
        self._annotate_audit_edit = annotate_audit_edit
        self.factory = RequestFactory()
        self.user = User.objects.create_user(username='tally-user')
        self.network = Network.objects.create(name='Net', slug='tally-net')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='tally-show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Orig', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3', tags=[], chapters_public=[],
        )

    def test_inbox_preview_sections_plus_bonus_equals_total(self):
        EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.user, status=EpisodeEditSuggestion.Status.PENDING,
            original_data={'title': 'Orig', 'tags': [], 'chapters': [], 'season_number': None},
            suggested_data={
                'title': 'New', 'tags': ['a', 'b'],
                'chapters': [{'startTime': 0, 'title': 'c1'}, {'startTime': 5, 'title': 'c2'}],
                'season_number': 2,
            },
        )
        edit = list(self._gather_inbox(self.network)['pending_edits'])[0]
        # title1 + tags1(flat) + chapters2 + season1 = base 5; 4 core fields -> sweep +2.
        self.assertEqual(edit.base_points, 5)
        self.assertEqual(edit.sweep_bonus, 2)
        self.assertEqual(edit.fr_bonus, 0)
        self.assertEqual(edit.base_points + edit.sweep_bonus + edit.fr_bonus, edit.total_points_preview)

    def test_audit_breakdown_reconciles_to_banked_points(self):
        EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.user, status=EpisodeEditSuggestion.Status.APPROVED,
            resolved_at=timezone.now(),
            original_data={'title': 'Orig'}, suggested_data={'title': 'New'},
            points=7, counter_deltas={'edits_title': 1},
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=10)
        edit = self._annotate_audit_edit(
            EpisodeEditSuggestion.objects.get(user=self.user), self.network)
        self.assertEqual(edit.total_points, 7)
        self.assertEqual(edit.base_points + edit.sweep_bonus + edit.fr_bonus, edit.total_points)
        self.assertEqual(edit.trust_after_revert, 3)  # 10 - 7

    def test_audit_points_badge_by_status(self):
        """Collapsed-row net-trust badge: approved=+points, rejected=-penalty,
        rolled_back=+0 (washes)."""
        from pod_manager.services.edits import REJECT_PENALTY
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=20)
        common = dict(episode=self.episode, user=self.user, resolved_at=timezone.now(),
                      original_data={'title': 'Orig'}, suggested_data={'title': 'New'})
        EpisodeEditSuggestion.objects.create(
            status=EpisodeEditSuggestion.Status.APPROVED, points=7,
            counter_deltas={'edits_title': 1}, **common)
        EpisodeEditSuggestion.objects.create(
            status=EpisodeEditSuggestion.Status.REJECTED, points=0, counter_deltas={}, **common)
        EpisodeEditSuggestion.objects.create(
            status=EpisodeEditSuggestion.Status.ROLLED_BACK, points=7,
            counter_deltas={'edits_title': 1}, **common)
        req = self.factory.get('/', {'network': self.network.slug})
        by_status = {e.status: e.audit_points
                     for e in self._gather_audit_log(req, self.network)['audit_page_obj']}
        self.assertEqual(by_status[EpisodeEditSuggestion.Status.APPROVED], 7)
        self.assertEqual(by_status[EpisodeEditSuggestion.Status.REJECTED], -REJECT_PENALTY)
        self.assertEqual(by_status[EpisodeEditSuggestion.Status.ROLLED_BACK], 0)

    def test_audit_legacy_unbanked_edit_shows_no_phantom_points(self):
        EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.user, status=EpisodeEditSuggestion.Status.APPROVED,
            resolved_at=timezone.now(),
            original_data={'title': 'Orig'}, suggested_data={'title': 'New'},
            points=0, counter_deltas={},
        )
        NetworkMembership.objects.create(user=self.user, network=self.network, trust_score=4)
        edit = self._annotate_audit_edit(
            EpisodeEditSuggestion.objects.get(user=self.user), self.network)
        self.assertEqual(edit.total_points, 0)
        self.assertEqual(edit.base_points, 0)
        self.assertEqual(edit.pts_title, 0)
        self.assertEqual(edit.trust_after_revert, 4)  # nothing to reverse


# ── 8a. speaker_edit_points() helper (§3.4) ──────────────────────────────────

class SpeakerEditPointsTests(SimpleTestCase):
    """Per-speaker award math: +1 per distinct (prior name → new name) change. A
    rename cascading to many ids scores once; a split (one name → several names)
    scores per distinct target."""

    def _points(self, edit_mappings, prior):
        from pod_manager.services.transcription import speaker_edit_points
        return speaker_edit_points(edit_mappings, prior)

    def test_first_time_naming_scores_one_each(self):
        self.assertEqual(self._points({'SPEAKER_00': 'Jim', 'SPEAKER_01': 'Aron'}, {}), (2, 2))

    def test_two_ids_to_one_name_first_time_scores_two(self):
        # Two distinct raw labels → Aron: distinct prior values, so two changes.
        self.assertEqual(self._points({'SPEAKER_00': 'Aron', 'SPEAKER_03': 'Aron'}, {}), (2, 2))

    def test_correction_scores_one(self):
        # Already named Aron → renamed Jim: one change, no new naming.
        self.assertEqual(self._points({'SPEAKER_00': 'Jim'}, {'SPEAKER_00': 'Aron'}), (1, 0))

    def test_mixed_naming_and_correction(self):
        prior = {'SPEAKER_00': 'Aron'}
        # SPEAKER_01 newly named, SPEAKER_00 corrected → 2 changes, newly 1.
        self.assertEqual(self._points({'SPEAKER_00': 'Jim', 'SPEAKER_01': 'Bob'}, prior), (2, 1))

    def test_noop_rename_to_raw_label_scores_zero(self):
        self.assertEqual(self._points({'SPEAKER_00': 'SPEAKER_00'}, {}), (0, 0))

    def test_rename_cascade_to_many_ids_scores_once(self):
        # Aron (00,02) → Jim, Jim (03,01) → Aron: two distinct changes regardless of
        # how many ids each name spans → +2.
        prior = {'SPEAKER_00': 'Aron', 'SPEAKER_02': 'Aron',
                 'SPEAKER_03': 'Jim', 'SPEAKER_01': 'Jim'}
        edit = {'SPEAKER_00': 'Jim', 'SPEAKER_02': 'Jim',
                'SPEAKER_03': 'Aron', 'SPEAKER_01': 'Aron'}
        self.assertEqual(self._points(edit, prior), (2, 0))

    def test_split_one_name_into_several_scores_per_target(self):
        # Aron (01) → Jim, Aron (02) → Roy, Jim (03) → Aron, Jim (00) → Dan:
        # four distinct (prior → new) pairs → +4 (the lossless un-collapse).
        prior = {'SPEAKER_01': 'Aron', 'SPEAKER_02': 'Aron',
                 'SPEAKER_03': 'Jim', 'SPEAKER_00': 'Jim'}
        edit = {'SPEAKER_01': 'Jim', 'SPEAKER_02': 'Roy',
                'SPEAKER_03': 'Aron', 'SPEAKER_00': 'Dan'}
        self.assertEqual(self._points(edit, prior), (4, 0))

    def test_partial_split_skips_noops(self):
        # Aron (01) → Roy, Aron (02) → Aron (no-op), Jim (03) → Jim (no-op),
        # Jim (00) → Dan: only the two real changes score → +2.
        prior = {'SPEAKER_01': 'Aron', 'SPEAKER_02': 'Aron',
                 'SPEAKER_03': 'Jim', 'SPEAKER_00': 'Jim'}
        edit = {'SPEAKER_01': 'Roy', 'SPEAKER_02': 'Aron',
                'SPEAKER_03': 'Jim', 'SPEAKER_00': 'Dan'}
        self.assertEqual(self._points(edit, prior), (2, 0))

    def test_two_different_names_merged_to_one_scores_two(self):
        # Two distinct people both declared to be Jim: each name changed → +2.
        prior = {'SPEAKER_00': 'Aron', 'SPEAKER_01': 'Bob'}
        self.assertEqual(self._points({'SPEAKER_00': 'Jim', 'SPEAKER_01': 'Jim'}, prior), (2, 0))


# ── 8b. supersede_speaker_edits() (re-transcription, §7) ─────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class SupersedeSpeakerEditsTests(TestCase):

    def setUp(self):
        _, _, self.ep = _make_fixture()
        self.user = User.objects.create_user(username=f'sup-{self.ep.id}')
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)

    def _edit(self, status, data):
        return EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user, status=status,
            suggested_data=data, original_data={},
            resolved_at=timezone.now() if status != EpisodeEditSuggestion.Status.PENDING else None,
        )

    def test_supersedes_approved_and_pending_speaker_edits(self):
        from pod_manager.services.transcription import supersede_speaker_edits
        S = EpisodeEditSuggestion.Status
        approved = self._edit(S.APPROVED, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        pending = self._edit(S.PENDING, {'speaker_mappings': {'SPEAKER_01': 'A.Ron'}})

        n = supersede_speaker_edits(self.ep.id)

        self.assertEqual(n, 2)
        approved.refresh_from_db(); pending.refresh_from_db()
        self.assertEqual(approved.status, S.SUPERSEDED)
        self.assertEqual(pending.status, S.SUPERSEDED)

    def test_leaves_metadata_and_rolled_back_edits_alone(self):
        from pod_manager.services.transcription import supersede_speaker_edits
        S = EpisodeEditSuggestion.Status
        meta = self._edit(S.APPROVED, {'title': 'New'})
        rolled = self._edit(S.ROLLED_BACK, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})

        n = supersede_speaker_edits(self.ep.id)

        self.assertEqual(n, 0)
        meta.refresh_from_db(); rolled.refresh_from_db()
        self.assertEqual(meta.status, S.APPROVED)
        self.assertEqual(rolled.status, S.ROLLED_BACK)

    def test_superseded_edits_excluded_from_replay_fold(self):
        from pod_manager.services.transcription import (
            fold_speaker_mappings, supersede_speaker_edits,
        )
        self._edit(EpisodeEditSuggestion.Status.APPROVED,
                   {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        self.assertEqual(fold_speaker_mappings(self.ep.id), {'SPEAKER_00': 'Jim'})
        supersede_speaker_edits(self.ep.id)
        self.assertEqual(fold_speaker_mappings(self.ep.id), {})


# ── 8c. backfill_speaker_ids command (Phase 6, §6) ───────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class BackfillSpeakerIdsTests(TestCase):
    """One-time conversion: recover the speaker_id base for the existing catalogue
    (lossless from whisper_raw, degraded fallback otherwise) + retroactive
    edits_speakers recompute."""

    def setUp(self):
        from django.core.management import call_command
        from io import StringIO
        self._call = call_command
        self._StringIO = StringIO
        self.tmp = tempfile.mkdtemp()
        self._media = override_settings(MEDIA_ROOT=self.tmp)
        self._media.enable()
        self.net, self.pod, self.ep = _make_fixture()
        self.user = User.objects.create_user(username=f'bf-{self.ep.id}')
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, language='en',
        )

    def tearDown(self):
        self._media.disable()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ------------------------------------------------------------
    def _write_legacy_words(self, segments, *, version='1.0.0', header=None):
        """Write a pre-1.1.0 .words file (no speaker_id) + stub sibling formats."""
        for ext in ('vtt', 'srt', 'json', 'html'):
            p = transcript_path(self.ep.id, ext)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b'stub')
        doc = {'version': version}
        doc.update(header or {'language': 'en', 'model': 'medium.en',
                              'transcribed_at': '2026-01-01T00:00:00'})
        doc['segments'] = segments
        wp = transcript_path(self.ep.id, 'words')
        wp.write_bytes(json.dumps(doc).encode('utf-8'))

    def _write_raw(self, asr_json):
        raw = transcript_path(self.ep.id, 'vtt').parent / f'{self.ep.id}.whisper_raw.txt'
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_text(asr_json, encoding='utf-8')

    def _read_words(self):
        return json.loads(transcript_path(self.ep.id, 'words').read_bytes().decode('utf-8'))

    def _approve(self, mappings, *, resolved_at=None):
        return EpisodeEditSuggestion.objects.create(
            episode=self.ep, user=self.user,
            suggested_data={'speaker_mappings': mappings},
            original_data={'speaker_mappings': {}},
            status=EpisodeEditSuggestion.Status.APPROVED,
            resolved_at=resolved_at or timezone.now(),
        )

    def _run(self, *args, scope=None):
        out = self._StringIO()
        scope = scope or f'--episode={self.ep.id}'
        self._call('backfill_speaker_ids', scope, *args, stdout=out, stderr=self._StringIO())
        return out.getvalue()

    # -- tests --------------------------------------------------------------
    def test_skips_already_110(self):
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 1, 'body': 'Hi', 'speaker_id': 'SPEAKER_00', 'speaker': 'Jim'}],
            version='1.1.0',
        )
        before = transcript_path(self.ep.id, 'words').read_bytes()
        out = self._run('--apply', '--skip-recompute')
        self.assertIn('skipped', out)
        self.assertEqual(transcript_path(self.ep.id, 'words').read_bytes(), before)

    def test_whisper_raw_recovers_id_and_folds_names(self):
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 2, 'body': 'Hello', 'speaker': 'SPEAKER_00'}],
        )
        self._write_raw(_MOCK_ASR_JSON)  # two SPEAKER_XX segments
        self._approve({'SPEAKER_00': 'Jim'})
        self._run('--apply', '--skip-recompute')
        doc = self._read_words()
        self.assertEqual(doc['version'], '1.1.0')
        # Pristine speaker_id recovered from the raw dump; name resolved via fold.
        self.assertEqual(doc['segments'][0]['speaker_id'], 'SPEAKER_00')
        self.assertEqual(doc['segments'][0]['speaker'], 'Jim')
        # The unmapped second speaker keeps its raw label.
        self.assertEqual(doc['segments'][1]['speaker_id'], 'SPEAKER_01')
        self.assertEqual(doc['segments'][1]['speaker'], 'SPEAKER_01')

    def test_preserves_existing_header_metadata(self):
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 2, 'body': 'Hello', 'speaker': 'SPEAKER_00'}],
            header={'language': 'es', 'model': 'large-v3', 'transcribed_at': '2025-05-05T05:05:05',
                    'title': 'Recovery Title', 'audio_url': 'https://x/a.mp3'},
        )
        self._write_raw(_MOCK_ASR_JSON)
        self._run('--apply', '--skip-recompute')
        doc = self._read_words()
        self.assertEqual(doc['model'], 'large-v3')
        self.assertEqual(doc['transcribed_at'], '2025-05-05T05:05:05')
        self.assertEqual(doc['language'], 'es')
        self.assertEqual(doc['title'], 'Recovery Title')

    def test_rolled_back_edit_reconciles_and_logs_name_change(self):
        # File still bakes in 'Jim' (old broken rollback), but the edit is
        # ROLLED_BACK so the fold drops it → name reverts to the raw label.
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 2, 'body': 'Hello', 'speaker': 'Jim'}],
        )
        self._write_raw(_MOCK_ASR_JSON)
        edit = self._approve({'SPEAKER_00': 'Jim'})
        edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        edit.save()
        out = self._run('--apply', '--skip-recompute')
        self.assertIn('names', out)
        doc = self._read_words()
        self.assertEqual(doc['segments'][0]['speaker'], 'SPEAKER_00')

    def test_fallback_seeds_id_from_speaker_when_raw_missing(self):
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 2, 'body': 'Hello', 'speaker': 'Aron'}],
        )
        out = self._run('--apply', '--skip-recompute')
        self.assertIn('seeded', out)
        doc = self._read_words()
        self.assertEqual(doc['version'], '1.1.0')
        self.assertEqual(doc['segments'][0]['speaker_id'], 'Aron')
        self.assertEqual(doc['segments'][0]['speaker'], 'Aron')

    def test_preview_writes_nothing(self):
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 2, 'body': 'Hello', 'speaker': 'SPEAKER_00'}],
        )
        self._write_raw(_MOCK_ASR_JSON)
        before = transcript_path(self.ep.id, 'words').read_bytes()
        out = self._run('--skip-recompute')  # no --apply
        self.assertIn('would', out)
        self.assertEqual(transcript_path(self.ep.id, 'words').read_bytes(), before)

    def test_r2_resident_hash_checks_and_bumps_version(self):
        # version>=1 + R2 enabled → write via the §4 hash-check path, bump version.
        self.transcript.version = 3
        self.transcript.save()
        legacy = json.dumps({
            'version': '1.0.0', 'language': 'en', 'model': 'm', 'transcribed_at': 't',
            'segments': [{'startTime': 0, 'endTime': 1, 'body': 'Hi', 'speaker': 'Aron'}],
        }).encode('utf-8')
        puts = []
        with override_settings(R2_MEDIA_ENABLED=True), \
             mock.patch('pod_manager.management.commands.backfill_speaker_ids.read_transcript_bytes',
                        return_value=legacy), \
             mock.patch('pod_manager.services.r2_storage.media_object_etag', return_value=None), \
             mock.patch('pod_manager.services.r2_storage.put_media_object',
                        side_effect=lambda k, c, ct: puts.append(k)):
            self._run('--apply', '--skip-recompute')
        self.transcript.refresh_from_db()
        self.assertEqual(self.transcript.version, 4)  # bumped (all 5 formats "changed")
        self.assertEqual(len(puts), 5)

    def test_recompute_sets_edits_speakers_idempotently(self):
        from datetime import timedelta
        # A migrated .words so the per-transcript loop cleanly skips this episode.
        self._write_legacy_words(
            [{'startTime': 0, 'endTime': 1, 'body': 'Hi', 'speaker_id': 'SPEAKER_00', 'speaker': 'A.Ron'}],
            version='1.1.0',
        )
        m = NetworkMembership.objects.create(user=self.user, network=self.net,
                                             edits_speakers=99, trust_score=42)
        t0 = timezone.now()
        self._approve({'SPEAKER_00': 'Jim'}, resolved_at=t0)                       # +1 naming
        self._approve({'SPEAKER_00': 'A.Ron'}, resolved_at=t0 + timedelta(minutes=1))  # +1 correction
        # A rolled-back edit must not count.
        rb = self._approve({'SPEAKER_01': 'Bob'}, resolved_at=t0 + timedelta(minutes=2))
        rb.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        rb.save()

        self._run('--apply', scope=f'--network={self.net.slug}')
        m.refresh_from_db()
        self.assertEqual(m.edits_speakers, 2)
        self.assertEqual(m.trust_score, 42)  # trust never re-credited

        # Idempotent: a second run leaves it at 2.
        self._run('--apply', scope=f'--network={self.net.slug}')
        m.refresh_from_db()
        self.assertEqual(m.edits_speakers, 2)


# ── 9. serve_transcript view ─────────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ServeTranscriptTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # Keep MEDIA_ROOT pointing at tmp for the entire test, including when
        # the view runs. enable()/disable() span setUp → tearDown.
        self._media = override_settings(MEDIA_ROOT=self.tmp)
        self._media.enable()
        self.client = Client()
        _, _, self.ep = _make_fixture()
        p = transcript_path(self.ep.id, 'vtt')
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b'WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello')
        self.transcript = Transcript.objects.create(
            episode=self.ep,
            status=Transcript.Status.COMPLETED,
            vtt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.vtt',
            source_audio_url='https://cdn.example.com/my-episode-title.mp3',
        )

    def tearDown(self):
        self._media.disable()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _get(self, ext='vtt', **kwargs):
        url = reverse('serve_transcript', kwargs={'episode_id': self.ep.id, 'ext': ext})
        return self.client.get(url, **kwargs)

    def test_serves_file_with_correct_content_type(self):
        resp = self._get()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'text/vtt')

    def test_revalidating_cache_and_cors_headers(self):
        # Requeued transcriptions overwrite the same URL, so the file must be
        # revalidated (no-cache) rather than cached as immutable — otherwise a
        # re-transcribed episode shows the stale text until a hard refresh.
        resp = self._get()
        self.assertIn('no-cache', resp['Cache-Control'])
        self.assertNotIn('immutable', resp['Cache-Control'])
        self.assertEqual(resp['Access-Control-Allow-Origin'], '*')

    def test_etag_present(self):
        resp = self._get()
        self.assertIn('ETag', resp)

    def test_304_on_etag_match(self):
        r1 = self._get()
        etag = r1['ETag']
        r2 = self._get(HTTP_IF_NONE_MATCH=etag)
        self.assertEqual(r2.status_code, 304)

    def test_404_for_nonexistent_episode(self):
        url = reverse('serve_transcript', kwargs={'episode_id': 99999, 'ext': 'vtt'})
        with override_settings(MEDIA_ROOT=self.tmp):
            resp = self.client.get(url)
        self.assertEqual(resp.status_code, 404)

    def test_content_disposition_uses_subscriber_audio_stem(self):
        # Names the transcript after the episode's audio, matching the MP3 download.
        resp = self._get()
        disposition = resp.get('Content-Disposition', '')
        self.assertIn('sub.vtt', disposition)

    def test_content_disposition_falls_back_to_public_for_gdrive(self):
        # Google Drive /uc links have no extension; fall back to the public URL
        # so the download isn't named "uc.vtt".
        self.ep.audio_url_subscriber = 'https://docs.google.com/uc?export=download&id=ABC'
        self.ep.audio_url_public = 'https://cdn.example.com/real-episode-title.mp3'
        self.ep.save(update_fields=['audio_url_subscriber', 'audio_url_public'])
        resp = self._get()
        disposition = resp.get('Content-Disposition', '')
        self.assertIn('real-episode-title.vtt', disposition)
        self.assertNotIn('uc.vtt', disposition)


# ── 10. backfill_transcripts_api ─────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class BackfillTranscriptsApiTests(TestCase):

    def setUp(self):
        cache.clear()
        self.staff   = User.objects.create_user(username='staff_bf',   is_staff=True)
        self.regular = User.objects.create_user(username='regular_bf', is_staff=False)
        self.net, self.pod, self.ep = _make_fixture()
        self.url = reverse('backfill_transcripts_api')

    def _post(self, user, body=None):
        self.client.force_login(user)
        return self.client.post(
            self.url,
            data=json.dumps(body or {}),
            content_type='application/json',
        )

    def test_non_staff_gets_403(self):
        resp = self._post(self.regular)
        self.assertEqual(resp.status_code, 403)

    def test_invalid_json_gets_400(self):
        self.client.force_login(self.staff)
        resp = self.client.post(self.url, data='!!notjson', content_type='application/json')
        self.assertEqual(resp.status_code, 400)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_queues_episode_with_no_transcript(self, mock_task):
        resp = self._post(self.staff, {'stagger': 0})
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content)
        self.assertEqual(data['queued'], 1)
        mock_task.apply_async.assert_called_once()

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_skips_completed_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)
        resp = self._post(self.staff)
        self.assertEqual(json.loads(resp.content)['queued'], 0)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_skips_pending_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.PENDING)
        resp = self._post(self.staff)
        self.assertEqual(json.loads(resp.content)['queued'], 0)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_requeues_failed_transcript(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.FAILED)
        resp = self._post(self.staff, {'stagger': 0})
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_podcast_slug_filter(self, mock_task):
        pod2 = Podcast.objects.create(network=self.net, title='P2', slug='p2-tx')
        Episode.objects.create(
            podcast=pod2, title='Ep2', pub_date=timezone.now(),
            raw_description='x', audio_url_subscriber='https://cdn.example.com/ep2.mp3',
        )
        resp = self._post(self.staff, {'podcast_slug': self.pod.slug, 'stagger': 0})
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    @mock.patch('pod_manager.services.transcription.run_transcription')
    def test_ide_path_calls_run_transcription_synchronously(self, mock_run):
        with override_settings(**{**TRANSCRIPTION_SETTINGS, 'IS_IDE': True}):
            resp = self._post(self.staff, {'stagger': 0})
        mock_run.assert_called_once_with(self.ep.pk)
        self.assertEqual(json.loads(resp.content)['queued'], 1)

    def test_unknown_podcast_slug_returns_404(self):
        resp = self._post(self.staff, {'podcast_slug': 'no-such-show'})
        self.assertEqual(resp.status_code, 404)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_transcription_kwargs_forwarded(self, mock_task):
        self._post(self.staff, {'stagger': 0, 'model': 'large', 'language': 'fr', 'num_speakers': 3})
        call_kwargs = mock_task.apply_async.call_args[1].get('kwargs', {})
        self.assertEqual(call_kwargs.get('model'), 'large')
        self.assertEqual(call_kwargs.get('language'), 'fr')
        self.assertEqual(call_kwargs.get('num_speakers'), 3)


# ── 11. retranscribe_episode_api ─────────────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class RetranscribeEpisodeApiTests(TestCase):

    def setUp(self):
        cache.clear()
        self.net, self.pod, self.ep = _make_fixture()
        self.owner = User.objects.create_user(username='owner_rt')
        self.other = User.objects.create_user(username='other_rt')
        self.net.owners.add(self.owner)

    def _post(self, user, ep_id=None, body=None):
        self.client.force_login(user)
        url = reverse('retranscribe_episode_api', kwargs={'episode_id': ep_id or self.ep.id})
        return self.client.post(url, data=json.dumps(body or {}), content_type='application/json')

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_owner_gets_200_and_queued_status(self, mock_task):
        resp = self._post(self.owner)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['status'], 'queued')

    def test_non_owner_gets_403(self):
        resp = self._post(self.other)
        self.assertEqual(resp.status_code, 403)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_resets_existing_transcript_to_pending(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)
        self._post(self.owner)
        t = Transcript.objects.get(episode=self.ep)
        self.assertEqual(t.status, Transcript.Status.PENDING)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_clears_error_message(self, mock_task):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.FAILED,
                                  error_message='old error')
        self._post(self.owner)
        t = Transcript.objects.get(episode=self.ep)
        self.assertIsNone(t.error_message)

    def test_episode_without_subscriber_audio_returns_400(self):
        net2, _, ep_pub = _make_fixture(subscriber=False)
        net2.owners.add(self.owner)
        resp = self._post(self.owner, ep_id=ep_pub.id)
        self.assertEqual(resp.status_code, 400)

    @mock.patch('pod_manager.services.transcription.run_transcription')
    def test_ide_path_calls_run_transcription_synchronously(self, mock_run):
        with override_settings(**{**TRANSCRIPTION_SETTINGS, 'IS_IDE': True}):
            resp = self._post(self.owner)
        mock_run.assert_called_once_with(self.ep.pk)

    @mock.patch('pod_manager.tasks.transcribe_episode')
    def test_transcription_kwargs_forwarded(self, mock_task):
        self._post(self.owner, body={'model': 'large', 'language': 'es'})
        call_kwargs = mock_task.apply_async.call_args[1].get('kwargs', {})
        self.assertEqual(call_kwargs.get('model'), 'large')
        self.assertEqual(call_kwargs.get('language'), 'es')


# ── 12. RSS feed podcast:transcript tags ─────────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class FeedTranscriptTagTests(TestCase):

    def setUp(self):
        self.net, self.pod, self.ep = _make_fixture()

    def _render(self, *, completed_transcript=False, has_access=False):
        from pod_manager.views.feeds import RSSFeedBuilder
        feed_type = 'private' if has_access else 'public'
        builder = RSSFeedBuilder(
            base_url='http://testserver',
            title=self.pod.title,
            description='Test feed',
            image_url='http://testserver/img.jpg',
            network=self.net,
            feed_type=feed_type,
        )
        builder.add_episode(self.ep, has_access=has_access)
        if completed_transcript:
            Transcript.objects.create(
                episode=self.ep,
                status=Transcript.Status.COMPLETED,
                vtt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.vtt',
                json_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.json',
                srt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.srt',
                html_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.html',
            )
        return builder.render()

    def test_completed_transcript_adds_podcast_transcript_elements(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('podcast:transcript', xml)

    def test_all_four_mime_types_present(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('text/vtt', xml)
        self.assertIn('application/json', xml)
        self.assertIn('application/x-subrip', xml)
        self.assertIn('text/html', xml)

    def test_no_transcript_means_no_tags(self):
        xml = self._render(completed_transcript=False)
        self.assertNotIn('podcast:transcript', xml)

    def test_podcast_namespace_always_declared(self):
        xml = self._render(completed_transcript=True)
        self.assertIn('podcastindex.org/namespace/1.0', xml)

    def test_transcript_url_contains_episode_id_and_ext(self):
        xml = self._render(completed_transcript=True)
        self.assertIn(f'/transcripts/{self.ep.id}.vtt', xml)

    def test_pending_transcript_does_not_generate_tags(self):
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.PENDING)
        xml = self._render(completed_transcript=False)
        self.assertNotIn('podcast:transcript', xml)

    # ── Section C: flag gating + auth placeholder ────────────────────────────

    def test_flag_off_public_variant_suppresses_tags(self):
        self.pod.allow_public_transcripts = False
        self.pod.save()
        xml = self._render(completed_transcript=True, has_access=False)
        self.assertNotIn('podcast:transcript', xml)

    def test_flag_off_private_variant_still_emits_tags(self):
        """An entitled (private-variant) listener gets the tags regardless of
        the public flag."""
        self.pod.allow_public_transcripts = False
        self.pod.save()
        xml = self._render(completed_transcript=True, has_access=True)
        self.assertIn('podcast:transcript', xml)

    def test_flag_on_public_variant_emits_tags_without_auth(self):
        # flag defaults True
        xml = self._render(completed_transcript=True, has_access=False)
        self.assertIn('podcast:transcript', xml)
        # no auth placeholder anywhere in the transcript block
        self.assertNotIn('auth=__VECTO_AUTH_TOKEN__', xml.split('podcast:transcript', 1)[1])

    def test_private_variant_carries_auth_placeholder(self):
        # lxml serialises the leading '&' of the transcript URL's auth as '&amp;'
        xml = self._render(completed_transcript=True, has_access=True)
        self.assertIn('&amp;auth=__VECTO_AUTH_TOKEN__', xml)

    def test_transcript_tag_points_at_our_endpoint_not_cdn(self):
        xml = self._render(completed_transcript=True, has_access=False)
        self.assertIn(f'/transcripts/{self.ep.id}.vtt', xml)


# ── 13. apply_approved_edit — speaker_mappings branch ────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class ApplyApprovedEditSpeakerMappingsTests(TestCase):
    """Speaker-label edits must not lock episode metadata.
    is_metadata_locked guards feed-ingested fields against being overwritten
    by the ingestor on re-import; speaker names are transcript-only and have
    no corresponding feed field, so they must not trigger the lock."""

    def setUp(self):
        _, _, self.ep = _make_fixture()
        Transcript.objects.create(episode=self.ep, status=Transcript.Status.COMPLETED)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_calls_apply_speaker_labels(self, mock_apply):
        mappings = {'SPEAKER_00': 'Jim', 'SPEAKER_01': 'A.Ron'}
        apply_approved_edit(self.ep, {'speaker_mappings': mappings})
        # Replay is recompute-from-base: no mappings argument — the chain is folded
        # from the episode's APPROVED edits inside apply_speaker_labels itself.
        mock_apply.assert_called_once_with(self.ep.id)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_only_edit_does_not_set_metadata_locked(self, _):
        """The early return in apply_approved_edit means is_metadata_locked
        is never touched for speaker-only edits."""
        self.ep.is_metadata_locked = False
        self.ep.save()
        apply_approved_edit(self.ep, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        self.ep.refresh_from_db()
        self.assertFalse(self.ep.is_metadata_locked)

    @mock.patch('pod_manager.services.transcription.apply_speaker_labels')
    def test_speaker_only_edit_does_not_alter_title(self, _):
        original_title = self.ep.title
        apply_approved_edit(self.ep, {'speaker_mappings': {'SPEAKER_00': 'Jim'}})
        self.ep.refresh_from_db()
        self.assertEqual(self.ep.title, original_title)

    def test_normal_field_edit_does_set_metadata_locked(self):
        self.ep.is_metadata_locked = False
        self.ep.save()
        apply_approved_edit(self.ep, {'title': 'New Title'})
        self.ep.refresh_from_db()
        self.assertTrue(self.ep.is_metadata_locked)


# ── 14. auto_delete_transcript_files signal ──────────────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class AutoDeleteTranscriptFilesTests(TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        _, _, self.ep = _make_fixture()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _create_transcript_with_file(self):
        with override_settings(MEDIA_ROOT=self.tmp):
            p = transcript_path(self.ep.id, 'vtt')
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b'WEBVTT\n')
            rel = str(p.relative_to(self.tmp))
        return Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, vtt_file=rel,
        ), p

    def test_file_deleted_on_transcript_delete(self):
        t, p = self._create_transcript_with_file()
        with override_settings(MEDIA_ROOT=self.tmp):
            t.delete()
        self.assertFalse(p.exists())

    def test_empty_bucket_directory_removed(self):
        t, p = self._create_transcript_with_file()
        bucket_dir = p.parent
        with override_settings(MEDIA_ROOT=self.tmp):
            t.delete()
        self.assertFalse(bucket_dir.exists())


# ── 15. Creator settings — whisper field persistence ─────────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class HandleUpdateNetworkWhisperTests(TestCase):

    def setUp(self):
        self.factory = RequestFactory()
        self.user    = User.objects.create_user(username='owner_hun')
        self.net     = Network.objects.create(name='WN', slug='wn-tx')

    def _call(self, post_data):
        from django.contrib.messages.storage.fallback import FallbackStorage
        from pod_manager.views.creator.actions import handle_update_network
        req = self.factory.post('/creator/', data={
            'theme_config': '{}',
            'patreon_campaign_id': '', 'website_url': '', 'default_image_url': '',
            'ignored_title_tags': '', 'description_cut_triggers': '',
            'footer_public': '', 'footer_private': '',
            **post_data,
        })
        req.user = self.user
        req.session = {}  # FallbackStorage only needs dict-like session
        req._messages = FallbackStorage(req)
        with mock.patch('pod_manager.views.creator.actions.task_rebuild_podcast_fragments'):
            handle_update_network(req, self.net)
        self.net.refresh_from_db()

    def test_saves_whisper_model(self):
        self._call({'whisper_model': 'large', 'whisper_language': 'en'})
        self.assertEqual(self.net.whisper_model, 'large')

    def test_saves_whisper_language(self):
        self._call({'whisper_model': 'medium.en', 'whisper_language': 'es'})
        self.assertEqual(self.net.whisper_language, 'es')

    def test_blank_model_clears_to_use_system_default(self):
        # Blank = network opts out of pinning a model, so resolution falls through
        # to WHISPER_DEFAULT_MODEL / WHISPER_MODEL at run time.
        self.net.whisper_model = 'large'
        self.net.save()
        self._call({'whisper_model': '', 'whisper_language': 'en'})
        self.assertEqual(self.net.whisper_model, '')

    def test_blank_language_falls_back_to_en(self):
        self._call({'whisper_model': 'medium.en', 'whisper_language': ''})
        self.assertEqual(self.net.whisper_language, 'en')

    def test_saves_speaker_counts(self):
        self._call({
            'whisper_model': 'medium.en', 'whisper_language': 'en',
            'whisper_min_speakers': '1',
            'whisper_num_speakers': '3',
            'whisper_max_speakers': '6',
        })
        self.assertEqual(self.net.whisper_min_speakers, 1)
        self.assertEqual(self.net.whisper_num_speakers, 3)
        self.assertEqual(self.net.whisper_max_speakers, 6)

    def test_saves_initial_prompt(self):
        self._call({
            'whisper_model': 'medium.en', 'whisper_language': 'en',
            'whisper_initial_prompt': 'Hosts: Jim and A.Ron.',
        })
        self.assertEqual(self.net.whisper_initial_prompt, 'Hosts: Jim and A.Ron.')


@override_settings(**TRANSCRIPTION_SETTINGS)
class HandleUpdateShowWhisperTests(TestCase):

    def setUp(self):
        self.factory = RequestFactory()
        self.user    = User.objects.create_user(username='owner_hus')
        self.net     = Network.objects.create(name='SWN', slug='swn-tx')
        self.pod     = Podcast.objects.create(network=self.net, title='SP2', slug='sp2-tx')

    def _call(self, post_data):
        from django.contrib.messages.storage.fallback import FallbackStorage
        from pod_manager.views.creator.actions import handle_update_show
        req = self.factory.post('/creator/', data={
            'show_id': str(self.pod.id),
            'public_feed_url': '', 'subscriber_feed_url': '',
            'show_footer_public': '', 'show_footer_private': '',
            **post_data,
        })
        req.user = self.user
        req.session = {}  # FallbackStorage only needs dict-like session
        req._messages = FallbackStorage(req)
        with mock.patch('pod_manager.views.creator.actions.task_rebuild_podcast_fragments'):
            handle_update_show(req, self.net)
        self.pod.refresh_from_db()

    def test_sets_whisper_model_override(self):
        self._call({'whisper_model': 'small'})
        self.assertEqual(self.pod.whisper_model, 'small')

    def test_blank_model_stores_none_to_inherit_from_network(self):
        self.pod.whisper_model = 'large'
        self.pod.save()
        self._call({'whisper_model': ''})
        self.assertIsNone(self.pod.whisper_model)

    def test_sets_speaker_overrides(self):
        self._call({'whisper_min_speakers': '1', 'whisper_num_speakers': '2', 'whisper_max_speakers': '4'})
        self.assertEqual(self.pod.whisper_min_speakers, 1)
        self.assertEqual(self.pod.whisper_num_speakers, 2)
        self.assertEqual(self.pod.whisper_max_speakers, 4)

    def test_blank_speaker_count_stores_none_to_inherit(self):
        self.pod.whisper_num_speakers = 4
        self.pod.save()
        self._call({'whisper_num_speakers': ''})
        self.assertIsNone(self.pod.whisper_num_speakers)

    def test_force_r2_serve_checkbox_enables(self):
        self._call({'force_r2_serve': 'on'})
        self.assertTrue(self.pod.force_r2_serve)

    def test_force_r2_serve_absent_checkbox_disables(self):
        self.pod.force_r2_serve = True
        self.pod.save()
        self._call({})  # checkbox not submitted == unchecked
        self.assertFalse(self.pod.force_r2_serve)

    def test_is_low_priority_checkbox_enables(self):
        self._call({'is_low_priority': 'on'})
        self.assertTrue(self.pod.is_low_priority)

    def test_is_low_priority_absent_checkbox_disables(self):
        self.pod.is_low_priority = True
        self.pod.save()
        self._call({})  # checkbox not submitted == unchecked
        self.assertFalse(self.pod.is_low_priority)


# ---------------------------------------------------------------------------
# Episode cross-publishing ("also appears in" other podcasts' feeds)
# ---------------------------------------------------------------------------

@override_settings(CACHES=TEST_CACHES)
class CrossPublishFeedTests(TestCase):
    """Cross-published episodes appear in the target podcast's feeds exactly
    once, premium gating follows the parent unless the link overrides to the
    target's tier, and mixes never duplicate an episode."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='n')
        self.tier = PatreonTier.objects.create(network=self.network, name='Premium', minimum_cents=500)
        self.parent = Podcast.objects.create(network=self.network, title='Parent', slug='parent', required_tier=self.tier)
        self.target = Podcast.objects.create(network=self.network, title='Target', slug='target')
        self.ep = Episode.objects.create(
            podcast=self.parent, title='Shared Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            guid_public='shared-guid-123',
        )
        self.link = EpisodeCrossPublication.objects.create(episode=self.ep, podcast=self.target)

    def _listener(self):
        user = User.objects.create_user(username='listener')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)
        return user, profile

    def _public_feed(self, slug):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        return views.generate_public_feed(req, podcast_slug=slug).content.decode('utf-8')

    def _custom_feed(self, profile, slug):
        req = self.factory.get('/feed/', {'auth': str(profile.feed_token), 'show': slug})
        req.network = self.network
        return views.generate_custom_feed(req).content.decode('utf-8')

    def test_cross_published_episode_appears_once_in_target_public_feed(self):
        xml = self._public_feed('target')
        self.assertEqual(xml.count('shared-guid-123'), 1)

    def test_cross_published_episode_still_in_parent_public_feed(self):
        xml = self._public_feed('parent')
        self.assertEqual(xml.count('shared-guid-123'), 1)

    def test_premium_only_episode_absent_from_target_public_feed(self):
        premium_ep = Episode.objects.create(
            podcast=self.parent, title='Premium Only', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
            guid_public='premium-guid-456',
        )
        EpisodeCrossPublication.objects.create(episode=premium_ep, podcast=self.target)
        xml = self._public_feed('target')
        self.assertNotIn('premium-guid-456', xml)

    def test_custom_feed_inherit_keeps_parent_gating(self):
        """A target-only listener (no pledge) must not see a premium-only
        cross-published episode while the link inherits the parent's tier."""
        _, profile = self._listener()
        premium_ep = Episode.objects.create(
            podcast=self.parent, title='Premium Only', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
            guid_public='premium-guid-456',
        )
        link = EpisodeCrossPublication.objects.create(episode=premium_ep, podcast=self.target)

        xml = self._custom_feed(profile, 'target')
        self.assertNotIn('premium-guid-456', xml)

        link.access_mode = EpisodeCrossPublication.AccessMode.TARGET
        link.save()
        xml = self._custom_feed(profile, 'target')
        self.assertEqual(xml.count('premium-guid-456'), 1)

    def test_user_mix_with_parent_and_target_yields_single_item(self):
        user, profile = self._listener()
        mix = UserMix.objects.create(user=user, network=self.network, name='Mix', is_active=True)
        mix.selected_podcasts.add(self.parent, self.target)
        req = self.factory.get(f'/feed/mix/{mix.unique_id}', {'auth': str(profile.feed_token)})
        xml = views.generate_mix_feed(req, unique_id=mix.unique_id).content.decode('utf-8')
        self.assertEqual(xml.count('shared-guid-123'), 1)
        # Parent appearance wins: title prefix uses the parent podcast.
        self.assertIn('[Parent]', xml)

    def test_user_mix_with_only_target_includes_cross_published_episode(self):
        user, profile = self._listener()
        mix = UserMix.objects.create(user=user, network=self.network, name='Mix', is_active=True)
        mix.selected_podcasts.add(self.target)
        req = self.factory.get(f'/feed/mix/{mix.unique_id}', {'auth': str(profile.feed_token)})
        xml = views.generate_mix_feed(req, unique_id=mix.unique_id).content.decode('utf-8')
        self.assertEqual(xml.count('shared-guid-123'), 1)

    def test_network_mix_dedupes_cross_published_episode(self):
        mix = NetworkMix.objects.create(network=self.network, name='Net Mix', slug='netmix')
        mix.selected_podcasts.add(self.parent, self.target)
        req = _make_tenant_request(self.factory, self.network, path='/feed/n/mix/netmix/')
        xml = views.generate_network_mix_feed(req, network_slug='n', mix_slug='netmix').content.decode('utf-8')
        self.assertEqual(xml.count('shared-guid-123'), 1)


# ── Section C: feed-level transcript emission end-to-end ─────────────────────

@override_settings(**TRANSCRIPTION_SETTINGS)
class FeedTranscriptEmissionTests(TestCase):
    """Section C, view level: the podcast:transcript tag emission is gated by
    the origin podcast's allow_public_transcripts flag, entitled feeds carry a
    real ?v=N&auth=<token>, and NO tokenless render path leaks the raw
    __VECTO_AUTH_TOKEN__ placeholder (public feed AND the network-mix
    session-user path)."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='n')
        self.pod = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            audio_url_subscriber='https://cdn.example.com/sub.mp3',
            guid_public='ep-guid-1',
        )
        Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED,
            vtt_file=f'transcriptions/{self.ep.id // 1000}/{self.ep.id}.vtt',
        )

    def _public_feed(self):
        req = _make_tenant_request(self.factory, self.network, path='/feed/')
        return views.generate_public_feed(req, podcast_slug='show').content.decode('utf-8')

    def _custom_feed(self, profile):
        req = self.factory.get('/feed/', {'auth': str(profile.feed_token), 'show': 'show'})
        req.network = self.network
        return views.generate_custom_feed(req).content.decode('utf-8')

    def _owner(self):
        user = User.objects.create_user(username='owner')
        profile = PatronProfile.objects.create(user=user, patreon_id=None)
        self.network.owners.add(user)
        NetworkMembership.objects.create(user=user, network=self.network)
        return user, profile

    # -- public feed gating ---------------------------------------------------

    def test_public_feed_flag_on_has_tags_at_our_endpoint(self):
        xml = self._public_feed()
        self.assertIn('podcast:transcript', xml)
        self.assertIn(f'/transcripts/{self.ep.id}.vtt', xml)
        # never a CDN object URL, only our on-platform endpoint
        self.assertNotIn('transcriptions/', xml)

    def test_public_feed_flag_off_no_tags(self):
        self.pod.allow_public_transcripts = False
        self.pod.save()
        cache.clear()
        xml = self._public_feed()
        self.assertNotIn('podcast:transcript', xml)

    def test_public_feed_no_placeholder_literal(self):
        xml = self._public_feed()
        self.assertNotIn('__VECTO_AUTH_TOKEN__', xml)
        self.assertNotIn('auth=', xml)  # tokenless: audio + transcript both stripped

    # -- entitled custom feed on a flag-off podcast ---------------------------

    def test_custom_feed_flag_off_entitled_carries_real_auth(self):
        self.pod.allow_public_transcripts = False
        self.pod.save()
        cache.clear()
        _, profile = self._owner()
        xml = self._custom_feed(profile)
        self.assertIn('podcast:transcript', xml)
        # tag present with the real feed token substituted (serialised '&amp;')
        self.assertIn(f'&amp;auth={profile.feed_token}', xml)
        self.assertNotIn('__VECTO_AUTH_TOKEN__', xml)

    def test_custom_feed_malformed_auth_404s(self):
        # feed_token is a UUIDField; a junk value must 404, not 500.
        req = self.factory.get('/feed/', {'auth': 'not-a-uuid', 'show': 'show'})
        req.network = self.network
        with self.assertRaises(Http404):
            views.generate_custom_feed(req)

    # -- network-mix session-user path (no feed_token) ------------------------

    def test_network_mix_session_user_strips_transcript_auth(self):
        """generate_network_mix_feed can serve PRIVATE fragments to a session
        user with NO feed token; without the &amp;auth strip the raw
        placeholder would leak into that XML."""
        self.pod.allow_public_transcripts = False
        self.pod.save()
        cache.clear()
        user, _ = self._owner()
        mix = NetworkMix.objects.create(network=self.network, name='Net Mix', slug='netmix')
        mix.selected_podcasts.add(self.pod)
        req = _make_tenant_request(self.factory, self.network,
                                   path='/feed/n/mix/netmix/', user=user)
        xml = views.generate_network_mix_feed(req, network_slug='n', mix_slug='netmix').content.decode('utf-8')
        # private fragment served (entitled owner) => tag present ...
        self.assertIn('podcast:transcript', xml)
        # ... but the placeholder is stripped, not left raw
        self.assertNotIn('__VECTO_AUTH_TOKEN__', xml)
        self.assertNotIn('auth=', xml)


@override_settings(CACHES=TEST_CACHES)
class CrossPublishPlayEpisodeTests(TestCase):
    """play_episode honours the per-link tier override when serving the
    subscriber audio."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Net', slug='n')
        self.tier = PatreonTier.objects.create(network=self.network, name='Premium', minimum_cents=500)
        self.parent = Podcast.objects.create(network=self.network, title='Parent', slug='parent', required_tier=self.tier)
        self.target = Podcast.objects.create(network=self.network, title='Target', slug='target')
        self.ep = Episode.objects.create(
            podcast=self.parent, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        user = User.objects.create_user(username='listener')
        self.profile = PatronProfile.objects.create(user=user, patreon_id=None)
        NetworkMembership.objects.create(user=user, network=self.network)
        self.link = EpisodeCrossPublication.objects.create(episode=self.ep, podcast=self.target)

    def _play(self):
        req = self.factory.get(f'/play/{self.ep.id}.mp3', {'auth': str(self.profile.feed_token)})
        req.network = self.network
        return views.play_episode(req, episode_id=self.ep.id)

    def test_inherit_mode_serves_public_audio_to_unentitled_listener(self):
        resp = self._play()
        self.assertEqual(resp['Location'], 'https://cdn.example.com/pub.mp3')

    def test_target_mode_grants_subscriber_audio_via_free_target(self):
        self.link.access_mode = EpisodeCrossPublication.AccessMode.TARGET
        self.link.save()
        resp = self._play()
        self.assertEqual(resp['Location'], 'https://cdn.example.com/priv.mp3')


@override_settings(CACHES=TEST_CACHES)
class CrossPublishActionTests(TestCase):
    """cross_publish_episodes bulk handler and its interaction with
    move_episodes."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.network = Network.objects.create(name='Net', slug='n')
        self.network.owners.add(self.owner)
        self.parent = Podcast.objects.create(network=self.network, title='Parent', slug='parent')
        self.target = Podcast.objects.create(network=self.network, title='Target', slug='target')

    def _ep(self, podcast=None, **kwargs):
        defaults = dict(
            podcast=podcast or self.parent, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_bulk_cross_publish_creates_links(self):
        ep1, ep2 = self._ep(title='A'), self._ep(title='B')
        self._post({
            'action': 'cross_publish_episodes',
            'episode_ids': [ep1.id, ep2.id],
            'target_podcast_id': self.target.id,
        })
        linked = set(EpisodeCrossPublication.objects.filter(podcast=self.target)
                     .values_list('episode_id', flat=True))
        self.assertEqual(linked, {ep1.id, ep2.id})

    def test_bulk_cross_publish_skips_episodes_parented_to_target(self):
        native = self._ep(podcast=self.target, title='Native')
        outsider = self._ep(title='Outsider')
        self._post({
            'action': 'cross_publish_episodes',
            'episode_ids': [native.id, outsider.id],
            'target_podcast_id': self.target.id,
        })
        linked = set(EpisodeCrossPublication.objects.filter(podcast=self.target)
                     .values_list('episode_id', flat=True))
        self.assertEqual(linked, {outsider.id})

    def test_bulk_cross_publish_multiple_targets(self):
        third = Podcast.objects.create(network=self.network, title='Third', slug='third')
        ep = self._ep()
        self._post({
            'action': 'cross_publish_episodes',
            'episode_ids': [ep.id],
            'target_podcast_ids': [self.target.id, third.id],
        })
        linked = set(ep.cross_publications.values_list('podcast_id', flat=True))
        self.assertEqual(linked, {self.target.id, third.id})

    def test_bulk_cross_publish_is_idempotent(self):
        ep = self._ep()
        for _ in range(2):
            self._post({
                'action': 'cross_publish_episodes',
                'episode_ids': [ep.id],
                'target_podcast_id': self.target.id,
            })
        self.assertEqual(EpisodeCrossPublication.objects.filter(episode=ep, podcast=self.target).count(), 1)

    def test_move_into_target_drops_redundant_link(self):
        ep = self._ep()
        EpisodeCrossPublication.objects.create(episode=ep, podcast=self.target)
        self._post({
            'action': 'move_episodes',
            'episode_ids': [ep.id],
            'target_podcast_id': self.target.id,
            'new_podcast_title': '', 'new_podcast_slug': '', 'new_podcast_tier_id': '',
        })
        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.target.id)
        self.assertFalse(EpisodeCrossPublication.objects.filter(episode=ep, podcast=self.target).exists())


@override_settings(CACHES=TEST_CACHES)
class CrossPublishSuggestionTests(TestCase):
    """cross_publish_podcast_ids through the community edit-suggestion flow:
    submission sanitization, inbox approval, and rollback."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user(username='owner')
        self.submitter = User.objects.create_user(username='submitter')
        self.network = Network.objects.create(name='Net', slug='n', auto_approve_trust_threshold=999)
        self.network.owners.add(self.owner)
        self.other_network = Network.objects.create(name='Other', slug='other')
        self.parent = Podcast.objects.create(network=self.network, title='Parent', slug='parent')
        self.target = Podcast.objects.create(network=self.network, title='Target', slug='target')
        self.foreign = Podcast.objects.create(network=self.other_network, title='Foreign', slug='foreign')
        self.episode = Episode.objects.create(
            podcast=self.parent, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        self.membership = NetworkMembership.objects.create(
            user=self.submitter, network=self.network, trust_score=5,
        )

    def _submit(self, payload, user=None):
        from django.contrib.messages.storage.fallback import FallbackStorage
        from django.contrib.sessions.backends.cache import SessionStore
        req = self.factory.post(
            reverse('submit_episode_edit', args=[self.episode.id]),
            data={'payload': json.dumps(payload)},
        )
        req.user = user or self.submitter
        req.network = self.network
        req.session = SessionStore()
        req.session.create()
        setattr(req, '_messages', FallbackStorage(req))
        return views.submit_episode_edit(req, self.episode.id)

    def _post(self, data):
        req = _make_tenant_request(self.factory, self.network,
                                   method='post', path='/creator/',
                                   data=data, user=self.owner)
        return views.creator_settings(req)

    def test_submission_strips_parent_and_foreign_network_ids(self):
        # Cross-publish is owner/admin only (§8a); an owner keeps the key and the
        # sanitizer still strips parent + foreign-network ids.
        self._submit({
            'title': 'Ep', 'description': '<p>x</p>', 'tags': [], 'chapters': [],
            'cross_publish_podcast_ids': [self.target.id, self.parent.id, self.foreign.id],
        }, user=self.owner)
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertEqual(suggestion.suggested_data['cross_publish_podcast_ids'], [self.target.id])
        # Pending edit must not create links yet.
        self.assertFalse(self.episode.cross_publications.exists())

    def test_trusted_submission_syncs_links_immediately(self):
        self.network.auto_approve_trust_threshold = 0
        self.network.save()
        self._submit({
            'title': 'Ep', 'description': '<p>x</p>', 'tags': [], 'chapters': [],
            'cross_publish_podcast_ids': [self.target.id],
        }, user=self.owner)
        link = EpisodeCrossPublication.objects.get(episode=self.episode, podcast=self.target)
        self.assertEqual(link.added_by, self.owner)

    def test_non_owner_submission_drops_cross_publish(self):
        """§8a lockdown: a non-owner POST carrying cross_publish_podcast_ids has the
        key dropped server-side, even on the trusted auto-approve path."""
        self.network.auto_approve_trust_threshold = 0
        self.network.save()
        self._submit({
            'title': 'A Different Title', 'description': '<p>x</p>', 'tags': [], 'chapters': [],
            'cross_publish_podcast_ids': [self.target.id],
        })  # default user = self.submitter (not an owner)
        suggestion = EpisodeEditSuggestion.objects.get(episode=self.episode)
        self.assertNotIn('cross_publish_podcast_ids', suggestion.suggested_data)
        self.assertFalse(self.episode.cross_publications.exists())

    def _pending_edit(self, suggested_ids, original_ids=None):
        return EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.PENDING,
            original_data={
                'title': self.episode.title, 'description': 'x', 'tags': [], 'chapters': [],
                'cross_publish_podcast_ids': original_ids or [],
            },
            suggested_data={
                'title': self.episode.title, 'description': 'x', 'tags': [], 'chapters': [],
                'cross_publish_podcast_ids': suggested_ids,
            },
        )

    def test_inbox_approval_syncs_links_and_awards_point(self):
        edit = self._pending_edit([self.target.id])
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_cross_publish': 'on',
            'edited_cross_publish_ids': json.dumps([self.target.id]),
        })
        self.assertTrue(EpisodeCrossPublication.objects.filter(
            episode=self.episode, podcast=self.target).exists())
        edit.refresh_from_db()
        self.assertEqual(edit.status, 'approved')
        self.membership.refresh_from_db()
        # Cross-publish is no longer scored (owner/admin-only, not a contribution).
        self.assertEqual(self.membership.trust_score, 5)

    def test_rollback_restores_previous_link_set(self):
        edit = self._pending_edit([self.target.id])
        self._post({
            'action': 'approve_edit',
            'edit_id': edit.id,
            'approve_cross_publish': 'on',
            'edited_cross_publish_ids': json.dumps([self.target.id]),
        })
        self.assertTrue(self.episode.cross_publications.exists())

        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.assertFalse(self.episode.cross_publications.exists())

    def test_rollback_of_pre_feature_edit_leaves_links_alone(self):
        """Edits approved before this feature have no cross_publish snapshot
        in original_data — rolling them back must not wipe current links."""
        EpisodeCrossPublication.objects.create(episode=self.episode, podcast=self.target)
        edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.submitter,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': 'Old', 'description': 'x', 'tags': [], 'chapters': []},
            suggested_data={'title': 'Ep', 'description': 'x', 'tags': [], 'chapters': []},
            resolved_at=timezone.now(),
        )
        self._post({'action': 'rollback_single_edit', 'edit_id': edit.id})
        self.assertTrue(EpisodeCrossPublication.objects.filter(
            episode=self.episode, podcast=self.target).exists())


@override_settings(CACHES=TEST_CACHES)
class CrossPublishServiceTests(TestCase):
    """validate_cross_targets and sync_cross_publications unit behaviour."""

    def setUp(self):
        from pod_manager.services.cross_publish import sync_cross_publications, validate_cross_targets
        self.sync = sync_cross_publications
        self.validate = validate_cross_targets
        self.network = Network.objects.create(name='Net', slug='n')
        self.parent = Podcast.objects.create(network=self.network, title='Parent', slug='parent')
        self.t1 = Podcast.objects.create(network=self.network, title='T1', slug='t1')
        self.t2 = Podcast.objects.create(network=self.network, title='T2', slug='t2')
        self.ep = Episode.objects.create(
            podcast=self.parent, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )

    def test_validate_drops_garbage_parent_and_unknown_ids(self):
        targets = self.validate(self.ep, ['abc', None, self.parent.id, self.t1.id, 999999], self.network)
        self.assertEqual({t.id for t in targets}, {self.t1.id})

    def test_sync_adds_removes_and_updates_modes(self):
        added, removed = self.sync(self.ep, [self.t1, self.t2])
        self.assertEqual(set(added), {self.t1.id, self.t2.id})
        self.assertEqual(removed, [])

        added, removed = self.sync(
            self.ep, [self.t1],
            modes={self.t1.id: EpisodeCrossPublication.AccessMode.TARGET},
        )
        self.assertEqual(added, [])
        self.assertEqual(removed, [self.t2.id])
        link = EpisodeCrossPublication.objects.get(episode=self.ep, podcast=self.t1)
        self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.TARGET)

    def test_sync_ignores_invalid_mode_values(self):
        self.sync(self.ep, [self.t1], modes={self.t1.id: 'bogus'})
        link = EpisodeCrossPublication.objects.get(episode=self.ep, podcast=self.t1)
        self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.INHERIT)


@override_settings(CACHES=TEST_CACHES)
class AutoCrossPublishServiceTests(TestCase):
    """Feed-level auto cross-publish engine: apply/backfill/teardown/re-mode/
    reeval, the auto-blind manual sync (S4), and manual promotion (E2)."""

    def setUp(self):
        from pod_manager.services import cross_publish
        self.cp = cross_publish
        self.network = Network.objects.create(name='Net', slug='acp')
        self.service = Podcast.objects.create(network=self.network, title='Service', slug='acp-service')
        self.dest1 = Podcast.objects.create(network=self.network, title='Dest1', slug='acp-dest1')
        self.dest2 = Podcast.objects.create(network=self.network, title='Dest2', slug='acp-dest2')

    def _ep(self, podcast=None, **kwargs):
        defaults = dict(
            podcast=podcast or self.service, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        defaults.update(kwargs)
        return Episode.objects.create(**defaults)

    def _auto_link(self, ep, dest):
        return EpisodeCrossPublication.objects.create(episode=ep, podcast=dest, auto_created=True)

    def test_apply_creates_auto_links_with_feed_mode(self):
        self.service.auto_crosspublish_targets.set([self.dest1, self.dest2])
        self.service.auto_crosspublish_access_mode = EpisodeCrossPublication.AccessMode.TARGET
        self.service.save()
        ep = self._ep()
        created = self.cp.apply_auto_cross_publish(ep)
        self.assertEqual(set(created), {self.dest1.id, self.dest2.id})
        for link in ep.cross_publications.all():
            self.assertTrue(link.auto_created)
            self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.TARGET)

    def test_apply_is_idempotent(self):
        self.service.auto_crosspublish_targets.set([self.dest1])
        ep = self._ep()
        self.cp.apply_auto_cross_publish(ep)
        self.assertEqual(self.cp.apply_auto_cross_publish(ep), [])
        self.assertEqual(ep.cross_publications.count(), 1)

    def test_apply_never_flips_existing_manual_link(self):
        self.service.auto_crosspublish_targets.set([self.dest1])
        ep = self._ep()
        EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest1)
        self.cp.apply_auto_cross_publish(ep)
        link = ep.cross_publications.get(podcast=self.dest1)
        self.assertFalse(link.auto_created)

    def test_apply_noop_without_targets(self):
        ep = self._ep()
        self.assertEqual(self.cp.apply_auto_cross_publish(ep), [])
        self.assertFalse(ep.cross_publications.exists())

    def test_backfill_links_every_episode_as_auto(self):
        eps = [self._ep(title=f'Ep {i}') for i in range(3)]
        touched = self.cp.sync_feed_auto_targets(self.service, added_ids=[self.dest1.id])
        self.assertEqual(touched, {self.dest1.id})
        for ep in eps:
            self.assertTrue(ep.cross_publications.filter(podcast=self.dest1, auto_created=True).exists())

    def test_backfill_excludes_source_feed_itself(self):
        self._ep()
        touched = self.cp.sync_feed_auto_targets(self.service, added_ids=[self.service.id])
        self.assertEqual(touched, set())
        self.assertFalse(EpisodeCrossPublication.objects.exists())

    def test_teardown_removes_only_that_destinations_auto_links(self):
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        self._auto_link(ep, self.dest2)
        manual = EpisodeCrossPublication.objects.create(
            episode=self._ep(title='Ep manual'), podcast=self.dest1)
        touched = self.cp.sync_feed_auto_targets(self.service, removed_ids=[self.dest1.id])
        self.assertEqual(touched, {self.dest1.id})
        self.assertFalse(ep.cross_publications.filter(podcast=self.dest1).exists())
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest2, auto_created=True).exists())
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=manual.id).exists())

    def test_resync_access_mode_updates_auto_links_only(self):
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        manual = EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest2)
        self.service.auto_crosspublish_access_mode = EpisodeCrossPublication.AccessMode.TARGET
        self.service.save()
        touched = self.cp.resync_feed_auto_access_mode(self.service)
        self.assertEqual(touched, {self.dest1.id})
        self.assertEqual(
            ep.cross_publications.get(podcast=self.dest1).access_mode,
            EpisodeCrossPublication.AccessMode.TARGET)
        manual.refresh_from_db()
        self.assertEqual(manual.access_mode, EpisodeCrossPublication.AccessMode.INHERIT)

    def test_manual_sync_is_blind_to_auto_rows(self):
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        self.assertEqual(self.cp.current_target_ids(ep), [])
        added, removed = self.cp.sync_cross_publications(ep, [self.dest2])
        self.assertEqual((added, removed), ([self.dest2.id], []))
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest1, auto_created=True).exists())

    def test_manual_sync_promotes_submitted_auto_target(self):
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        added, _ = self.cp.sync_cross_publications(
            ep, [self.dest1],
            modes={self.dest1.id: EpisodeCrossPublication.AccessMode.TARGET})
        self.assertEqual(added, [self.dest1.id])
        link = ep.cross_publications.get(podcast=self.dest1)
        self.assertFalse(link.auto_created)
        self.assertEqual(link.access_mode, EpisodeCrossPublication.AccessMode.TARGET)

    def test_promoted_link_survives_feed_teardown(self):
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        self.cp.sync_cross_publications(ep, [self.dest1])
        self.cp.sync_feed_auto_targets(self.service, removed_ids=[self.dest1.id])
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest1).exists())

    def test_add_cross_publications_is_add_only(self):
        ep = self._ep()
        existing = EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest1)
        added = self.cp.add_cross_publications(ep, [self.dest2, self.service])
        self.assertEqual(added, [self.dest2.id])
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=existing.id).exists())
        self.assertFalse(ep.cross_publications.filter(podcast=self.service).exists())

    def test_validate_feed_cross_targets_excludes_self_and_foreign(self):
        other_network = Network.objects.create(name='Other', slug='acp-other')
        foreign = Podcast.objects.create(network=other_network, title='F', slug='acp-f')
        targets = self.cp.validate_feed_cross_targets(
            self.service, [self.service.id, self.dest1.id, foreign.id, 'junk'], self.network)
        self.assertEqual({t.id for t in targets}, {self.dest1.id})

    def test_move_reevaluates_auto_links_and_preserves_manual(self):
        new_parent = Podcast.objects.create(network=self.network, title='NewHome', slug='acp-home')
        new_parent.auto_crosspublish_targets.set([self.dest2])
        self.service.auto_crosspublish_targets.set([self.dest1])
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        dest3 = Podcast.objects.create(network=self.network, title='Dest3', slug='acp-dest3')
        manual = EpisodeCrossPublication.objects.create(episode=ep, podcast=dest3)

        move_episodes([ep.id], new_parent, base_url='http://n.test',
                      rebuild_fragments=False)

        self.assertFalse(ep.cross_publications.filter(podcast=self.dest1).exists())
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest2, auto_created=True).exists())
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=manual.id, auto_created=False).exists())

    def test_move_manual_collision_with_new_parent_target_stays_manual(self):
        new_parent = Podcast.objects.create(network=self.network, title='NewHome2', slug='acp-home2')
        new_parent.auto_crosspublish_targets.set([self.dest2])
        ep = self._ep()
        EpisodeCrossPublication.objects.create(episode=ep, podcast=self.dest2)
        move_episodes([ep.id], new_parent, base_url='http://n.test',
                      rebuild_fragments=False)
        link = ep.cross_publications.get(podcast=self.dest2)
        self.assertFalse(link.auto_created)

    def test_backfill_task_creates_links_and_rebuilds_shells(self):
        from pod_manager.tasks import task_apply_feed_auto_cross_publish
        ep = self._ep()
        with mock.patch('pod_manager.tasks.task_rebuild_podcast_shell') as shell:
            task_apply_feed_auto_cross_publish(self.service.id, [self.dest1.id], 'http://n.test')
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest1, auto_created=True).exists())
        shell.assert_called_once_with(self.dest1.id, 'http://n.test')

    def test_teardown_task_removes_links_and_rebuilds_shells(self):
        from pod_manager.tasks import task_teardown_feed_auto_cross_publish
        ep = self._ep()
        self._auto_link(ep, self.dest1)
        with mock.patch('pod_manager.tasks.task_rebuild_podcast_shell') as shell:
            task_teardown_feed_auto_cross_publish(self.service.id, [self.dest1.id], 'http://n.test')
        self.assertFalse(ep.cross_publications.exists())
        shell.assert_called_once_with(self.dest1.id, 'http://n.test')

    def test_tasks_tolerate_missing_source_feed(self):
        from pod_manager.tasks import (
            task_apply_feed_auto_cross_publish, task_teardown_feed_auto_cross_publish,
        )
        task_apply_feed_auto_cross_publish(999999, [self.dest1.id], 'http://n.test')
        task_teardown_feed_auto_cross_publish(999999, [self.dest1.id], 'http://n.test')


class AutoCrossPublishIngestHookTests(TestCase):
    """commit_episode()'s feed-level auto cross-publish hook: fires for new
    and updated episodes, skips the guid_update_only path, and the
    auto-migration reeval swaps auto links to the new parent's targets."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='acp-ingest')
        self.service = Podcast.objects.create(network=self.network, title='Service', slug='acpi-service')
        self.dest = Podcast.objects.create(network=self.network, title='Dest', slug='acpi-dest')
        self.service.auto_crosspublish_targets.set([self.dest])

    def _entry(self, **kw):
        base = dict(id='guid-acp-1', title='Ep 1')
        base.update(kw)
        return _FakeEntry(**base)

    def _commit(self, podcast, pub_entry):
        from pod_manager.ingesters.default import commit_episode
        stdout = mock.Mock()
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(podcast, pub_entry, None, 'Test', stdout)
        return stdout

    def _stdout_lines(self, stdout):
        return [c.args[0] for c in stdout.write.call_args_list]

    def test_new_episode_auto_cross_publishes_and_logs(self):
        stdout = self._commit(self.service, self._entry())
        ep = Episode.objects.get(guid_public='guid-acp-1')
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest, auto_created=True).exists())
        self.assertTrue(any('[AUTO-CP]' in line for line in self._stdout_lines(stdout)))

    def test_updated_episode_heals_missing_auto_link(self):
        self._commit(self.service, self._entry())
        EpisodeCrossPublication.objects.all().delete()
        self._commit(self.service, self._entry())
        ep = Episode.objects.get(guid_public='guid-acp-1')
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest, auto_created=True).exists())

    def test_guid_update_only_path_skips_auto_cp(self):
        owner = Podcast.objects.create(network=self.network, title='Owner', slug='acpi-owner')
        owner_dest = Podcast.objects.create(network=self.network, title='OwnerDest', slug='acpi-odest')
        owner.auto_crosspublish_targets.set([owner_dest])
        Episode.objects.create(
            podcast=owner, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-acp-1',
        )
        low = Podcast.objects.create(
            network=self.network, title='Low', slug='acpi-low', is_low_priority=True)
        low.auto_crosspublish_targets.set([self.dest])
        self._commit(low, self._entry())
        self.assertFalse(EpisodeCrossPublication.objects.exists())

    def test_auto_migration_swaps_auto_links_to_new_parent(self):
        low = Podcast.objects.create(
            network=self.network, title='Low', slug='acpi-low2', is_low_priority=True)
        low_dest = Podcast.objects.create(network=self.network, title='LowDest', slug='acpi-lowdest')
        low.auto_crosspublish_targets.set([low_dest])
        ep = Episode.objects.create(
            podcast=low, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-acp-1',
        )
        EpisodeCrossPublication.objects.create(episode=ep, podcast=low_dest, auto_created=True)
        manual_dest = Podcast.objects.create(network=self.network, title='Manual', slug='acpi-manual')
        manual = EpisodeCrossPublication.objects.create(episode=ep, podcast=manual_dest)

        self._commit(self.service, self._entry())

        ep.refresh_from_db()
        self.assertEqual(ep.podcast_id, self.service.id)
        self.assertFalse(ep.cross_publications.filter(podcast=low_dest).exists())
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest, auto_created=True).exists())
        self.assertTrue(EpisodeCrossPublication.objects.filter(id=manual.id, auto_created=False).exists())


class AutoCrossPublishPublishHookTests(TestCase):
    """Manually published episodes never pass through commit_episode — the
    publish flow's _sync_cross applies feed-level auto cross-publish itself."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='acp-pub')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='acpp-show')
        self.dest = Podcast.objects.create(network=self.network, title='Dest', slug='acpp-dest')
        self.podcast.auto_crosspublish_targets.set([self.dest])
        self.owner = User.objects.create_user('acppowner', password='x')
        self.network.owners.add(self.owner)
        self.client.force_login(self.owner)

    def _post_publish(self, action, **extra):
        data = {'action': action, 'network_slug': self.network.slug,
                'podcast_id': self.podcast.id, 'title': 'New Ep',
                'tags_json': '[]', 'chapters_json': 'null'}
        data.update(extra)
        with mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            return self.client.post(reverse('publish_episode'), data)

    def test_published_episode_gets_auto_links(self):
        self._post_publish('publish')
        ep = Episode.objects.get(title='New Ep')
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest, auto_created=True).exists())

    def test_draft_episode_gets_auto_links(self):
        self._post_publish('draft')
        ep = Episode.objects.get(title='New Ep')
        self.assertTrue(ep.cross_publications.filter(podcast=self.dest, auto_created=True).exists())


@override_settings(CACHES=TEST_CACHES)
class HiddenFeedVisibilityTests(TestCase):
    """Rollout step 7: listener-facing visibility of hidden feeds.

    user_feeds (the Your Feeds directory) drops every hidden feed for
    non-owners; home (the Dashboard) drops a hidden feed's chip AND episodes
    ONLY when the feed is not cross-published (D6). The owner show-hidden toggle
    (D5) reveals hidden feeds on both pages, scoped per network so a co-viewed
    non-owned network stays filtered.
    """

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.owner = User.objects.create_user('hf-owner', password='x')

        self.netA = Network.objects.create(name='Net A', slug='hf-a')
        self.netA.owners.add(self.owner)
        self.netB = Network.objects.create(name='Net B', slug='hf-b',
                                           custom_domain='netb.example.com')
        # Owner is a paying member (not owner) of B, so ?network=all merges both.
        NetworkMembership.objects.create(user=self.owner, network=self.netB,
                                         is_active_patron=True)

        # Network A feeds.
        self.vis_a = Podcast.objects.create(network=self.netA, title='Visible A', slug='vis-a')
        self.xp_a = Podcast.objects.create(network=self.netA, title='Hidden XP A', slug='xp-a', is_hidden=True)
        self.xplink_a = Podcast.objects.create(network=self.netA, title='Hidden Link A', slug='xplink-a', is_hidden=True)
        self.plain_a = Podcast.objects.create(network=self.netA, title='Hidden Plain A', slug='plain-a', is_hidden=True)
        # xp_a is cross-published via a feed-level auto target; xplink_a via an
        # actual per-episode link — exercising both arms of the D6 predicate.
        self.xp_a.auto_crosspublish_targets.set([self.vis_a])

        # Network B feeds.
        self.vis_b = Podcast.objects.create(network=self.netB, title='Visible B', slug='vis-b')
        self.plain_b = Podcast.objects.create(network=self.netB, title='Hidden Plain B', slug='plain-b', is_hidden=True)

        self.eps = {}
        for pod in (self.vis_a, self.xp_a, self.xplink_a, self.plain_a, self.vis_b, self.plain_b):
            self.eps[pod.slug] = Episode.objects.create(
                podcast=pod, title=f'Ep {pod.slug}', pub_date=timezone.now(),
                is_published=True, raw_description='x', clean_description='x')
        EpisodeCrossPublication.objects.create(episode=self.eps['xplink-a'], podcast=self.vis_a)

        self.mix = NetworkMix.objects.create(network=self.netA, name='Super A', slug='super-a')

    # -- helpers -----------------------------------------------------------

    def _anon(self):
        from django.contrib.auth.models import AnonymousUser
        return AnonymousUser()

    def _ctx(self, view, user, *, session=None, req_network=None, **params):
        req = _make_tenant_request(self.factory, req_network or self.netA,
                                   path='/', data=params, user=user)
        if session is not None:
            req.session = session
        with mock.patch('pod_manager.views.listener.main.render') as m:
            view(req)
        return m.call_args.args[2], req

    def _home(self, user, **kw):
        return self._ctx(views.home, user, **kw)

    def _feeds(self, user, **kw):
        return self._ctx(views.user_feeds, user, **kw)

    @staticmethod
    def _chip_slugs(ctx):
        return {p.slug for p in ctx['podcasts']}

    @staticmethod
    def _stream_titles(ctx):
        return {e.title for e in ctx['episodes']}

    @staticmethod
    def _dir_slugs(ctx):
        return {p['podcast'].slug for p in ctx['available_podcasts']}

    # -- user_feeds (Your Feeds directory) ---------------------------------

    def test_user_feeds_excludes_all_hidden_for_non_owner(self):
        ctx, _ = self._feeds(self._anon())
        slugs = self._dir_slugs(ctx)
        self.assertIn('vis-a', slugs)
        # Section 1: the directory drops EVERY hidden feed, even a cross-
        # published one (the cross-pub exception is Dashboard-only).
        self.assertNotIn('xp-a', slugs)
        self.assertNotIn('xplink-a', slugs)
        self.assertNotIn('plain-a', slugs)

    def test_user_feeds_networkmix_unaffected(self):
        ctx, _ = self._feeds(self._anon())
        mix_names = {f['mix'].slug for f in ctx['feed_data'] if f['is_network_mix']}
        self.assertIn('super-a', mix_names)

    def test_user_feeds_owner_toggle_off_matches_listener(self):
        ctx, _ = self._feeds(self.owner)
        self.assertFalse(ctx['show_hidden'])
        self.assertNotIn('plain-a', self._dir_slugs(ctx))
        self.assertNotIn('xp-a', self._dir_slugs(ctx))

    def test_user_feeds_owner_toggle_on_reveals_hidden(self):
        ctx, _ = self._feeds(self.owner, show_hidden='1')
        self.assertTrue(ctx['show_hidden'])
        self.assertTrue(ctx['show_hidden_available'])
        slugs = self._dir_slugs(ctx)
        self.assertIn('plain-a', slugs)
        self.assertIn('xp-a', slugs)

    def test_user_feeds_owner_toggle_renders_hidden_badge(self):
        req = _make_tenant_request(self.factory, self.netA, path='/',
                                   data={'show_hidden': '1'}, user=self.owner)
        resp = views.user_feeds(req)
        self.assertIn(b'HIDDEN', resp.content)
        # The control is a switch, so its label stays "Show hidden feeds" and the
        # on-state is carried by aria-checked + the class; the href flips it back
        # off. (It was a button whose label read "Hide hidden feeds" when on.)
        self.assertIn(b'switch-toggle is-on', resp.content)
        self.assertIn(b'aria-checked="true"', resp.content)
        self.assertIn(b'href="?show_hidden=0', resp.content)

    def test_user_feeds_non_owner_cannot_force_param(self):
        ctx, _ = self._feeds(self._anon(), show_hidden='1')
        self.assertFalse(ctx['show_hidden_available'])
        self.assertNotIn('plain-a', self._dir_slugs(ctx))

    def test_user_feeds_toggle_is_per_network(self):
        ctx, _ = self._feeds(self.owner, network='all', show_hidden='1')
        slugs = self._dir_slugs(ctx)
        # Owned network A: hidden feed revealed. Member-only network B: stays
        # filtered even though the toggle is on.
        self.assertIn('plain-a', slugs)
        self.assertIn('vis-b', slugs)
        self.assertNotIn('plain-b', slugs)

    # -- home (Dashboard) --------------------------------------------------

    def test_home_hidden_cross_published_keeps_chip_and_episodes(self):
        ctx, _ = self._home(self._anon())
        chips = self._chip_slugs(ctx)
        titles = self._stream_titles(ctx)
        self.assertIn('xp-a', chips)
        self.assertIn('xplink-a', chips)
        self.assertIn('Ep xp-a', titles)
        self.assertIn('Ep xplink-a', titles)

    def test_home_hidden_not_cross_published_dropped(self):
        ctx, _ = self._home(self._anon())
        self.assertNotIn('plain-a', self._chip_slugs(ctx))
        self.assertNotIn('Ep plain-a', self._stream_titles(ctx))

    def test_home_non_hidden_unchanged(self):
        ctx, _ = self._home(self._anon())
        self.assertIn('vis-a', self._chip_slugs(ctx))
        self.assertIn('Ep vis-a', self._stream_titles(ctx))

    def test_home_owner_toggle_reveals_hidden_not_cross_published(self):
        ctx, _ = self._home(self.owner, show_hidden='1')
        self.assertIn('plain-a', self._chip_slugs(ctx))
        self.assertIn('Ep plain-a', self._stream_titles(ctx))

    def test_home_owner_toggle_off_hides(self):
        ctx, _ = self._home(self.owner)
        self.assertNotIn('plain-a', self._chip_slugs(ctx))
        self.assertNotIn('Ep plain-a', self._stream_titles(ctx))

    def test_home_non_owner_cannot_force_param(self):
        ctx, _ = self._home(self._anon(), show_hidden='1')
        self.assertNotIn('plain-a', self._chip_slugs(ctx))
        self.assertNotIn('Ep plain-a', self._stream_titles(ctx))

    def test_home_toggle_persisted_in_session(self):
        _, req1 = self._home(self.owner, show_hidden='1')
        ctx2, _ = self._home(self.owner, session=req1.session)
        self.assertTrue(ctx2['podcasts'].exists())
        self.assertIn('plain-a', self._chip_slugs(ctx2))

    def test_home_renders_crosspub_toggle(self):
        req = _make_tenant_request(self.factory, self.netA, path='/', user=self.owner)
        resp = views.home(req)
        self.assertIn(b'crosspubToggle', resp.content)
        self.assertIn(b'Include cross-published episodes', resp.content)

    def test_home_show_filter_includes_cross_published_by_default(self):
        # xplink-a's episode is cross-published INTO vis-a; filtering by vis-a
        # surfaces it alongside vis-a's own episode when the toggle is on.
        ctx, _ = self._home(self._anon(), show='vis-a')
        self.assertTrue(ctx['include_cross_published'])
        titles = self._stream_titles(ctx)
        self.assertIn('Ep vis-a', titles)
        self.assertIn('Ep xplink-a', titles)

    def test_home_show_filter_crosspub_off_restricts_to_parent(self):
        ctx, _ = self._home(self._anon(), show='vis-a', crosspub='0')
        self.assertFalse(ctx['include_cross_published'])
        titles = self._stream_titles(ctx)
        self.assertIn('Ep vis-a', titles)
        self.assertNotIn('Ep xplink-a', titles)

    def test_home_toggle_is_per_network(self):
        ctx, _ = self._home(self.owner, network='all', show_hidden='1')
        chips = self._chip_slugs(ctx)
        titles = self._stream_titles(ctx)
        self.assertIn('plain-a', chips)
        self.assertIn('Ep plain-a', titles)
        # Member-only network B: its hidden feed stays filtered.
        self.assertNotIn('plain-b', chips)
        self.assertNotIn('Ep plain-b', titles)


class VendoredAssetTests(SimpleTestCase):
    """Guards that every front-end library is self-hosted (not pulled from a
    CDN at runtime) and that the vendored files are present and resolvable.

    These run without a database (SimpleTestCase) and are the regression net
    for the dependency-vendoring work: if someone re-adds a CDN <script>/<link>
    or a vendored file goes missing/downgrades, the suite fails here.
    """

    TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

    # CSS/JS library hosts we must never load from at runtime.
    CDN_HOSTS = (
        "cdn.jsdelivr.net", "cdn.quilljs.com", "unpkg.com",
        "cdnjs.cloudflare.com", "stackpath.bootstrapcdn.com",
        "maxcdn.bootstrapcdn.com", "code.jquery.com",
    )

    # Static path -> substring that must appear in the file (version / API
    # marker). None means "presence only".
    EXPECTED_ASSETS = {
        "pod_manager/css/bootstrap.min.css": "Bootstrap  v5.3.8",
        "pod_manager/js/bootstrap.bundle.min.js": "Bootstrap v5.3.8",
        "pod_manager/css/bootstrap-icons.css": "Bootstrap Icons v1.13.1",
        "pod_manager/fonts/bootstrap-icons.woff2": None,
        "pod_manager/fonts/bootstrap-icons.woff": None,
        "pod_manager/css/plyr.css": None,
        "pod_manager/js/plyr.js": None,
        "pod_manager/img/plyr.svg": None,
        "pod_manager/js/diff.min.js": "diffWordsWithSpace",
        "pod_manager/css/quill.snow.css": "ql-snow",
        "pod_manager/js/quill.js": None,
        "pod_manager/js/htmx.min.js": 'version:"2.0.8"',
        "pod_manager/js/fullcalendar.min.js": "FullCalendar Standard Bundle v6.1.15",
        "pod_manager/audio/blank.wav": None,
    }

    def _iter_templates(self):
        for path in self.TEMPLATE_DIR.rglob("*.html"):
            yield path, path.read_text(encoding="utf-8")

    def test_no_cdn_script_or_stylesheet_refs(self):
        """No template may load a JS/CSS library from a known CDN host.

        Image services (gravatar, ui-avatars) are intentionally remote and not
        matched here — we only flag <script src> and <link ... .css> pointing
        at library CDNs.
        """
        script_re = re.compile(r"""<script[^>]+src=["']https?://([^"'/]+)""", re.I)
        link_re = re.compile(r"""<link[^>]+href=["']https?://[^"']+\.css""", re.I)
        offenders = []
        for path, text in self._iter_templates():
            rel = path.relative_to(self.TEMPLATE_DIR)
            for host in script_re.findall(text):
                if any(cdn in host for cdn in self.CDN_HOSTS):
                    offenders.append(f"{rel}: <script> from {host}")
            for match in link_re.finditer(text):
                if any(cdn in match.group(0) for cdn in self.CDN_HOSTS):
                    offenders.append(f"{rel}: {match.group(0)[:80]}")
        self.assertEqual(
            offenders, [],
            "Templates load library assets from a CDN; vendor them instead:\n"
            + "\n".join(offenders),
        )

    def test_vendored_assets_present_and_resolvable(self):
        """Every expected vendored file resolves via the staticfiles finder."""
        from django.contrib.staticfiles import finders
        for static_path, marker in self.EXPECTED_ASSETS.items():
            found = finders.find(static_path)
            self.assertIsNotNone(
                found, f"Vendored asset not found on disk: {static_path}"
            )
            if marker is not None:
                content = Path(found).read_text(encoding="utf-8", errors="ignore")
                self.assertIn(
                    marker, content,
                    f"{static_path} is missing expected marker {marker!r} "
                    "(wrong version or corrupted download?)",
                )

    def test_bootstrap_icons_font_paths_are_relative_to_css(self):
        """The vendored bootstrap-icons.css must point at ../fonts/ (siblings of
        css/), not the upstream ./fonts/ — otherwise the glyph font 404s."""
        from django.contrib.staticfiles import finders
        css = Path(finders.find("pod_manager/css/bootstrap-icons.css")).read_text(
            encoding="utf-8"
        )
        self.assertIn("../fonts/bootstrap-icons.woff", css)
        self.assertNotIn('url("./fonts/', css)


@override_settings(DEBUG=False, ALLOWED_HOSTS=['*'])
class CustomFontRenderTests(TestCase):
    """A network with a custom font upload renders an @font-face block
    referencing that network's font URL (Feature 5 step 5)."""

    def test_renders_font_face_for_network_with_custom_font(self):
        cache.clear()  # tenant_custom_domains is cached for 60s across requests
        network = Network.objects.create(
            name='Themed Net', slug='themed-font', custom_domain='themed-font.example.test',
            custom_font_upload=_woff2_upload(), custom_font_family='Brand Sans',
        )
        resp = self.client.get('/', HTTP_HOST='themed-font.example.test')
        self.assertContains(resp, '@font-face')
        self.assertContains(resp, network.display_font_url)
        self.assertContains(resp, 'Brand Sans')


# ===========================================================================
# R2 audio mirror — service + backfill command (Phase 3/4/5)
# ===========================================================================
import hashlib as _hashlib
from io import StringIO

from botocore.exceptions import ClientError as _ClientError
from django.core.management import call_command
from django.core.management.base import CommandError

from pod_manager.services import r2_mirror
from pod_manager.services.r2_mirror import MirrorSkipped, mirror_episode_audio


def _not_found_error():
    return _ClientError({'Error': {'Code': '404'}}, 'HeadObject')


def _fake_r2_client(head_object_side_effect):
    """A MagicMock standing in for a boto3 S3 client. head_object_side_effect is
    a list of return values / exceptions consumed in call order (first call =
    the dedupe existence probe; a second {} = the post-upload verify)."""
    client = mock.MagicMock()
    client.head_object.side_effect = head_object_side_effect
    client.upload_file.return_value = None
    client.delete_object.return_value = None
    return client


@override_settings(
    R2_MIRROR_ENABLED=True,
    R2_PUBLIC_HOST='https://audio.test',
    R2_KEY_PREFIX='',
    R2_BUCKET='vecto-audio-test',
)
class R2MirrorServiceTests(TestCase):
    """mirror_episode_audio(): guards, key construction, idempotency, dedupe,
    re-version orphan recording, and re-adoption — all with a mocked R2 client
    and a real local temp file (so the content hash is exercised for real)."""

    def setUp(self):
        # R2_MIRROR_ENABLED is True for this class, so the standalone save signal
        # would dispatch a real mirror task on episode creation. Neutralize the
        # task dispatch — these tests call mirror_episode_audio() directly.
        p = mock.patch('pod_manager.tasks.task_mirror_episode_audio.delay')
        p.start()
        self.addCleanup(p.stop)
        self.network = Network.objects.create(name='Net', slug='net')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')
        self.episode = Episode.objects.create(
            podcast=self.podcast,
            title='Ep 1',
            pub_date=timezone.now(),
            raw_description='x',
            clean_description='x',
            audio_url_subscriber='https://traffic.libsyn.com/x/myep.mp3',
        )

    # 12-byte valid ID3 header so the mirror's is_audio_file() chokepoint accepts
    # the fixture without needing a real MP3 (tests stay offline). Prepended to
    # BOTH the temp file and the expected-key hash so the content hash matches.
    _AUDIO_MAGIC = b'ID3\x04\x00\x00\x00\x00\x00\x00\x00\x00'

    def _temp_audio(self, content=b'fake-audio-bytes'):
        f = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
        f.write(self._AUDIO_MAGIC + content)
        f.close()
        p = Path(f.name)
        self.addCleanup(lambda: p.unlink(missing_ok=True))
        return p

    def _expected_key_and_url(self, content):
        h = _hashlib.sha256(self._AUDIO_MAGIC + content).hexdigest()
        key = f"{self.network.id}/{self.podcast.id}/myep-{h[:16]}.mp3"
        return key, f"https://audio.test/{key}"

    # -- guards --------------------------------------------------------------
    def test_skips_when_not_premium(self):
        self.episode.audio_url_public = self.episode.audio_url_subscriber
        self.episode.save(update_fields=['audio_url_public'])
        with self.assertRaises(MirrorSkipped):
            mirror_episode_audio(self.episode.id, local_path=self._temp_audio())

    def test_skips_when_no_subscriber_audio(self):
        self.episode.audio_url_subscriber = None
        self.episode.save(update_fields=['audio_url_subscriber'])
        with self.assertRaises(MirrorSkipped):
            mirror_episode_audio(self.episode.id)

    def test_skips_dead_s3_source(self):
        self.episode.audio_url_subscriber = 'https://bucket.s3.amazonaws.com/ep.mp3'
        self.episode.save(update_fields=['audio_url_subscriber'])
        with self.assertRaises(MirrorSkipped):
            mirror_episode_audio(self.episode.id, local_path=self._temp_audio())

    def test_disabled_master_switch(self):
        with override_settings(R2_MIRROR_ENABLED=False):
            with self.assertRaises(MirrorSkipped):
                mirror_episode_audio(self.episode.id, local_path=self._temp_audio())

    def test_manual_requires_local_path(self):
        with self.assertRaises(ValueError):
            mirror_episode_audio(self.episode.id, manual=True)

    # -- manual owner upload (bypasses subscriber-URL gates) ------------------
    # Object-key naming isn't under test here (R2MirrorServiceTests above
    # already covers that) — these just confirm manual=True proceeds to a real
    # upload despite subscriber-URL states that would otherwise raise
    # MirrorSkipped.
    def test_manual_upload_succeeds_with_no_subscriber_audio(self):
        self.episode.audio_url_subscriber = None
        self.episode.save(update_fields=['audio_url_subscriber'])
        path = self._temp_audio(b'manual-upload-bytes')
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client):
            result = mirror_episode_audio(self.episode.id, local_path=path, force=True, manual=True)
        self.assertEqual(result['status'], 'mirrored')
        self.assertTrue(result['r2_url'])

    def test_manual_upload_succeeds_when_existing_subscriber_url_is_dead(self):
        # e.g. a private feed link that's since 404'd — manual upload must not
        # care, since it never fetches that URL.
        self.episode.audio_url_subscriber = 'https://dead-host.example.com/gone.mp3'
        self.episode.save(update_fields=['audio_url_subscriber'])
        path = self._temp_audio(b'manual-upload-over-dead-url')
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client):
            result = mirror_episode_audio(self.episode.id, local_path=path, force=True, manual=True)
        self.assertEqual(result['status'], 'mirrored')
        self.assertTrue(result['r2_url'])

    def test_manual_upload_succeeds_over_dead_s3_source(self):
        # normal ingestion refuses to mirror from the dead S3 bucket — manual
        # upload bypasses that check too, same reasoning as above.
        self.episode.audio_url_subscriber = 'https://bucket.s3.amazonaws.com/ep.mp3'
        self.episode.save(update_fields=['audio_url_subscriber'])
        path = self._temp_audio(b'manual-upload-over-s3-dead')
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client):
            result = mirror_episode_audio(self.episode.id, local_path=path, force=True, manual=True)
        self.assertEqual(result['status'], 'mirrored')
        self.assertTrue(result['r2_url'])

    # -- upload + persist ----------------------------------------------------
    def test_mirror_uploads_and_persists(self):
        content = b'hello-audio'
        path = self._temp_audio(content)
        key, url = self._expected_key_and_url(content)
        client = _fake_r2_client([_not_found_error(), {}])  # miss, then verify
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value='etag123:99'):
            result = mirror_episode_audio(self.episode.id, local_path=path)

        self.assertEqual(result['status'], 'mirrored')
        self.assertEqual(result['key'], key)
        client.upload_file.assert_called_once()
        cargs, ckwargs = client.upload_file.call_args
        self.assertEqual(cargs[2], key)  # (local, bucket, key)
        self.assertEqual(ckwargs['ExtraArgs']['ContentType'], 'audio/mpeg')
        self.assertIn('immutable', ckwargs['ExtraArgs']['CacheControl'])

        self.episode.refresh_from_db()
        self.assertEqual(self.episode.r2_url, url)
        self.assertIsNotNone(self.episode.r2_uploaded_at)
        self.assertEqual(self.episode.r2_source_signature, 'etag123:99')

    def test_content_type_for_m4a(self):
        self.episode.audio_url_subscriber = 'https://traffic.libsyn.com/x/myep.m4a'
        self.episode.save(update_fields=['audio_url_subscriber'])
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value=''):
            mirror_episode_audio(self.episode.id, local_path=self._temp_audio())
        _, ckwargs = client.upload_file.call_args
        self.assertEqual(ckwargs['ExtraArgs']['ContentType'], 'audio/mp4')

    # -- dedupe --------------------------------------------------------------
    def test_dedupe_skips_upload(self):
        client = _fake_r2_client([{}])  # object already present
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value=''):
            result = mirror_episode_audio(self.episode.id, local_path=self._temp_audio())
        self.assertEqual(result['status'], 'deduped')
        client.upload_file.assert_not_called()
        self.episode.refresh_from_db()
        self.assertTrue(self.episode.r2_url)

    # -- idempotency ---------------------------------------------------------
    def test_idempotent_skip_when_source_unchanged(self):
        self.episode.r2_url = 'https://audio.test/0/0/old-aaaaaaaaaaaaaaaa.mp3'
        self.episode.r2_source_signature = 'sig-1'
        self.episode.save(update_fields=['r2_url', 'r2_source_signature'])
        with mock.patch.object(r2_mirror, 'get_r2_client') as gc, \
             mock.patch.object(r2_mirror, '_head_signature', return_value='sig-1'):
            result = mirror_episode_audio(self.episode.id)
        self.assertEqual(result['status'], 'skipped')
        gc.assert_not_called()  # never even built a client

    # -- re-version / orphans ------------------------------------------------
    def test_force_reversion_records_orphan(self):
        old_key = f"{self.network.id}/{self.podcast.id}/old-bbbbbbbbbbbbbbbb.mp3"
        self.episode.r2_url = f"https://audio.test/{old_key}"
        self.episode.r2_source_signature = 'old'
        self.episode.save(update_fields=['r2_url', 'r2_source_signature'])

        content = b'new-version-bytes'
        new_key, new_url = self._expected_key_and_url(content)
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value='new'):
            result = mirror_episode_audio(self.episode.id, local_path=self._temp_audio(content), force=True)

        self.assertEqual(result['key'], new_key)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.r2_url, new_url)
        orphan = R2OrphanedObject.objects.get(key=old_key)
        self.assertEqual(orphan.reason, R2OrphanedObject.Reason.REVERSION)
        self.assertEqual(orphan.episode_id, self.episode.id)

    def test_reversion_not_orphaned_when_key_shared(self):
        old_key = f"{self.network.id}/{self.podcast.id}/old-cccccccccccccccc.mp3"
        old_url = f"https://audio.test/{old_key}"
        self.episode.r2_url = old_url
        self.episode.save(update_fields=['r2_url'])
        # A second episode still points at the same key -> not an orphan.
        Episode.objects.create(
            podcast=self.podcast, title='Ep 2', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_subscriber='https://traffic.libsyn.com/x/other.mp3',
            r2_url=old_url,
        )
        client = _fake_r2_client([_not_found_error(), {}])
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value='new'):
            mirror_episode_audio(self.episode.id, local_path=self._temp_audio(b'changed'), force=True)
        self.assertFalse(R2OrphanedObject.objects.filter(key=old_key).exists())

    def test_readoption_clears_orphan_for_live_key(self):
        content = b'readopt-bytes'
        key, _ = self._expected_key_and_url(content)
        R2OrphanedObject.objects.create(key=key, reason=R2OrphanedObject.Reason.RECONCILIATION)
        client = _fake_r2_client([{}])  # dedupe hit -> key becomes live again
        with mock.patch.object(r2_mirror, 'get_r2_client', return_value=client), \
             mock.patch.object(r2_mirror, '_head_signature', return_value=''):
            mirror_episode_audio(self.episode.id, local_path=self._temp_audio(content))
        self.assertFalse(R2OrphanedObject.objects.filter(key=key).exists())


class GDriveConfirmationTests(TestCase):
    """gdrive_download.follow_confirmation(): click through Drive's large-file
    'couldn't scan for viruses' interstitial to the real download."""

    def _resp(self, *, ctype='text/html', text=''):
        r = mock.Mock()
        r.headers = {'Content-Type': ctype}
        r.text = text
        r.raise_for_status = mock.Mock()
        return r

    def test_looks_like_interstitial(self):
        from pod_manager.services import gdrive_download as g
        self.assertTrue(g.looks_like_interstitial(self._resp(ctype='text/html; charset=utf-8')))
        self.assertFalse(g.looks_like_interstitial(self._resp(ctype='audio/mpeg')))

    def test_modern_download_form(self):
        from pod_manager.services import gdrive_download as g
        html = (
            '<form id="download-form" action="https://drive.usercontent.google.com/download" method="get">'
            '<input type="hidden" name="id" value="FILEID">'
            '<input type="hidden" name="export" value="download">'
            '<input type="hidden" name="confirm" value="t">'
            '<input type="hidden" name="uuid" value="UUID-123"></form>'
        )
        file_resp = self._resp(ctype='audio/mpeg')
        session = mock.Mock(cookies={})
        session.get = mock.Mock(return_value=file_resp)
        out = g.follow_confirmation(
            session, 'https://docs.google.com/uc?export=download&id=FILEID', self._resp(text=html), 300)
        self.assertIs(out, file_resp)
        args, kwargs = session.get.call_args
        self.assertEqual(args[0], 'https://drive.usercontent.google.com/download')
        self.assertEqual(kwargs['params']['confirm'], 't')
        self.assertEqual(kwargs['params']['uuid'], 'UUID-123')

    def test_cookie_token_flow(self):
        from pod_manager.services import gdrive_download as g
        file_resp = self._resp(ctype='audio/mpeg')
        session = mock.Mock(cookies={'download_warning_abc': 'TOKEN9'})
        session.get = mock.Mock(return_value=file_resp)
        out = g.follow_confirmation(
            session, 'https://drive.google.com/uc?export=download&id=X',
            self._resp(text='<html><body>can&#39;t scan</body></html>'), 300)
        self.assertIs(out, file_resp)
        self.assertIn('confirm=TOKEN9', session.get.call_args[0][0])

    def test_unresolvable_returns_original(self):
        from pod_manager.services import gdrive_download as g
        first = self._resp(text='<html>nothing useful</html>')
        session = mock.Mock(cookies={})
        session.get = mock.Mock()
        out = g.follow_confirmation(session, 'https://docs.google.com/uc?id=X', first, 300)
        self.assertIs(out, first)
        session.get.assert_not_called()

    def test_offsite_form_action_refused(self):
        from pod_manager.services import gdrive_download as g
        html = ('<form id="download-form" action="https://evil.example.com/steal" method="get">'
                '<input type="hidden" name="id" value="X"></form>')
        session = mock.Mock(cookies={})
        session.get = mock.Mock()
        first = self._resp(text=html)
        out = g.follow_confirmation(session, 'https://docs.google.com/uc?id=X', first, 300)
        self.assertIs(out, first)  # non-google action ignored (SSRF guard)
        session.get.assert_not_called()


class R2BackfillCommandTests(TestCase):
    """manage.py mirror_audio_to_r2 bulk selection + dispatch. apply_async is
    patched so no task actually runs (and tests never touch R2). R2_MIRROR_ENABLED
    stays False (the test default) so the standalone save signal doesn't fire
    during episode creation — the command dispatches via apply_async regardless."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='net')
        self.other_net = Network.objects.create(name='Other', slug='other')
        self.p1 = Podcast.objects.create(network=self.network, title='P1', slug='p1')
        self.p2 = Podcast.objects.create(network=self.network, title='P2', slug='p2')

        def mk(podcast, sub, **extra):
            return Episode.objects.create(
                podcast=podcast, title='t', pub_date=timezone.now(),
                raw_description='x', clean_description='x',
                audio_url_subscriber=sub, **extra,
            )

        self.e_gd = mk(self.p1, 'https://docs.google.com/uc?export=download&id=abc')
        self.e_lib = mk(self.p1, 'https://traffic.libsyn.com/x/a.mp3')
        self.e_s3 = mk(self.p2, 'https://bucket.s3.amazonaws.com/a.mp3')       # unfetchable
        self.e_done = mk(self.p2, 'https://traffic.libsyn.com/x/b.mp3',
                         r2_url='https://audio.test/x/done.mp3')               # already mirrored
        # Not premium: subscriber == public -> excluded by is_premium.
        self.e_pub = mk(self.p2, 'https://x.megaphone.fm/p.mp3',
                        audio_url_public='https://x.megaphone.fm/p.mp3')

    def _run(self, *args):
        # The bulk command previews by default; these dispatch-asserting tests run
        # in apply mode. Preview behavior is covered by test_preview_is_the_default.
        out = StringIO()
        with mock.patch('pod_manager.tasks.task_mirror_episode_audio.apply_async') as m:
            call_command('mirror_audio_to_r2', '--apply', *args, stdout=out, stderr=StringIO())
        ids = {c.kwargs['args'][0] for c in m.call_args_list}
        return ids, m, out.getvalue()

    def test_requires_a_scope(self):
        with self.assertRaises(CommandError):
            call_command('mirror_audio_to_r2', stdout=StringIO(), stderr=StringIO())

    def test_all_selects_premium_fetchable_missing(self):
        ids, _, _ = self._run('--all')
        # gdrive + libsyn; s3 (unfetchable), done (already mirrored), pub (not premium) excluded
        self.assertEqual(ids, {self.e_gd.id, self.e_lib.id})

    def test_origins_filter(self):
        ids, _, _ = self._run('--all', '--origins=gdrive')
        self.assertEqual(ids, {self.e_gd.id})

    def test_origins_alone_is_a_valid_scope(self):
        # --origins without --all/--network/--podcast should work, not error.
        ids, _, _ = self._run('--origins=gdrive')
        self.assertEqual(ids, {self.e_gd.id})

    def test_podcast_filter(self):
        ids, _, _ = self._run('--podcast=p1')
        self.assertEqual(ids, {self.e_gd.id, self.e_lib.id})

    def test_network_filter_excludes_other_network(self):
        Podcast.objects.create(network=self.other_net, title='OP', slug='op')
        ids, _, _ = self._run('--network=other')
        self.assertEqual(ids, set())

    def test_force_includes_already_mirrored_and_passes_force(self):
        ids, m, _ = self._run('--all', '--force')
        self.assertIn(self.e_done.id, ids)
        self.assertTrue(all(c.kwargs['kwargs']['force'] for c in m.call_args_list))

    def test_preview_is_the_default(self):
        # No --apply: preview only — lists targets, dispatches nothing.
        out = StringIO()
        with mock.patch('pod_manager.tasks.task_mirror_episode_audio.apply_async') as m:
            call_command('mirror_audio_to_r2', '--all', stdout=out, stderr=StringIO())
        m.assert_not_called()
        self.assertIn('would mirror', out.getvalue())

    def test_limit_caps_dispatch_count(self):
        # Two premium fetchable episodes available; --limit=1 dispatches only one.
        ids, m, _ = self._run('--all', '--limit', '1')
        self.assertEqual(len(ids), 1)
        self.assertTrue(ids <= {self.e_gd.id, self.e_lib.id})


class ChapterExtractionTests(TestCase):
    """Unit coverage for the style-driven description → chapters parser."""

    def test_trailing_paren_style(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        desc = (
            "<p>There are SPOILERS ahead.</p>"
            "<p>DTF St. Louis (00:01:33)</p>"
            "<p>One Battle After Another (2025) (00:39:38)</p>"
        )
        result = extract_chapters_from_html(desc)
        # First marker isn't at 0 → an Intro chapter is prepended.
        self.assertEqual(result['version'], '1.2.0')
        self.assertEqual(result['chapters'], [
            {'startTime': 0, 'title': 'Intro'},
            {'startTime': 93, 'title': 'DTF St. Louis'},
            {'startTime': 2378, 'title': 'One Battle After Another (2025)'},
        ])

    def test_hms_and_ms_timecodes(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        desc = "A (01:24:09)\nB (1:10:26)\nC (35:32)"
        chapters = extract_chapters_from_html(desc)['chapters']
        times = {c['title']: c['startTime'] for c in chapters}
        self.assertEqual(times['A'], 5049)
        self.assertEqual(times['B'], 4226)
        self.assertEqual(times['C'], 35 * 60 + 32)

    def test_single_digit_minute_in_hms_is_recognized(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        # "00:1:13" — a hand-typed single-digit minute — must parse, not be dropped
        # (the drop used to lose the first real chapter and leave only the Intro).
        desc = "Cold Open (00:1:13)\nMain Topic (00:42:05)"
        chapters = extract_chapters_from_html(desc)['chapters']
        self.assertEqual(chapters, [
            {'startTime': 0, 'title': 'Intro'},
            {'startTime': 73, 'title': 'Cold Open'},
            {'startTime': 2525, 'title': 'Main Topic'},
        ])

    def test_leading_time_style(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        desc = "<ul><li>(00:00:00) Intro</li><li>00:05:30 - Topic Two</li></ul>"
        chapters = extract_chapters_from_html(desc)['chapters']
        self.assertEqual(chapters, [
            {'startTime': 0, 'title': 'Intro'},
            {'startTime': 330, 'title': 'Topic Two'},
        ])

    def test_no_intro_added_when_first_is_zero(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        chapters = extract_chapters_from_html("Open (00:00:00)\nNext (00:10:00)")['chapters']
        self.assertEqual(chapters[0], {'startTime': 0, 'title': 'Open'})

    def test_prose_without_two_markers_yields_nothing(self):
        from pod_manager.services.chapter_extraction import extract_chapters_from_html
        self.assertIsNone(extract_chapters_from_html("Just some show notes, no chapters."))
        self.assertIsNone(extract_chapters_from_html("One lonely marker (00:01:00)"))


class ExtractDescriptionChaptersCommandTests(TestCase):
    """manage.py extract_description_chapters: scope, preview/apply, leave-alone."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='net')
        self.other_net = Network.objects.create(name='Other', slug='other')
        self.p1 = Podcast.objects.create(network=self.network, title='P1', slug='p1')
        self.p2 = Podcast.objects.create(network=self.network, title='P2', slug='p2')
        self.desc = (
            "<p>DTF St. Louis (00:01:33)</p>"
            "<p>Shoresey Spoilers (00:48:36)</p>"
        )

        def mk(podcast, desc='', **extra):
            return Episode.objects.create(
                podcast=podcast, title='t', pub_date=timezone.now(),
                raw_description=desc, clean_description=desc, **extra,
            )

        self.e_has_chaps = mk(self.p1, self.desc)
        self.e_blank = mk(self.p1, self.desc)
        self.e_no_markers = mk(self.p2, 'Plain notes, nothing to see.')
        # Give one episode existing chapters so it's left alone by default.
        self.e_has_chaps.chapters_public = {'version': '1.2.0',
                                            'chapters': [{'startTime': 5, 'title': 'Manual'}]}
        self.e_has_chaps.save()

    def _run(self, *args):
        out = StringIO()
        call_command('extract_description_chapters', '--network=net', *args,
                     stdout=out, stderr=StringIO())
        return out.getvalue()

    def _reload(self, ep):
        return Episode.objects.get(pk=ep.pk)

    def test_unknown_network_errors(self):
        with self.assertRaises(CommandError):
            call_command('extract_description_chapters', '--network=nope',
                         stdout=StringIO(), stderr=StringIO())

    def test_unknown_podcast_errors(self):
        with self.assertRaises(CommandError):
            call_command('extract_description_chapters', '--network=net', '--podcast=ghost',
                         stdout=StringIO(), stderr=StringIO())

    def test_preview_is_default_writes_nothing(self):
        out = self._run()
        self.assertIn('Would apply', out)
        self.assertIsNone(self._reload(self.e_blank).chapters_public)

    def test_apply_writes_chapters_to_both_sides(self):
        self._run('--apply')
        ep = self._reload(self.e_blank)
        titles = [c['title'] for c in ep.chapters_public['chapters']]
        self.assertEqual(titles, ['Intro', 'DTF St. Louis', 'Shoresey Spoilers'])
        # Both feeds are filled from the single stored description.
        self.assertEqual(ep.chapters_public, ep.chapters_private)

    def test_existing_chapters_left_alone_by_default(self):
        self._run('--apply')
        ep = self._reload(self.e_has_chaps)
        self.assertEqual(ep.chapters_public['chapters'], [{'startTime': 5, 'title': 'Manual'}])

    def test_overwrite_replaces_existing(self):
        self._run('--overwrite', '--apply')
        ep = self._reload(self.e_has_chaps)
        titles = [c['title'] for c in ep.chapters_public['chapters']]
        self.assertIn('DTF St. Louis', titles)

    def test_no_markers_episode_is_not_updated(self):
        self._run('--apply')
        self.assertIsNone(self._reload(self.e_no_markers).chapters_public)

    def test_podcast_scope_limits_to_podcast(self):
        self._run('--podcast=p1', '--apply')
        self.assertIsNotNone(self._reload(self.e_blank).chapters_public)
        # p2's episode is out of scope (it had no markers anyway, but assert scoping).
        self.assertIsNone(self._reload(self.e_no_markers).chapters_public)

    def test_episode_scope_limits_to_episode(self):
        self._run('--episode', str(self.e_blank.id), '--apply')
        self.assertIsNotNone(self._reload(self.e_blank).chapters_public)
        # The other episode is out of scope and keeps its manual chapters.
        self.assertEqual(
            self._reload(self.e_has_chaps).chapters_public['chapters'],
            [{'startTime': 5, 'title': 'Manual'}],
        )

    def test_limit_caps_examined(self):
        # Three episodes in the network; --limit=1 examines only one.
        out = self._run('--limit', '1')
        self.assertIn('examined 1', out)

    def test_other_network_untouched(self):
        Podcast.objects.create(network=self.other_net, title='OP', slug='op')
        self._run('--apply')
        # nothing in the other network was even considered
        self.assertIsNone(self._reload(self.e_no_markers).chapters_public)


@override_settings(CACHES=TEST_CACHES)
class CommandSafetyIdiomTests(TestCase):
    """Step 0 safety-idiom contract: mutating commands preview by default, --apply
    executes, and irreversible deletions abort without --yes. One regression guard
    per changed command — the call-site flips in tasks.py rely on this holding."""

    # -- prune_logs (irreversible deletion) ---------------------------------
    def _make_old_log(self):
        entry = LogEntry.objects.create(
            level=LogEntry.Level.INFO, level_no=20, logger_name='t',
            module='t', func_name='t', lineno=1, message='old',
        )
        old = timezone.now() - timedelta(days=99)
        LogEntry.objects.filter(pk=entry.pk).update(created_at=old)
        return entry

    def test_prune_logs_preview_keeps_rows(self):
        self._make_old_log()
        call_command('prune_logs', stdout=StringIO())
        self.assertEqual(LogEntry.objects.count(), 1)

    def test_prune_logs_apply_without_yes_aborts(self):
        self._make_old_log()
        with self.assertRaises(CommandError):
            call_command('prune_logs', '--apply', stdout=StringIO())
        self.assertEqual(LogEntry.objects.count(), 1)

    def test_prune_logs_apply_yes_deletes(self):
        self._make_old_log()
        call_command('prune_logs', '--apply', '--yes', stdout=StringIO())
        self.assertEqual(LogEntry.objects.count(), 0)

    # -- clean_mix_images (irreversible deletion) ---------------------------
    def _media_with_orphan(self):
        media_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, media_root, ignore_errors=True)
        covers = Path(media_root) / 'mix_covers'
        covers.mkdir()
        (covers / 'orphan.jpg').write_bytes(b'x')
        return media_root, covers / 'orphan.jpg'

    def test_clean_mix_images_preview_keeps_file(self):
        media_root, orphan = self._media_with_orphan()
        with override_settings(MEDIA_ROOT=media_root):
            call_command('clean_mix_images', stdout=StringIO())
        self.assertTrue(orphan.exists())

    def test_clean_mix_images_apply_without_yes_aborts(self):
        media_root, orphan = self._media_with_orphan()
        with override_settings(MEDIA_ROOT=media_root):
            with self.assertRaises(CommandError):
                call_command('clean_mix_images', '--apply', stdout=StringIO())
        self.assertTrue(orphan.exists())

    def test_clean_mix_images_apply_yes_deletes(self):
        media_root, orphan = self._media_with_orphan()
        with override_settings(MEDIA_ROOT=media_root):
            call_command('clean_mix_images', '--apply', '--yes', stdout=StringIO())
        self.assertFalse(orphan.exists())

    # -- destructive R2 commands abort without --yes ------------------------
    def test_r2_cleanup_orphans_apply_without_yes_aborts(self):
        # The --yes gate fires before any R2 client is touched, so no mock needed.
        with self.assertRaises(CommandError):
            call_command('r2_cleanup_orphans', '--apply', stdout=StringIO())

    def test_purge_r2_dev_apply_without_yes_aborts(self):
        with self.assertRaises(CommandError):
            call_command('purge_r2_dev', '--apply', stdout=StringIO())

    def test_purge_r2_media_dev_apply_without_yes_aborts(self):
        with self.assertRaises(CommandError):
            call_command('purge_r2_media_dev', '--apply', stdout=StringIO())

    # -- clear_transcription_queue (irreversible deletion) ------------------
    def test_clear_transcription_queue_preview_does_not_purge(self):
        with mock.patch(
            'pod_manager.management.commands.clear_transcription_queue.purge_transcription_queue'
        ) as m:
            call_command('clear_transcription_queue', stdout=StringIO())
        m.assert_not_called()

    def test_clear_transcription_queue_apply_without_yes_aborts(self):
        with mock.patch(
            'pod_manager.management.commands.clear_transcription_queue.purge_transcription_queue'
        ) as m:
            with self.assertRaises(CommandError):
                call_command('clear_transcription_queue', '--apply', stdout=StringIO())
        m.assert_not_called()

    def test_clear_transcription_queue_apply_yes_purges(self):
        with mock.patch(
            'pod_manager.management.commands.clear_transcription_queue.purge_transcription_queue',
            return_value={'purged': 0, 'deleted': 0},
        ) as m:
            call_command('clear_transcription_queue', '--apply', '--yes', stdout=StringIO())
        m.assert_called_once()

    # -- backfill prune gates (irreversible deletion) -----------------------
    # The --prune --apply --yes gate fires before any storage/R2 call, so these
    # need no mocking.
    def test_backfill_media_to_r2_prune_apply_without_yes_aborts(self):
        with self.assertRaises(CommandError):
            call_command('backfill_media_to_r2', '--all', '--prune', '--apply', stdout=StringIO())

    def test_backfill_transcripts_to_r2_prune_apply_without_yes_aborts(self):
        with self.assertRaises(CommandError):
            call_command('backfill_transcripts_to_r2', '--all', '--prune', '--apply', stdout=StringIO())

    # -- rewind_gdrive_audio (reversible mutation; guards the task call-site) -
    def _rewind_csv_and_episode(self):
        net = Network.objects.create(name='Net', slug='net')
        pod = Podcast.objects.create(network=net, title='Show', slug='show')
        ep = Episode.objects.create(
            podcast=pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_subscriber='https://docs.google.com/uc?id=z',
            audio_url_public='https://bucket.s3.amazonaws.com/a.mp3',
            audio_locked=True, match_reason='GDrive Recovery (EXACT)',
        )
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        csv_path = d / 'run.csv'
        csv_path.write_text(f"Episode ID\n{ep.id}\n", encoding='utf-8')
        return ep, str(csv_path)

    def test_rewind_preview_does_not_change_episode(self):
        ep, csv_path = self._rewind_csv_and_episode()
        call_command('rewind_gdrive_audio', csv_path, stdout=StringIO())
        ep.refresh_from_db()
        self.assertTrue(ep.audio_locked)
        self.assertEqual(ep.audio_url_subscriber, 'https://docs.google.com/uc?id=z')

    def test_rewind_apply_restores_s3_url(self):
        ep, csv_path = self._rewind_csv_and_episode()
        call_command('rewind_gdrive_audio', csv_path, '--apply', stdout=StringIO())
        ep.refresh_from_db()
        self.assertFalse(ep.audio_locked)
        self.assertEqual(ep.audio_url_subscriber, 'https://bucket.s3.amazonaws.com/a.mp3')
        self.assertEqual(ep.audio_url_public, '')
        self.assertEqual(ep.match_reason, 'Missing Audio')

    # -- backfill_baldmove_tags (reversible mutation) -----------------------
    def _baldmove_episode(self):
        net = Network.objects.create(name='Bald Move', slug='baldmove')
        pod = Podcast.objects.create(network=net, title='Show', slug='show')
        return Episode.objects.create(
            podcast=pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            link='https://baldmove.com/?p=1', tags=[],
        )

    def test_backfill_baldmove_tags_preview_does_not_save(self):
        ep = self._baldmove_episode()
        with mock.patch(
            'pod_manager.management.commands.backfill_baldmove_tags.scrape_tags_from_wp',
            return_value=['spoilers'],
        ), mock.patch('pod_manager.management.commands.backfill_baldmove_tags.time.sleep'):
            call_command('backfill_baldmove_tags', stdout=StringIO())
        ep.refresh_from_db()
        self.assertEqual(ep.tags, [])

    def test_backfill_baldmove_tags_apply_saves(self):
        ep = self._baldmove_episode()
        with mock.patch(
            'pod_manager.management.commands.backfill_baldmove_tags.scrape_tags_from_wp',
            return_value=['spoilers'],
        ), mock.patch('pod_manager.management.commands.backfill_baldmove_tags.time.sleep'):
            call_command('backfill_baldmove_tags', '--apply', stdout=StringIO())
        ep.refresh_from_db()
        self.assertEqual(ep.tags, ['spoilers'])


def _fake_list_client(objects):
    """MagicMock S3 client whose list_objects_v2 paginator yields one page of
    `objects` = [(key, last_modified), ...]."""
    client = mock.MagicMock()
    page = {'Contents': [{'Key': k, 'LastModified': lm} for k, lm in objects]}
    paginator = mock.MagicMock()
    paginator.paginate.return_value = [page]
    client.get_paginator.return_value = paginator
    return client


@override_settings(R2_PUBLIC_HOST='https://audio.test', R2_BUCKET='vecto-audio-test', R2_KEY_PREFIX='')
class R2MaintenanceTests(TestCase):
    """reconcile_orphans / cleanup_orphans / rekey_episode_audio / purge_dev_prefix.
    R2_MIRROR_ENABLED stays False (test default) so creating episodes here never
    fires the standalone mirror signal; the R2 client is always mocked."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='net')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show')

    def _episode(self, **extra):
        return Episode.objects.create(
            podcast=self.podcast, title='t', pub_date=timezone.now(),
            raw_description='x', clean_description='x', **extra,
        )

    # -- reconcile -----------------------------------------------------------
    def test_reconcile_records_only_unreferenced_old_prod_keys(self):
        from pod_manager.services import r2_maintenance
        net, pod = self.network.id, self.podcast.id
        ref_key = f'{net}/{pod}/ref-aaaaaaaaaaaaaaaa.mp3'
        self._episode(r2_url=f'https://audio.test/{ref_key}')
        old = timezone.now() - timedelta(days=30)
        new = timezone.now()
        objects = [
            (ref_key, old),                                  # referenced -> skip
            (f'{net}/{pod}/orphan-bbbbbbbbbbbbbbbb.mp3', old),  # unreferenced+old -> record
            ('dev/9/9/devthing-cccccccccccccccc.mp3', old),  # dev namespace -> skip
            (f'{net}/{pod}/tooNew-dddddddddddddddd.mp3', new),  # too new -> skip
        ]
        client = _fake_list_client(objects)
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            result = r2_maintenance.reconcile_orphans(apply=True, age_days=7)
        keys = set(R2OrphanedObject.objects.values_list('key', flat=True))
        self.assertEqual(keys, {f'{net}/{pod}/orphan-bbbbbbbbbbbbbbbb.mp3'})
        self.assertEqual(result['scanned'], 4)

    def test_reconcile_dry_run_records_nothing(self):
        from pod_manager.services import r2_maintenance
        objects = [('1/1/x-eeeeeeeeeeeeeeee.mp3', timezone.now() - timedelta(days=30))]
        client = _fake_list_client(objects)
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            r2_maintenance.reconcile_orphans(apply=False, age_days=7)
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    # -- cleanup -------------------------------------------------------------
    @override_settings(R2_ORPHAN_RETENTION_DAYS=90, R2_REKEY_GRACE_DAYS=7)
    def test_cleanup_deletes_expired_unreferenced_only(self):
        from pod_manager.services import r2_maintenance
        now = timezone.now()
        R = R2OrphanedObject.Reason
        expired_rev = R2OrphanedObject.objects.create(
            key='1/1/expired-rev.mp3', reason=R.REVERSION, orphaned_at=now - timedelta(days=100))
        R2OrphanedObject.objects.create(
            key='1/1/fresh-rev.mp3', reason=R.REVERSION, orphaned_at=now - timedelta(days=10))
        expired_rekey = R2OrphanedObject.objects.create(
            key='1/1/expired-rekey.mp3', reason=R.MOVE_REKEY, orphaned_at=now - timedelta(days=10))
        R2OrphanedObject.objects.create(
            key='1/1/fresh-rekey.mp3', reason=R.MOVE_REKEY, orphaned_at=now - timedelta(days=3))
        # Expired but RE-ADOPTED (a live episode points at it) -> drop row, keep object.
        readopt_key = '1/1/readopt.mp3'
        self._episode(r2_url=f'https://audio.test/{readopt_key}')
        R2OrphanedObject.objects.create(
            key=readopt_key, reason=R.REVERSION, orphaned_at=now - timedelta(days=100))

        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            result = r2_maintenance.cleanup_orphans(apply=True)

        self.assertEqual(set(result['deleted']), {'1/1/expired-rev.mp3', '1/1/expired-rekey.mp3'})
        self.assertEqual(result['readopted'], 1)
        # delete_objects called with exactly the two expired-unreferenced keys
        _, ckwargs = client.delete_objects.call_args
        deleted = {o['Key'] for o in ckwargs['Delete']['Objects']}
        self.assertEqual(deleted, {'1/1/expired-rev.mp3', '1/1/expired-rekey.mp3'})
        # rows: the two expired-unreferenced + the readopted row are gone; fresh remain
        remaining = set(R2OrphanedObject.objects.values_list('key', flat=True))
        self.assertEqual(remaining, {'1/1/fresh-rev.mp3', '1/1/fresh-rekey.mp3'})

    @override_settings(R2_ORPHAN_RETENTION_DAYS=90, R2_REKEY_GRACE_DAYS=7)
    def test_cleanup_dry_run_deletes_nothing(self):
        from pod_manager.services import r2_maintenance
        R2OrphanedObject.objects.create(
            key='1/1/expired.mp3', reason=R2OrphanedObject.Reason.REVERSION,
            orphaned_at=timezone.now() - timedelta(days=100))
        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            r2_maintenance.cleanup_orphans(apply=False)
        client.delete_objects.assert_not_called()
        self.assertEqual(R2OrphanedObject.objects.count(), 1)

    # -- rekey ---------------------------------------------------------------
    def test_rekey_copies_and_records_move_orphan(self):
        from pod_manager.services import r2_maintenance
        # r2_url points at an OLD parent (999/888); current parent is net/pod.
        ep = self._episode(r2_url='https://audio.test/999/888/file-1234567890abcdef.mp3')
        client = mock.MagicMock()
        client.head_object.return_value = {}  # verify after copy
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            result = r2_maintenance.rekey_episode_audio(ep.id)

        new_key = f'{self.network.id}/{self.podcast.id}/file-1234567890abcdef.mp3'
        self.assertEqual(result['status'], 'rekeyed')
        self.assertEqual(result['new_key'], new_key)
        _, ckwargs = client.copy_object.call_args
        self.assertEqual(ckwargs['CopySource']['Key'], '999/888/file-1234567890abcdef.mp3')
        self.assertEqual(ckwargs['Key'], new_key)
        ep.refresh_from_db()
        self.assertEqual(ep.r2_url, f'https://audio.test/{new_key}')
        orphan = R2OrphanedObject.objects.get(key='999/888/file-1234567890abcdef.mp3')
        self.assertEqual(orphan.reason, R2OrphanedObject.Reason.MOVE_REKEY)

    def test_rekey_noop_when_key_matches_current_parent(self):
        from pod_manager.services import r2_maintenance
        key = f'{self.network.id}/{self.podcast.id}/file-1234567890abcdef.mp3'
        ep = self._episode(r2_url=f'https://audio.test/{key}')
        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            result = r2_maintenance.rekey_episode_audio(ep.id)
        self.assertEqual(result['status'], 'noop')
        client.copy_object.assert_not_called()

    # -- purge dev -----------------------------------------------------------
    def test_purge_dev_lists_with_prefix_and_deletes(self):
        from pod_manager.services import r2_maintenance
        objects = [('dev/1/1/a.mp3', timezone.now()), ('dev/2/2/b.mp3', timezone.now())]
        client = _fake_list_client(objects)
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            result = r2_maintenance.purge_dev_prefix()
        # listing was scoped to the dev/ prefix
        _, pkwargs = client.get_paginator.return_value.paginate.call_args
        self.assertEqual(pkwargs.get('Prefix'), 'dev/')
        self.assertEqual(result['deleted'], 2)
        client.delete_objects.assert_called_once()


class R2MoveRekeyHookTests(TestCase):
    """handle_move_episodes dispatches the rekey task only for moved episodes that
    actually have an r2_url, and only when R2_MIRROR_ENABLED."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='net')
        self.p1 = Podcast.objects.create(network=self.network, title='P1', slug='p1')
        self.p2 = Podcast.objects.create(network=self.network, title='P2', slug='p2')
        self.user = User.objects.create_user('mover', password='x')
        # Created under R2_MIRROR_ENABLED=False (default) -> no mirror signal.
        self.ep_mirrored = Episode.objects.create(
            podcast=self.p1, title='m', pub_date=timezone.now(), raw_description='x',
            clean_description='x', r2_url='https://audio.test/9/9/m-aaaaaaaaaaaaaaaa.mp3')
        self.ep_plain = Episode.objects.create(
            podcast=self.p1, title='p', pub_date=timezone.now(), raw_description='x',
            clean_description='x')

    @override_settings(R2_MIRROR_ENABLED=True)
    def test_move_dispatches_rekey_for_mirrored_only(self):
        from pod_manager.views.creator import actions
        factory = RequestFactory()
        req = factory.post('/', {
            'episode_ids': [self.ep_mirrored.id, self.ep_plain.id],
            'target_podcast_id': self.p2.id,
        })
        req.user = self.user
        with mock.patch('pod_manager.tasks.task_rekey_episode_audio.delay') as rekey, \
             mock.patch.object(actions, 'messages'), \
             mock.patch.object(actions, 'task_rebuild_episode_fragments'):
            actions.handle_move_episodes(req, self.network)

        # both episodes moved
        self.ep_mirrored.refresh_from_db()
        self.ep_plain.refresh_from_db()
        self.assertEqual(self.ep_mirrored.podcast_id, self.p2.id)
        self.assertEqual(self.ep_plain.podcast_id, self.p2.id)
        # only the mirrored one is re-keyed
        rekey_ids = {c.args[0] for c in rekey.call_args_list}
        self.assertEqual(rekey_ids, {self.ep_mirrored.id})

    @override_settings(R2_MIRROR_ENABLED=False)
    def test_move_skips_rekey_when_mirror_disabled(self):
        from pod_manager.views.creator import actions
        factory = RequestFactory()
        req = factory.post('/', {
            'episode_ids': [self.ep_mirrored.id],
            'target_podcast_id': self.p2.id,
        })
        req.user = self.user
        with mock.patch('pod_manager.tasks.task_rekey_episode_audio.delay') as rekey, \
             mock.patch.object(actions, 'messages'), \
             mock.patch.object(actions, 'task_rebuild_episode_fragments'):
            actions.handle_move_episodes(req, self.network)
        rekey.assert_not_called()


@override_settings(CACHES=TEST_CACHES)
class CommandLogStreamTests(TestCase):
    """Contract for the shared CommandLogStream buffer util (Admin Command Console,
    §8). Guards the streaming behavior the polled log callers (import_feed_poll,
    gdrive_recovery_poll) and the GDrive recovery/rewind tasks rely on."""

    def setUp(self):
        from pod_manager.admin_console.log_stream import CommandLogStream
        self.CommandLogStream = CommandLogStream
        cache.clear()

    def test_write_appends_sse_framed_lines_to_cache(self):
        stream = self.CommandLogStream('admin_cmd_test')
        stream.write("hello\nworld\n")
        self.assertEqual(cache.get('admin_cmd_test'), "data: hello\n\ndata: world\n\n")

    def test_captured_returns_raw_unframed_text(self):
        stream = self.CommandLogStream('admin_cmd_test')
        stream.write("line one\n")
        stream.write("line two\n")
        # captured() is the post-run parse/persist source — no SSE framing.
        self.assertEqual(stream.captured(), "line one\nline two\n")

    def test_done_sentinel_is_framed_for_sse_close(self):
        # The SSE views close when "[DONE]" appears in the tailed chunk; callers
        # write the sentinel themselves, so it must frame identically.
        stream = self.CommandLogStream('admin_cmd_test')
        stream.write('[DONE]')
        self.assertIn("[DONE]", cache.get('admin_cmd_test'))
        self.assertEqual(cache.get('admin_cmd_test'), "data: [DONE]\n\n")

    def test_empty_write_is_a_noop(self):
        stream = self.CommandLogStream('admin_cmd_test')
        self.assertEqual(stream.write(''), 0)
        self.assertIsNone(cache.get('admin_cmd_test'))
        self.assertEqual(stream.captured(), '')

    def test_blank_lines_produce_no_cache_chunk_but_are_captured(self):
        # splitlines() drops blank lines from the framed buffer, but the raw text
        # is still captured verbatim (matches the legacy _RecoveryStream behavior).
        stream = self.CommandLogStream('admin_cmd_test')
        stream.write("\n")
        self.assertIsNone(cache.get('admin_cmd_test'))
        self.assertEqual(stream.captured(), "\n")


# ===========================================================================
# Admin Command Console — Step 2 backend (design §13). Registry, introspection /
# widget resolver, the CommandRun model + runner task, and the superuser routes.
# ===========================================================================

class AdminConsoleSchemaTests(TestCase):
    """Registry + introspection/reconstruction contract (§4b/§5/§5a/§15)."""

    def _schema(self, name):
        from pod_manager.admin_console.schema import build_schema
        return build_schema(name)

    def _field(self, name, dest):
        for f in self._schema(name)['fields']:
            if f['dest'] == dest:
                return f
        self.fail(f"{dest!r} not in schema for {name}")

    def test_every_discovered_command_is_registered(self):
        # §4c self-policing: no pod_manager command should be silently unexposed.
        from pod_manager.admin_console.schema import unregistered_commands
        self.assertEqual(unregistered_commands(), [])

    def test_globals_are_hidden_from_the_form(self):
        dests = {f['dest'] for f in self._schema('mirror_audio_to_r2')['fields']}
        for hidden in ('help', 'verbosity', 'settings', 'pythonpath', 'no_color'):
            self.assertNotIn(hidden, dests)

    def test_widget_inference_conventional_dests(self):
        self.assertEqual(self._field('mirror_audio_to_r2', 'network')['widget'], 'network')
        self.assertEqual(self._field('mirror_audio_to_r2', 'podcast')['widget'], 'podcast')
        self.assertEqual(self._field('mirror_audio_to_r2', 'episode')['widget'], 'episode')
        self.assertEqual(self._field('mirror_audio_to_r2', 'apply')['widget'], 'flag')
        self.assertEqual(self._field('mirror_audio_to_r2', 'limit')['widget'], 'number')

    def test_podcast_multi_inference_from_append(self):
        # backfill_transcripts --podcast dest='podcasts' action='append'
        f = self._field('backfill_transcripts', 'podcasts')
        self.assertEqual(f['widget'], 'podcast_multi')
        self.assertTrue(f['multi'])

    def test_int_picker_ships_id_values_slug_picker_ships_slugs(self):
        # ingest_feed's podcast_id is type=int → option values must be ids (selecting a
        # slug crashes reconstruction); mirror's --podcast is a slug field → slug values.
        net = Network.objects.create(name='Bald Move', slug='baldmove')
        pod = Podcast.objects.create(network=net, title='Mr Robot', slug='mrrobot')
        id_field = self._field('ingest_feed', 'podcast_id')
        self.assertEqual([o['value'] for o in id_field['options']], [pod.id])
        slug_field = self._field('mirror_audio_to_r2', 'podcast')
        self.assertIn('mrrobot', [o['value'] for o in slug_field['options']])
        # And the id round-trips through reconstruction without crashing.
        inv = self._reconstruct('ingest_feed', {'podcast_id': pod.id})
        self.assertEqual(inv['args'], [pod.id])

    def test_choice_widget_ships_inline_options(self):
        f = self._field('recover_gdrive_audio', 'min_confidence')
        self.assertEqual(f['widget'], 'choice')
        self.assertEqual([o['value'] for o in f['options']], ['HIGH', 'MEDIUM', 'LOW'])

    def test_registry_override_enum_multi(self):
        f = self._field('mirror_audio_to_r2', 'origins')
        self.assertEqual(f['widget'], 'enum_multi:audio_origins')
        self.assertTrue(f['multi'])
        self.assertEqual([o['value'] for o in f['options']], ['gdrive', 'libsyn', 'other'])

    def test_registry_override_single_enum(self):
        # backfill_transcripts --model / --language are free-form args given curated
        # single-select enum pickers via field_widgets (§5a).
        model = self._field('backfill_transcripts', 'model')
        self.assertEqual(model['widget'], 'enum:whisper_models')
        self.assertFalse(model['multi'])
        self.assertIn('large-v3', [o['value'] for o in model['options']])
        lang = self._field('backfill_transcripts', 'language')
        self.assertEqual(lang['widget'], 'enum:whisper_languages')
        self.assertIn('es', [o['value'] for o in lang['options']])
        # A picked value round-trips through reconstruction as a plain --flag=value.
        inv = self._reconstruct('backfill_transcripts', {'apply': True, 'model': 'large', 'language': 'es'})
        self.assertEqual(inv['options']['model'], 'large')
        self.assertEqual(inv['options']['language'], 'es')

    def test_csv_path_override(self):
        self.assertEqual(self._field('recover_gdrive_audio', 'csv_path')['widget'], 'csv_path')

    def test_sensitive_field_flagged(self):
        schema = self._schema('crawl_by_id')
        self.assertEqual(schema['sensitive'], ['cookie_value'])
        self.assertTrue(self._field('crawl_by_id', 'cookie_value')['sensitive'])
        self.assertFalse(self._field('crawl_by_id', 'cookie_name')['sensitive'])

    def test_docs_come_from_in_code_sources(self):
        schema = self._schema('mirror_audio_to_r2')
        self.assertTrue(schema['summary'])               # Command.help
        self.assertIn('Mirror', schema['long_doc'])      # module docstring

    def test_import_error_surfaces_as_disabled_card(self):
        # §15.8: one un-importable command must not break the console.
        from pod_manager.admin_console import schema as schema_mod
        with mock.patch.object(schema_mod, 'load_command_class', side_effect=ImportError('no discord')):
            schema = schema_mod.build_schema('run_discord_bot')
        self.assertIn('no discord', schema['import_error'])
        self.assertEqual(schema['fields'], [])

    # -- reconstruction (form → invocation) ---------------------------------
    def _reconstruct(self, name, payload):
        from pod_manager.admin_console.schema import reconstruct_invocation
        return reconstruct_invocation(name, payload)

    def test_reconstruct_coerces_int_positional(self):
        inv = self._reconstruct('ingest_feed', {'podcast_id': '42'})
        self.assertEqual(inv['args'], [42])

    def test_reconstruct_missing_required_positional_raises(self):
        from pod_manager.admin_console.schema import InvalidInvocation
        with self.assertRaises(InvalidInvocation):
            self._reconstruct('ingest_feed', {})

    def test_reconstruct_revalidates_choice(self):
        from pod_manager.admin_console.schema import InvalidInvocation
        with self.assertRaises(InvalidInvocation):
            self._reconstruct('recover_gdrive_audio', {'csv_path': 'x.csv', 'min_confidence': 'BOGUS'})

    def test_reconstruct_enum_multi_comma_joins(self):
        inv = self._reconstruct('mirror_audio_to_r2', {'origins': ['gdrive', 'libsyn'], 'apply': True})
        self.assertEqual(inv['options']['origins'], 'gdrive,libsyn')
        self.assertIs(inv['options']['apply'], True)

    def test_reconstruct_nargs_plus_multi_positional(self):
        inv = self._reconstruct('rewind_gdrive_audio', {'csv_paths': ['a.csv', 'b.csv'], 'apply': True})
        self.assertEqual(inv['args'], ['a.csv', 'b.csv'])

    def test_reconstruct_redacts_sensitive(self):
        inv = self._reconstruct('crawl_by_id', {
            'cookie_name': 'wp', 'cookie_value': 'SECRET', 'start': 34, 'end': 50,
        })
        # Real invocation keeps the value; persisted/displayed copies are redacted.
        self.assertEqual(inv['options']['cookie_value'], 'SECRET')
        self.assertEqual(inv['redacted_options']['cookie_value'], '***')
        self.assertNotIn('SECRET', inv['command_line'])
        self.assertIn('***', inv['command_line'])

    def test_command_line_is_shell_quoted_for_copy_paste(self):
        # Recovery CSVs have spaces; the paste-ready command line must quote them so a
        # deep-link command (recover_gdrive_audio) copies into a terminal correctly (§5b).
        inv = self._reconstruct('recover_gdrive_audio', {
            'csv_path': 'Vecto Recovery Links.csv', 'podcast_title': 'Watchmen', 'apply': True,
        })
        self.assertIn("'Vecto Recovery Links.csv'", inv['command_line'])
        # Reassembled args stay unquoted (they go to call_command, not a shell).
        self.assertEqual(inv['args'], ['Vecto Recovery Links.csv', 'Watchmen'])


class AdminConsoleViewTests(TestCase):
    """Superuser routes + dispatch/poll/history (§3/§7/§9)."""

    def setUp(self):
        cache.clear()
        self.superuser = User.objects.create_user('root', password='x')
        self.superuser.is_superuser = True
        self.superuser.is_staff = True
        self.superuser.save()
        self.staff = User.objects.create_user('mod', password='x')
        self.staff.is_staff = True
        self.staff.save()
        self.client = Client()

    # -- access control -----------------------------------------------------
    def test_anonymous_redirected_to_login(self):
        resp = self.client.get(reverse('admin_console'))
        self.assertEqual(resp.status_code, 302)

    def test_staff_nonsuper_forbidden(self):
        self.client.force_login(self.staff)
        self.assertEqual(self.client.get(reverse('admin_console')).status_code, 403)

    def test_superuser_allowed(self):
        self.client.force_login(self.superuser)
        self.assertEqual(self.client.get(reverse('admin_console')).status_code, 200)

    # -- console list -------------------------------------------------------
    def test_console_groups_commands_and_reports_no_unregistered(self):
        self.client.force_login(self.superuser)
        resp = self.client.get(reverse('admin_console'))
        self.assertTemplateUsed(resp, 'pod_manager/admin_console.html')
        cats = {c['category'] for c in resp.context['categories']}
        self.assertIn('R2 / Storage', cats)
        self.assertEqual(resp.context['unregistered'], [])
        # Every discovered command is registered, so the counts line up — avoids a
        # brittle hardcoded total each time a command is added.
        from pod_manager.admin_console.registry import REGISTRY
        self.assertEqual(resp.context['discovered_count'], len(REGISTRY))

    # -- detail -------------------------------------------------------------
    def test_command_detail_returns_schema(self):
        self.client.force_login(self.superuser)
        data = self.client.get(reverse('admin_console_command_detail', args=['mirror_audio_to_r2'])).json()
        self.assertTrue(data['runnable'])
        self.assertIn('recent_runs', data)

    def test_command_detail_unregistered_404(self):
        self.client.force_login(self.superuser)
        resp = self.client.get(reverse('admin_console_command_detail', args=['nope']))
        self.assertEqual(resp.status_code, 404)

    # -- run dispatch -------------------------------------------------------
    def _run(self, name, body):
        return self.client.post(
            reverse('admin_console_run', args=[name]),
            data=json.dumps(body), content_type='application/json',
        )

    def test_run_rejects_non_runnable(self):
        self.client.force_login(self.superuser)
        for name in ('run_discord_bot', 'crawl_by_id'):
            self.assertEqual(self._run(name, {'fields': {}}).status_code, 403)

    def test_run_rejects_deep_link_command(self):
        self.client.force_login(self.superuser)
        self.assertEqual(self._run('recover_gdrive_audio', {'fields': {'csv_path': 'x.csv'}}).status_code, 403)

    def test_run_invalid_args_400(self):
        self.client.force_login(self.superuser)
        resp = self._run('ingest_feed', {'fields': {}})
        self.assertEqual(resp.status_code, 400)

    def test_run_danger_requires_confirmation(self):
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            resp = self._run('purge_r2_dev', {'fields': {'apply': True, 'yes': True}})
            self.assertEqual(resp.status_code, 400)
            delay.assert_not_called()

    def test_run_creates_commandrun_and_dispatches(self):
        from pod_manager.models import CommandRun
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            resp = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
        self.assertEqual(resp.status_code, 200)
        run_id = resp.json()['run_id']
        run = CommandRun.objects.get(run_id=run_id)
        self.assertEqual(run.command, 'ingest_feed')
        self.assertEqual(run.status, CommandRun.Status.QUEUED)
        self.assertEqual(run.user, self.superuser)
        delay.assert_called_once()
        # The task receives the *real* coerced invocation.
        _rid, name, args, options = delay.call_args.args
        self.assertEqual(name, 'ingest_feed')
        self.assertEqual(args, [7])

    def test_run_danger_dispatches_when_confirmed(self):
        from pod_manager.models import CommandRun
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            resp = self._run('purge_r2_dev', {
                'fields': {'apply': True, 'yes': True}, 'confirm': 'purge_r2_dev',
            })
        self.assertEqual(resp.status_code, 200)
        delay.assert_called_once()
        self.assertEqual(CommandRun.objects.filter(command='purge_r2_dev').count(), 1)

    def test_danger_preview_skips_confirmation(self):
        # purge_r2_dev is always-danger, but a preview (no --apply) mutates nothing,
        # so it dispatches without the typed confirm — the gate is only for real applies.
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            resp = self._run('purge_r2_dev', {'fields': {}})
        self.assertEqual(resp.status_code, 200)
        delay.assert_called_once()

    def test_prune_subfield_triggers_dynamic_danger_gate(self):
        # backfill_media_to_r2 is benign by default but --prune deletes, so a pruning
        # run needs the typed confirm while a plain backfill does not (danger_fields).
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            # Plain backfill: no confirm required.
            ok = self._run('backfill_media_to_r2', {'fields': {'all': True, 'apply': True}})
            self.assertEqual(ok.status_code, 200)
            # --prune without confirm is blocked…
            blocked = self._run('backfill_media_to_r2',
                                {'fields': {'all': True, 'apply': True, 'yes': True, 'prune': True}})
            self.assertEqual(blocked.status_code, 400)
            # …and allowed once confirmed.
            confirmed = self._run('backfill_media_to_r2', {
                'fields': {'all': True, 'apply': True, 'yes': True, 'prune': True},
                'confirm': 'backfill_media_to_r2',
            })
            self.assertEqual(confirmed.status_code, 200)
        self.assertEqual(delay.call_count, 2)  # plain + confirmed-prune dispatched; blocked did not

    def test_run_soft_blocks_identical_in_flight_run(self):
        # §14 (resolved): a second identical invocation while one is queued/running and
        # CONFIRMED ALIVE is rejected (409); a *different* invocation still dispatches.
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            delay.return_value.id = 'live-task-1'
            first = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
            self.assertEqual(first.status_code, 200)
            # The blocker's task id shows up as live -> the duplicate is rejected.
            with mock.patch('pod_manager.views.admin_console._live_task_ids',
                            return_value=({'live-task-1'}, True)):
                dup = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
                self.assertEqual(dup.status_code, 409)
            other = self._run('ingest_feed', {'fields': {'podcast_id': '8'}})
            self.assertEqual(other.status_code, 200)
        self.assertEqual(delay.call_count, 2)  # first + other; the duplicate never dispatched

    def test_run_self_heals_stale_run(self):
        # A queued/running row whose worker died (task id not among live tasks) is
        # auto-cleared so the command isn't blocked forever; the new run dispatches.
        from pod_manager.models import CommandRun
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            delay.return_value.id = 'dead-task'
            first = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
            self.assertEqual(first.status_code, 200)
            stale = CommandRun.objects.get(run_id=first.json()['run_id'])
            # inspect responds but the task isn't running anywhere -> stale.
            with mock.patch('pod_manager.views.admin_console._live_task_ids',
                            return_value=(set(), True)):
                again = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
            self.assertEqual(again.status_code, 200)
        stale.refresh_from_db()
        self.assertEqual(stale.status, CommandRun.Status.FAILED)
        self.assertIn('auto-cleared', stale.error)
        # The fresh run exists and is distinct from the cleared one.
        live = CommandRun.objects.get(run_id=again.json()['run_id'])
        self.assertEqual(live.status, CommandRun.Status.QUEUED)

    def test_run_blocks_when_worker_unreachable(self):
        # If no worker answers inspect we can't prove the blocker is dead, so we stay
        # conservative and block (the operator can Cancel to force-clear).
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay') as delay:
            delay.return_value.id = 'unknown-task'
            self.assertEqual(self._run('ingest_feed', {'fields': {'podcast_id': '7'}}).status_code, 200)
            with mock.patch('pod_manager.views.admin_console._live_task_ids',
                            return_value=(set(), False)):
                dup = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
            self.assertEqual(dup.status_code, 409)

    # -- cancel -------------------------------------------------------------
    def _cancel(self, run_id):
        return self.client.post(reverse('admin_console_run_cancel', args=[str(run_id)]))

    def test_cancel_revokes_live_task_and_clears_row(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(
            command='backfill_transcripts_to_r2', status=CommandRun.Status.RUNNING,
            celery_task_id='task-xyz',
        )
        self.client.force_login(self.superuser)
        with mock.patch('celery.app.control.Control.revoke') as revoke:
            resp = self._cancel(run.run_id)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()['revoked'])
        revoke.assert_called_once_with('task-xyz', terminate=True)
        run.refresh_from_db()
        self.assertEqual(run.status, CommandRun.Status.FAILED)
        self.assertIn('cancelled by', run.error)

    def test_cancel_zombie_row_without_task_clears_without_revoke(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(command='ingest_feed', status=CommandRun.Status.RUNNING)
        self.client.force_login(self.superuser)
        resp = self._cancel(run.run_id)
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json()['revoked'])
        run.refresh_from_db()
        self.assertEqual(run.status, CommandRun.Status.FAILED)

    def test_cancel_terminal_run_is_409(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(command='ingest_feed', status=CommandRun.Status.COMPLETED)
        self.client.force_login(self.superuser)
        self.assertEqual(self._cancel(run.run_id).status_code, 409)

    def test_cancel_missing_run_is_404(self):
        import uuid as _uuid
        self.client.force_login(self.superuser)
        self.assertEqual(self._cancel(_uuid.uuid4()).status_code, 404)

    def test_cancel_requires_superuser(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(command='ingest_feed', status=CommandRun.Status.RUNNING)
        resp = self._cancel(run.run_id)
        self.assertIn(resp.status_code, (302, 403))

    def test_run_503_and_drops_row_when_broker_down(self):
        # If `.delay()` raises (broker unreachable), the run degrades to a 503 and leaves
        # no orphan `queued` row behind (§7).
        from pod_manager.models import CommandRun
        self.client.force_login(self.superuser)
        with mock.patch('pod_manager.tasks.task_run_management_command.delay',
                        side_effect=RuntimeError('broker down')) as delay:
            resp = self._run('ingest_feed', {'fields': {'podcast_id': '7'}})
        self.assertEqual(resp.status_code, 503)
        delay.assert_called_once()
        self.assertEqual(CommandRun.objects.filter(command='ingest_feed').count(), 0)

    # -- build (copy-box serializer, §5b) -----------------------------------
    def _build(self, name, body):
        return self.client.post(
            reverse('admin_console_build', args=[name]),
            data=json.dumps(body), content_type='application/json',
        )

    def test_build_returns_quoted_command_line_for_deep_link_command(self):
        self.client.force_login(self.superuser)
        data = self._build('recover_gdrive_audio', {
            'fields': {'csv_path': 'Vecto Recovery Links.csv', 'apply': True},
        }).json()
        self.assertTrue(data['valid'])
        self.assertIn("recover_gdrive_audio 'Vecto Recovery Links.csv' --apply", data['command_line'])

    def test_build_incomplete_form_is_soft_invalid(self):
        self.client.force_login(self.superuser)
        data = self._build('recover_gdrive_audio', {'fields': {}}).json()
        self.assertFalse(data['valid'])
        self.assertIsNone(data['command_line'])
        self.assertTrue(data['error'])

    # -- poll / detail / history -------------------------------------------
    def test_run_poll_returns_delta_and_status(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(command='ingest_feed', status=CommandRun.Status.RUNNING)
        cache.set(f"admin_cmd_{run.run_id}", "data: hello\n\n")
        self.client.force_login(self.superuser)
        url = reverse('admin_console_run_poll', args=[str(run.run_id)])
        data = self.client.get(url).json()
        self.assertEqual(data['chunk'], "data: hello\n\n")
        self.assertEqual(data['status'], 'running')
        # Polling again from the returned offset yields no duplicate output.
        data2 = self.client.get(url, {'offset': data['offset']}).json()
        self.assertEqual(data2['chunk'], '')

    def test_run_detail_includes_log(self):
        from pod_manager.models import CommandRun
        run = CommandRun.objects.create(command='ingest_feed', status=CommandRun.Status.COMPLETED, log='all output')
        self.client.force_login(self.superuser)
        data = self.client.get(reverse('admin_console_run_detail', args=[str(run.run_id)])).json()
        self.assertEqual(data['log'], 'all output')

    def test_history_filters_by_command(self):
        from pod_manager.models import CommandRun
        CommandRun.objects.create(command='ingest_feed')
        CommandRun.objects.create(command='prune_logs')
        self.client.force_login(self.superuser)
        data = self.client.get(reverse('admin_console_history'), {'command': 'prune_logs'}).json()
        self.assertEqual(len(data['runs']), 1)
        self.assertEqual(data['runs'][0]['command'], 'prune_logs')

    def test_episode_search_typeahead(self):
        net = Network.objects.create(name='Bald Move', slug='baldmove')
        pod = Podcast.objects.create(network=net, title='Watchmen', slug='watchmen')
        ep = Episode.objects.create(
            podcast=pod, title='The Pilot', pub_date=timezone.now(), raw_description='x',
        )
        self.client.force_login(self.superuser)
        data = self.client.get(reverse('admin_console_episode_search'), {'q': 'Pilot'}).json()
        self.assertEqual([r['id'] for r in data['results']], [ep.id])
        # Empty query returns nothing rather than the whole table.
        self.assertEqual(self.client.get(reverse('admin_console_episode_search')).json()['results'], [])


class TaskRunManagementCommandTests(TestCase):
    """The generic runner task records lifecycle + log on CommandRun (§7/§8a)."""

    def setUp(self):
        cache.clear()

    def test_happy_path_records_completed_and_streams_done(self):
        from pod_manager.models import CommandRun
        from pod_manager.tasks import task_run_management_command
        run = CommandRun.objects.create(command='ingest_feed', command_line='python manage.py ingest_feed 7')

        def fake_call(name, *args, stdout=None, **kw):
            stdout.write("did the thing\n")

        with mock.patch('pod_manager.tasks.call_command', side_effect=fake_call):
            task_run_management_command(str(run.run_id), 'ingest_feed', [7], {})

        run.refresh_from_db()
        self.assertEqual(run.status, CommandRun.Status.COMPLETED)
        self.assertIn('did the thing', run.log)
        self.assertIsNotNone(run.started_at)
        self.assertIsNotNone(run.finished_at)
        self.assertIn('[DONE]', cache.get(f"admin_cmd_{run.run_id}"))

    def test_failure_records_failed_and_reraises(self):
        from pod_manager.models import CommandRun
        from pod_manager.tasks import task_run_management_command
        run = CommandRun.objects.create(command='ingest_feed', command_line='python manage.py ingest_feed 7')

        with mock.patch('pod_manager.tasks.call_command', side_effect=RuntimeError('boom')):
            with self.assertRaises(RuntimeError):
                task_run_management_command(str(run.run_id), 'ingest_feed', [7], {})

        run.refresh_from_db()
        self.assertEqual(run.status, CommandRun.Status.FAILED)
        self.assertIn('boom', run.error)
        self.assertIn('[DONE]', cache.get(f"admin_cmd_{run.run_id}"))

    def test_command_emitted_summary_lands_in_result_summary(self):
        # A command that emits a [SUMMARY] line has it sliced into result_summary (§8a).
        from pod_manager.models import CommandRun
        from pod_manager.tasks import task_run_management_command
        from pod_manager.admin_console.summary import emit_summary
        run = CommandRun.objects.create(command='mirror_audio_to_r2', command_line='x')

        def fake_call(name, *args, stdout=None, **kw):
            stdout.write("Dispatched 5 mirror task(s) to Celery.\n")
            emit_summary(stdout, {"mode": "celery", "dispatched": 5})

        with mock.patch('pod_manager.tasks.call_command', side_effect=fake_call):
            task_run_management_command(str(run.run_id), 'mirror_audio_to_r2', [], {})

        run.refresh_from_db()
        self.assertEqual(run.result_summary, {"mode": "celery", "dispatched": 5})

    def test_no_summary_leaves_result_summary_null(self):
        from pod_manager.models import CommandRun
        from pod_manager.tasks import task_run_management_command
        run = CommandRun.objects.create(command='ingest_feed', command_line='x')

        with mock.patch('pod_manager.tasks.call_command', side_effect=lambda *a, **k: k['stdout'].write("plain output\n")):
            task_run_management_command(str(run.run_id), 'ingest_feed', [], {})

        run.refresh_from_db()
        self.assertIsNone(run.result_summary)


class CommandSummaryHelperTests(TestCase):
    """The command-emitted [SUMMARY] convention (admin_console/summary.py, §8a)."""

    def test_emit_then_extract_round_trips(self):
        import io
        from pod_manager.admin_console.summary import emit_summary, extract_summary
        buf = io.StringIO()
        emit_summary(buf, {"deleted": 12, "kept": 3})
        self.assertEqual(extract_summary(buf.getvalue()), {"deleted": 12, "kept": 3})

    def test_last_summary_wins(self):
        from pod_manager.admin_console.summary import extract_summary
        captured = '[SUMMARY] {"n": 1}\nworking...\n[SUMMARY] {"n": 2}\n'
        self.assertEqual(extract_summary(captured), {"n": 2})

    def test_malformed_or_missing_yields_none(self):
        from pod_manager.admin_console.summary import extract_summary
        self.assertIsNone(extract_summary("just regular output\n"))
        self.assertIsNone(extract_summary("[SUMMARY] not-json\n"))
        self.assertIsNone(extract_summary(""))

    def test_real_command_emits_parseable_summary(self):
        # End-to-end guard: a real command's emit_summary line round-trips through
        # extract_summary. prune_logs preview deletes nothing, so it's safe.
        import io
        from django.core.management import call_command
        from pod_manager.admin_console.summary import extract_summary
        buf = io.StringIO()
        call_command('prune_logs', stdout=buf)
        summary = extract_summary(buf.getvalue())
        self.assertIsNotNone(summary)
        self.assertIs(summary['applied'], False)
        self.assertIn('deleted', summary)


@override_settings(CACHES=TEST_CACHES)
class LiveImportPollTests(TestCase):
    """The polled feed-import endpoints (migrated from SSE → polling)."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.network = Network.objects.create(name='Bald Move', slug='baldmove')
        self.podcast = Podcast.objects.create(network=self.network, title='Watchmen', slug='watchmen')
        self.user = User.objects.create_user('creator', password='x')

    def test_poll_returns_delta_and_done(self):
        from pod_manager.views import import_feed_poll
        cache.set(f"import_logs_{self.podcast.id}", "data: hello\n\ndata: [DONE]\n\n")
        req = _make_tenant_request(self.factory, self.network, path='/import/poll/', user=self.user)
        data = json.loads(import_feed_poll(req, self.podcast.id).content)
        self.assertIn('hello', data['chunk'])
        self.assertTrue(data['done'])
        # Re-poll from the returned offset → no duplicate output.
        req2 = _make_tenant_request(self.factory, self.network, path='/import/poll/',
                                    data={'offset': data['offset']}, user=self.user)
        self.assertEqual(json.loads(import_feed_poll(req2, self.podcast.id).content)['chunk'], '')

    def test_start_seeds_buffer_and_dispatches(self):
        from pod_manager.views import import_feed_start
        with mock.patch('pod_manager.tasks.task_ingest_feed.delay') as delay:
            req = _make_tenant_request(self.factory, self.network, method='post',
                                       path='/import/start/', user=self.user)
            resp = import_feed_start(req, self.podcast.id)
        self.assertEqual(resp.status_code, 200)
        delay.assert_called_once_with(self.podcast.id)
        self.assertIn('[QUEUED]', cache.get(f"import_logs_{self.podcast.id}"))


class TaskSmartPollFeedsTests(TestCase):
    """task_smart_poll_feeds(): normal-priority podcasts dispatch immediately,
    low-priority podcasts are staggered via a 10-minute countdown."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netpoll')
        self.normal = Podcast.objects.create(network=self.network, title='Normal', slug='normal-poll')
        self.low = Podcast.objects.create(network=self.network, title='Low', slug='low-poll')
        self.low.is_low_priority = True
        self.low.save()
        Episode.objects.create(
            podcast=self.normal, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        Episode.objects.create(
            podcast=self.low, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )

    def test_normal_priority_dispatched_immediately(self):
        from pod_manager.tasks import task_smart_poll_feeds
        with mock.patch('pod_manager.tasks.task_ingest_feed.delay') as delay, \
             mock.patch('pod_manager.tasks.task_ingest_feed.apply_async') as apply_async:
            task_smart_poll_feeds()
        delay.assert_any_call(self.normal.id)
        for call in apply_async.call_args_list:
            self.assertNotEqual(call.kwargs.get('args'), [self.normal.id])

    def test_low_priority_staggered_with_countdown(self):
        from pod_manager.tasks import task_smart_poll_feeds
        with mock.patch('pod_manager.tasks.task_ingest_feed.delay') as delay, \
             mock.patch('pod_manager.tasks.task_ingest_feed.apply_async') as apply_async:
            task_smart_poll_feeds()
        apply_async.assert_any_call(args=[self.low.id], countdown=600)
        for call in delay.call_args_list:
            self.assertNotEqual(call.args, (self.low.id,))


@override_settings(CACHES=TEST_CACHES)
class GdriveRecoveryPollTests(TestCase):
    """The polled GDrive-recovery log endpoint (migrated from SSE → polling)."""

    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.user = User.objects.create_user('creator', password='x')

    def test_poll_returns_delta_and_rejects_bad_uuid(self):
        from pod_manager.views import gdrive_recovery_poll
        import uuid as _uuid
        rid = str(_uuid.uuid4())
        cache.set(f"gdrive_recovery_{rid}", "data: working\n\n")
        req = _make_tenant_request(self.factory, None, path='/creator/gdrive-recovery/poll/', user=self.user)
        data = json.loads(gdrive_recovery_poll(req, rid).content)
        self.assertIn('working', data['chunk'])
        self.assertFalse(data['done'])
        self.assertEqual(gdrive_recovery_poll(req, 'not-a-uuid').status_code, 400)


class _FakeEntry(dict):
    """Minimal feedparser-FeedParserDict stand-in: dict (so .get() works) plus
    attribute access (so getattr(entry, 'id', None) works, as the real ingester
    code paths use both depending on the field)."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class ExtractSeasonEpisodeTests(SimpleTestCase):
    """extract_season_episode(): pure parsing, no DB needed."""

    def test_reads_season_episode_type(self):
        from pod_manager.ingesters.default import extract_season_episode
        entry = _FakeEntry(itunes_season='5', itunes_episode='3', itunes_episodetype='full')
        self.assertEqual(extract_season_episode(entry), (5, 3, 'full'))

    def test_missing_fields_return_none_and_empty_string(self):
        from pod_manager.ingesters.default import extract_season_episode
        self.assertEqual(extract_season_episode(_FakeEntry()), (None, None, ''))

    def test_non_numeric_values_become_none(self):
        from pod_manager.ingesters.default import extract_season_episode
        entry = _FakeEntry(itunes_season='S5', itunes_episode='')
        self.assertEqual(extract_season_episode(entry), (None, None, ''))

    def test_type_is_truncated_to_50_chars(self):
        from pod_manager.ingesters.default import extract_season_episode
        entry = _FakeEntry(itunes_episodetype='x' * 60)
        _, _, etype = extract_season_episode(entry)
        self.assertEqual(len(etype), 50)


class ExtractExplicitTests(SimpleTestCase):
    """extract_explicit(): tri-state parse of itunes:explicit."""

    def test_modern_true_false(self):
        from pod_manager.ingesters.default import extract_explicit
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='true')), True)
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='false')), False)

    def test_legacy_spellings(self):
        from pod_manager.ingesters.default import extract_explicit
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='yes')), True)
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='explicit')), True)
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='clean')), False)
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit='no')), False)

    def test_real_bool_passthrough(self):
        from pod_manager.ingesters.default import extract_explicit
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit=True)), True)
        self.assertIs(extract_explicit(_FakeEntry(itunes_explicit=False)), False)

    def test_missing_or_unknown_returns_none(self):
        from pod_manager.ingesters.default import extract_explicit
        self.assertIsNone(extract_explicit(_FakeEntry()))
        self.assertIsNone(extract_explicit(_FakeEntry(itunes_explicit='')))
        self.assertIsNone(extract_explicit(_FakeEntry(itunes_explicit='maybe')))


class FeedTagExtractionTests(SimpleTestCase):
    """extract_feed_tags() + merge_tags(): dedup and ordering."""

    def test_extracts_and_dedupes_terms(self):
        from pod_manager.ingesters.default import extract_feed_tags
        entry = _FakeEntry(tags=[{'term': 'Drama'}, {'term': ' Comedy '}, {'term': 'drama'}, {'term': ''}])
        self.assertEqual(extract_feed_tags(entry), ['Drama', 'Comedy'])

    def test_no_tags_attr_returns_empty(self):
        from pod_manager.ingesters.default import extract_feed_tags
        self.assertEqual(extract_feed_tags(_FakeEntry()), [])
        self.assertEqual(extract_feed_tags(None), [])

    def test_merge_tags_case_insensitive_first_wins(self):
        from pod_manager.ingesters.default import merge_tags
        self.assertEqual(
            merge_tags(['Drama', 'Comedy'], ['comedy', 'Sci-Fi'], None),
            ['Drama', 'Comedy', 'Sci-Fi'],
        )


class ManageEpisodeUploadAudioTests(TestCase):
    """manage_episode's upload_audio action: owner gate, validation, R2 mirror
    call shape, transcript reset, and transcription dispatch."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netupload')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='showupload')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        self.owner = User.objects.create_user('owner', password='x')
        self.network.owners.add(self.owner)
        self.other = User.objects.create_user('other', password='x')
        self.url = reverse('manage_episode', args=[self.episode.id])

    def _upload(self, user, filename='ep.mp3', content=b'fake-bytes'):
        self.client.force_login(user)
        f = SimpleUploadedFile(filename, content, content_type='audio/mpeg')
        return self.client.post(self.url, {'action': 'upload_audio', 'audio_file': f})

    def test_non_owner_forbidden(self):
        resp = self._upload(self.other)
        self.assertEqual(resp.status_code, 403)
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.audio_url_subscriber)

    def test_no_file_shows_error(self):
        self.client.force_login(self.owner)
        resp = self.client.post(self.url, {'action': 'upload_audio'})
        msgs = [str(m) for m in get_messages(resp.wsgi_request)]
        self.assertTrue(any('No audio file' in m for m in msgs))

    def test_bad_extension_rejected_before_mirroring(self):
        with mock.patch('pod_manager.services.r2_mirror.mirror_episode_audio') as mocked:
            self._upload(self.owner, filename='ep.txt')
        mocked.assert_not_called()

    def test_success_sets_subscriber_url_and_queues_transcription(self):
        with mock.patch(
            'pod_manager.services.r2_mirror.mirror_episode_audio',
            return_value={'status': 'mirrored', 'r2_url': 'https://r2.example.com/ep.mp3', 'key': 'k', 'reason': ''},
        ) as mocked_mirror, \
             mock.patch('pod_manager.services.transcription.dispatch_transcription') as mocked_dispatch, \
             mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            resp = self._upload(self.owner)

        self.assertEqual(resp.status_code, 302)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.audio_url_subscriber, 'https://r2.example.com/ep.mp3')

        args, kwargs = mocked_mirror.call_args
        self.assertEqual(args[0], self.episode.id)
        self.assertTrue(kwargs.get('manual'))
        self.assertTrue(kwargs.get('force'))

        mocked_dispatch.assert_called_once_with(self.episode.id)
        transcript = Transcript.objects.get(episode=self.episode)
        self.assertEqual(transcript.status, Transcript.Status.PENDING)

    def test_resets_failed_transcript_instead_of_orphaning(self):
        transcript = Transcript.objects.create(
            episode=self.episode, status=Transcript.Status.FAILED, error_message='boom',
        )
        with mock.patch(
            'pod_manager.services.r2_mirror.mirror_episode_audio',
            return_value={'status': 'mirrored', 'r2_url': 'https://r2.example.com/ep.mp3', 'key': 'k', 'reason': ''},
        ), mock.patch('pod_manager.services.transcription.dispatch_transcription'), \
             mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            self._upload(self.owner)

        transcript.refresh_from_db()
        self.assertEqual(transcript.status, Transcript.Status.PENDING)
        self.assertIsNone(transcript.error_message)

    def test_awaiting_recovery_transcript_also_reset(self):
        transcript = Transcript.objects.create(
            episode=self.episode, status=Transcript.Status.AWAITING_RECOVERY,
        )
        with mock.patch(
            'pod_manager.services.r2_mirror.mirror_episode_audio',
            return_value={'status': 'mirrored', 'r2_url': 'https://r2.example.com/ep.mp3', 'key': 'k', 'reason': ''},
        ), mock.patch('pod_manager.services.transcription.dispatch_transcription'), \
             mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            self._upload(self.owner)

        transcript.refresh_from_db()
        self.assertEqual(transcript.status, Transcript.Status.PENDING)

    def test_mirror_skipped_shows_error_and_leaves_episode_untouched(self):
        from pod_manager.services.r2_mirror import MirrorSkipped
        with mock.patch(
            'pod_manager.services.r2_mirror.mirror_episode_audio', side_effect=MirrorSkipped('nope'),
        ):
            resp = self._upload(self.owner)
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.audio_url_subscriber)
        msgs = [str(m) for m in get_messages(resp.wsgi_request)]
        self.assertTrue(any('Upload rejected' in m for m in msgs))

    def _post_explicit(self, user, value):
        self.client.force_login(user)
        with mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            return self.client.post(self.url, {'action': 'update_explicit', 'explicit': value})

    def test_update_explicit_true_false_inherit(self):
        self._post_explicit(self.owner, 'true')
        self.episode.refresh_from_db()
        self.assertIs(self.episode.explicit, True)

        self._post_explicit(self.owner, 'false')
        self.episode.refresh_from_db()
        self.assertIs(self.episode.explicit, False)

        self._post_explicit(self.owner, '')
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.explicit)

    def test_update_explicit_forbidden_for_non_owner(self):
        resp = self._post_explicit(self.other, 'true')
        self.assertEqual(resp.status_code, 403)
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.explicit)


class ManageEpisodeMoveEpisodeTests(TestCase):
    """manage_episode's move_episode action: owner gate, successful move via
    services.episode_move, no-op when target == current podcast."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netmove')
        self.podcast_a = Podcast.objects.create(network=self.network, title='Show A', slug='showa-move')
        self.podcast_b = Podcast.objects.create(network=self.network, title='Show B', slug='showb-move')
        self.episode = Episode.objects.create(
            podcast=self.podcast_a, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )
        self.owner = User.objects.create_user('owner', password='x')
        self.network.owners.add(self.owner)
        self.other = User.objects.create_user('other', password='x')
        self.url = reverse('manage_episode', args=[self.episode.id])

    def _move(self, user, target_podcast_id):
        self.client.force_login(user)
        return self.client.post(self.url, {'action': 'move_episode', 'target_podcast_id': target_podcast_id})

    def test_non_owner_forbidden(self):
        resp = self._move(self.other, self.podcast_b.id)
        self.assertEqual(resp.status_code, 403)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.podcast_a.id)

    def test_successful_move(self):
        resp = self._move(self.owner, self.podcast_b.id)
        self.assertEqual(resp.status_code, 302)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.podcast_b.id)
        self.assertIsNotNone(self.episode.podcast_pinned_at)
        self.assertEqual(self.episode.podcast_pinned_by_id, self.owner.id)
        msgs = [str(m) for m in get_messages(resp.wsgi_request)]
        self.assertTrue(any('moved to "Show B"' in m for m in msgs))

    def test_noop_when_target_is_current_podcast(self):
        resp = self._move(self.owner, self.podcast_a.id)
        self.assertEqual(resp.status_code, 302)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.podcast_a.id)
        self.assertIsNone(self.episode.podcast_pinned_at)
        msgs = [str(m) for m in get_messages(resp.wsgi_request)]
        self.assertTrue(any('already in that feed' in m for m in msgs))


class CommitEpisodeSeasonNumberTests(TestCase):
    """commit_episode(): season/episode/type applied when unlocked, left alone
    (and logged) when locked."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='net2')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='show2')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-1',
        )

    def _entry(self, **kw):
        base = dict(id='guid-1', title='Ep 1', itunes_season='5', itunes_episode='3', itunes_episodetype='full')
        base.update(kw)
        return _FakeEntry(**base)

    def test_unlocked_episode_gets_season_episode(self):
        from pod_manager.ingesters.default import commit_episode
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, self._entry(), None, 'Test', mock.Mock())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 5)
        self.assertEqual(self.episode.episode_number, 3)
        self.assertEqual(self.episode.episode_type, 'full')

    def test_locked_episode_not_overwritten(self):
        self.episode.is_metadata_locked = True
        self.episode.season_number = 1
        self.episode.episode_number = 1
        self.episode.save(update_fields=['is_metadata_locked', 'season_number', 'episode_number'])
        from pod_manager.ingesters.default import commit_episode
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, self._entry(), None, 'Test', mock.Mock())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 1)
        self.assertEqual(self.episode.episode_number, 1)

    def test_locked_episode_logs_would_be_values(self):
        self.episode.is_metadata_locked = True
        self.episode.save(update_fields=['is_metadata_locked'])
        from pod_manager.ingesters.default import commit_episode
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'), \
             self.assertLogs('pod_manager.ingesters.default', level='INFO') as cm:
            commit_episode(self.podcast, self._entry(), None, 'Test', mock.Mock())
        # New format shows current->feed, e.g. "season None->5, episode None->3".
        self.assertTrue(any(
            f'episode {self.episode.id}' in line and 'season None->5' in line and 'episode None->3' in line
            for line in cm.output
        ))

    def test_explicit_ingested_even_when_locked(self):
        self.episode.is_metadata_locked = True
        self.episode.save(update_fields=['is_metadata_locked'])
        from pod_manager.ingesters.default import commit_episode
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, self._entry(itunes_explicit='true'), None, 'Test', mock.Mock())
        self.episode.refresh_from_db()
        self.assertIs(self.episode.explicit, True)

    def test_explicit_not_clobbered_when_feed_omits_it(self):
        self.episode.explicit = True
        self.episode.save(update_fields=['explicit'])
        from pod_manager.ingesters.default import commit_episode
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, self._entry(), None, 'Test', mock.Mock())  # no itunes_explicit
        self.episode.refresh_from_db()
        self.assertIs(self.episode.explicit, True)

    def test_tags_ingested_from_feed_when_unlocked(self):
        from pod_manager.ingesters.default import commit_episode
        entry = self._entry(tags=[{'term': 'Drama'}, {'term': 'drama'}, {'term': 'Comedy'}])
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, entry, None, 'Test', mock.Mock())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.tags, ['Drama', 'Comedy'])

    def test_tags_not_overwritten_when_locked(self):
        self.episode.is_metadata_locked = True
        self.episode.tags = ['Curated']
        self.episode.save(update_fields=['is_metadata_locked', 'tags'])
        from pod_manager.ingesters.default import commit_episode
        entry = self._entry(tags=[{'term': 'Drama'}])
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(self.podcast, entry, None, 'Test', mock.Mock())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.tags, ['Curated'])


class CommitEpisodeAutoMigrationTests(TestCase):
    """commit_episode()'s GUID auto-migration hook: episodes move off
    low-priority feeds onto normal-priority ones (as if bulk-moved, minus the
    pin), the pin and the divergence guard block it, and a low-priority
    ingester updates only GUIDs on episodes it doesn't own."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netmigrate')
        self.low = Podcast.objects.create(
            network=self.network, title='Overflow', slug='overflow-lp', is_low_priority=True)
        self.home = Podcast.objects.create(network=self.network, title='Home', slug='home-np')
        self.episode = Episode.objects.create(
            podcast=self.low, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-mig-1',
        )

    def _entry(self, **kw):
        base = dict(id='guid-mig-1', title='Ep 1')
        base.update(kw)
        return _FakeEntry(**base)

    def _commit(self, podcast, pub_entry=None, sub_entry=None, enhancer=None):
        from pod_manager.ingesters.default import commit_episode
        stdout = mock.Mock()
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            commit_episode(podcast, pub_entry, sub_entry, 'Test', stdout, enhancer)
        return stdout

    def _stdout_lines(self, stdout):
        return [c.args[0] for c in stdout.write.call_args_list]

    def test_migrates_from_low_priority_owner_to_normal_ingester(self):
        with mock.patch('pod_manager.tasks.task_rebuild_episode_fragments.delay') as svc_rebuild, \
             self.assertLogs('pod_manager.ingesters.default', level='INFO') as cm:
            stdout = self._commit(self.home, pub_entry=self._entry())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.home.id)
        self.assertIsNone(self.episode.podcast_pinned_at)
        self.assertIsNone(self.episode.podcast_pinned_by)
        self.assertTrue(any(
            "[AUTO-MIGRATE] 'Ep 1': Overflow -> Home" in line
            for line in self._stdout_lines(stdout)))
        self.assertTrue(any('Auto-migrated episode' in line for line in cm.output))
        # rebuild_fragments=False — only commit_episode's own dispatch (patched
        # at the ingester module) runs, never a second one from the service.
        svc_rebuild.assert_not_called()

    def test_no_migration_when_owner_is_normal_priority(self):
        self.low.is_low_priority = False
        self.low.save(update_fields=['is_low_priority'])
        self._commit(self.home, pub_entry=self._entry())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.low.id)

    def test_no_migration_when_both_are_low_priority(self):
        self.home.is_low_priority = True
        self.home.save(update_fields=['is_low_priority'])
        self._commit(self.home, pub_entry=self._entry())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.low.id)

    def test_no_migration_when_episode_is_pinned(self):
        self.episode.podcast_pinned_at = timezone.now()
        self.episode.save(update_fields=['podcast_pinned_at'])
        stdout = self._commit(self.home, pub_entry=self._entry())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.low.id)
        self.assertFalse(any('[AUTO-MIGRATE]' in line for line in self._stdout_lines(stdout)))

    def test_divergence_guard_skips_and_writes_skip_migrate(self):
        other = Episode.objects.create(
            podcast=self.low, title='Ep 1 priv', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_private='guid-mig-priv',
        )
        stdout = self._commit(self.home, pub_entry=self._entry(),
                              sub_entry=self._entry(id='guid-mig-priv'))
        self.episode.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.low.id)
        self.assertEqual(other.podcast_id, self.low.id)
        self.assertTrue(any('[SKIP MIGRATE]' in line for line in self._stdout_lines(stdout)))

    def test_low_priority_ingester_updates_only_guids_on_non_owned_episode(self):
        self.episode.podcast = self.home
        self.episode.audio_url_public = 'https://cdn.test/orig.mp3'
        self.episode.season_number = 2
        self.episode.save(update_fields=['podcast', 'audio_url_public', 'season_number'])
        enhancer = mock.Mock()
        entry = self._entry(
            title='Stale Overflow Title', itunes_season='9',
            enclosures=[_FakeEntry(href='https://cdn.test/stale.mp3')],
        )
        self._commit(self.low, pub_entry=entry,
                     sub_entry=self._entry(id='guid-mig-priv'), enhancer=enhancer)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.home.id)
        self.assertEqual(self.episode.title, 'Ep 1')
        self.assertEqual(self.episode.audio_url_public, 'https://cdn.test/orig.mp3')
        self.assertEqual(self.episode.season_number, 2)
        self.assertEqual(self.episode.guid_private, 'guid-mig-priv')
        enhancer.assert_not_called()

    @override_settings(R2_MIRROR_ENABLED=True)
    def test_migration_runs_cross_pub_cleanup_and_rekey(self):
        self.episode.r2_url = 'https://audio.test/1/1/ep-aaaaaaaaaaaaaaaa.mp3'
        self.episode.save(update_fields=['r2_url'])
        EpisodeCrossPublication.objects.create(episode=self.episode, podcast=self.home)
        with mock.patch('pod_manager.tasks.task_rekey_episode_audio.delay') as rekey:
            self._commit(self.home, pub_entry=self._entry())
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.home.id)
        self.assertFalse(EpisodeCrossPublication.objects.filter(
            episode=self.episode, podcast=self.home).exists())
        rekey.assert_called_once_with(self.episode.id)

    def test_metadata_locked_episode_still_migrates_fields_untouched(self):
        self.episode.is_metadata_locked = True
        self.episode.title = 'Curated'
        self.episode.save(update_fields=['is_metadata_locked', 'title'])
        self._commit(self.home, pub_entry=self._entry(title='Feed Title'))
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.podcast_id, self.home.id)
        self.assertEqual(self.episode.title, 'Curated')


class BaldmoveEnhancerTagTests(TestCase):
    """baldmove_enhancer(): merges scraped tags onto the RSS tags the default
    ingester already set, and only web-scrapes when the feed had none."""

    def setUp(self):
        self.network = Network.objects.create(name='BM', slug='bm')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='bm-show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
        )

    def test_merges_feed_tags_without_scraping_when_present(self):
        from pod_manager.ingesters.baldmove import baldmove_enhancer
        self.episode.tags = ['Existing']  # as set by the default ingester
        pub = _FakeEntry(tags=[{'term': 'Drama'}, {'term': 'existing'}])
        with mock.patch('pod_manager.ingesters.baldmove.scrape_tags_from_patreon') as sp, \
             mock.patch('pod_manager.ingesters.baldmove.scrape_tags_from_wp') as sw:
            baldmove_enhancer(self.episode, pub, None, False, mock.Mock())
        self.assertEqual(self.episode.tags, ['Existing', 'Drama'])  # 'existing' deduped
        sp.assert_not_called()
        sw.assert_not_called()

    def test_scrapes_and_merges_only_when_feed_empty(self):
        from pod_manager.ingesters.baldmove import baldmove_enhancer
        self.episode.tags = []
        self.episode.link = 'https://patreon.com/posts/123'
        pub = _FakeEntry(link='https://patreon.com/posts/123')  # no tags
        with mock.patch('pod_manager.ingesters.baldmove.scrape_tags_from_patreon',
                         return_value=['Scraped', 'Drama']) as sp:
            baldmove_enhancer(self.episode, pub, None, False, mock.Mock())
        sp.assert_called_once()
        self.assertEqual(self.episode.tags, ['Scraped', 'Drama'])


@override_settings(CACHES=TEST_CACHES)
class BackfillSeasonEpisodeTagsCommandTests(TestCase):
    """backfill_season_episode_tags: preview-by-default, --force, --bypass-lock,
    --episode scoping. get_feed() is mocked — no real network I/O."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(name='Net', slug='net')
        self.podcast = Podcast.objects.create(
            network=self.network, title='Show', slug='show',
            public_feed_url='https://feeds.example.com/pub.xml',
        )
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            guid_public='guid-1',
        )

    def _fake_feed(self, itunes_season='5', itunes_episode='3'):
        entry = _FakeEntry(id='guid-1', itunes_season=itunes_season, itunes_episode=itunes_episode,
                            itunes_episodetype='full')
        feed = mock.Mock()
        feed.entries = [entry]
        return feed

    def test_requires_a_scope(self):
        from django.core.management import call_command
        from django.core.management.base import CommandError
        with self.assertRaises(CommandError):
            call_command('backfill_season_episode_tags')

    def test_preview_is_the_default(self):
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', network='net')
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.season_number)
        self.assertIsNone(self.episode.episode_number)

    def test_apply_updates_missing_values(self):
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', network='net', apply=True)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 5)
        self.assertEqual(self.episode.episode_number, 3)
        self.assertEqual(self.episode.episode_type, 'full')

    def test_apply_skips_already_set_without_force(self):
        self.episode.season_number = 1
        self.episode.episode_number = 1
        self.episode.save(update_fields=['season_number', 'episode_number'])
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', network='net', apply=True)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 1)  # unchanged

    def test_apply_with_force_overwrites_existing_values(self):
        self.episode.season_number = 1
        self.episode.episode_number = 1
        self.episode.save(update_fields=['season_number', 'episode_number'])
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', network='net', apply=True, force=True)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 5)
        self.assertEqual(self.episode.episode_number, 3)

    def test_locked_episode_skipped_by_default(self):
        self.episode.is_metadata_locked = True
        self.episode.save(update_fields=['is_metadata_locked'])
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', network='net', apply=True)
        self.episode.refresh_from_db()
        self.assertIsNone(self.episode.season_number)

    def test_bypass_lock_forces_locked_episode(self):
        self.episode.is_metadata_locked = True
        self.episode.save(update_fields=['is_metadata_locked'])
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=self._fake_feed()):
            call_command('backfill_season_episode_tags', episode=self.episode.id,
                         apply=True, bypass_lock=True)
        self.episode.refresh_from_db()
        self.assertEqual(self.episode.season_number, 5)
        self.assertEqual(self.episode.episode_number, 3)

    def test_episode_scope_ignores_other_episodes_in_same_podcast(self):
        other = Episode.objects.create(
            podcast=self.podcast, title='Ep 2', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-2',
        )
        entry2 = _FakeEntry(id='guid-2', itunes_season='9', itunes_episode='9', itunes_episodetype='full')
        feed = mock.Mock()
        feed.entries = [self._fake_feed().entries[0], entry2]
        from django.core.management import call_command
        with mock.patch('pod_manager.management.commands.backfill_season_episode_tags.get_feed',
                         return_value=feed):
            call_command('backfill_season_episode_tags', episode=self.episode.id, apply=True)
        self.episode.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(self.episode.season_number, 5)
        self.assertIsNone(other.season_number)  # out of scope, untouched


class ReleaseCalendarServiceTests(TestCase):
    """services/release_calendar: natural-key matching (numbered shows via
    season+episode, unnumbered via type+title with the blank-type wildcard),
    the ±60-day stale window, ensure_'s explicit-id / match / auto-create
    fallthrough, idempotency, and network scoping of explicit ids."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netcal')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='showcal')
        self.when = timezone.now() + timedelta(days=7)

    def _episode(self, **kw):
        base = dict(podcast=self.podcast, title='Ep 1', pub_date=self.when,
                    scheduled_at=self.when, is_published=False,
                    raw_description='x', clean_description='x')
        base.update(kw)
        return Episode.objects.create(**base)

    def _entry(self, **kw):
        base = dict(network=self.network, podcast=self.podcast, title='Ep 1',
                    scheduled_at=self.when)
        base.update(kw)
        return CalendarEntry.objects.create(**base)

    def test_numbered_show_matches_on_season_episode(self):
        entry = self._entry(title='Planned placeholder', season_number=2, episode_number=5)
        ep = self._episode(title='Actual on-air title', season_number=2, episode_number=5)
        self.assertEqual(match_calendar_entry(ep), entry)

    def test_numbered_show_requires_both_numbers_to_agree(self):
        self._entry(season_number=2, episode_number=6)
        ep = self._episode(season_number=2, episode_number=5)
        self.assertIsNone(match_calendar_entry(ep))

    def test_unnumbered_show_blank_type_entry_matches_typed_episode(self):
        # A planner's entry typically leaves episode_type blank while the real
        # episode arrives typed ('full') — the wildcard must bridge that.
        entry = self._entry(title='Watch Party', episode_type='')
        ep = self._episode(title='watch party', episode_type='full')
        self.assertEqual(match_calendar_entry(ep), entry)

    def test_unnumbered_show_conflicting_type_does_not_match(self):
        self._entry(title='Watch Party', episode_type='bonus')
        ep = self._episode(title='Watch Party', episode_type='full')
        self.assertIsNone(match_calendar_entry(ep))

    def test_window_excludes_stale_entry(self):
        stale = self._entry(season_number=1, episode_number=1,
                            scheduled_at=self.when - timedelta(days=90))
        ep = self._episode(season_number=1, episode_number=1)
        self.assertIsNone(match_calendar_entry(ep))
        created = ensure_calendar_entry_for_episode(ep)
        stale.refresh_from_db()
        self.assertIsNone(stale.episode)
        self.assertNotEqual(created.id, stale.id)

    def test_ensure_auto_creates_when_no_match(self):
        ep = self._episode(season_number=3, episode_number=1, episode_type='full')
        entry = ensure_calendar_entry_for_episode(ep)
        self.assertEqual(CalendarEntry.objects.count(), 1)
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.network, self.network)
        self.assertEqual(entry.podcast, self.podcast)
        self.assertEqual(entry.title, ep.title)
        self.assertEqual(entry.season_number, 3)
        self.assertEqual(entry.episode_number, 1)
        self.assertEqual(entry.episode_type, 'full')
        self.assertEqual(entry.scheduled_at, self.when)

    def test_ensure_is_idempotent_and_resyncs_time(self):
        ep = self._episode(season_number=2, episode_number=5)
        first = ensure_calendar_entry_for_episode(ep)
        later = self.when + timedelta(days=2)
        ep = Episode.objects.get(pk=ep.pk)
        ep.scheduled_at = later
        ep.save(update_fields=['scheduled_at'])
        second = ensure_calendar_entry_for_episode(ep)
        self.assertEqual(first.id, second.id)
        self.assertEqual(CalendarEntry.objects.count(), 1)
        self.assertEqual(second.scheduled_at, later)

    def test_ensure_explicit_id_links_freeform_entry_and_syncs_podcast(self):
        # A freeform entry that would never auto-match (no podcast, other title).
        entry = self._entry(podcast=None, title='Live Watch')
        ep = self._episode(title='Something Unrelated')
        result = ensure_calendar_entry_for_episode(ep, calendar_entry_id=entry.id)
        self.assertEqual(result.id, entry.id)
        entry.refresh_from_db()
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.podcast, self.podcast)
        self.assertEqual(entry.scheduled_at, self.when)
        self.assertEqual(CalendarEntry.objects.count(), 1)

    def test_link_syncs_fields_and_description_from_episode(self):
        # Once linked, the episode is the public source of truth: an explicit
        # freeform pick adopts the episode's title/S/E/type and description.
        entry = self._entry(podcast=None, title='Live Watch', notes='planner notes')
        ep = self._episode(title='Real Title', season_number=2, episode_number=5,
                           episode_type='full',
                           clean_description='<p>Big &amp; bold finale</p>')
        ensure_calendar_entry_for_episode(ep, calendar_entry_id=entry.id)
        entry.refresh_from_db()
        self.assertEqual(entry.title, 'Real Title')
        self.assertEqual(entry.season_number, 2)
        self.assertEqual(entry.episode_number, 5)
        self.assertEqual(entry.episode_type, 'full')
        self.assertEqual(entry.notes, 'Big & bold finale')

    def test_link_preserves_planned_fields_when_episode_blank(self):
        entry = self._entry(title='Planned', season_number=4, episode_number=2,
                            episode_type='bonus', notes='keep me')
        ep = self._episode(title='Actual', season_number=4, episode_number=2,
                           episode_type='', clean_description='')
        ensure_calendar_entry_for_episode(ep)
        entry.refresh_from_db()
        self.assertEqual(entry.title, 'Actual')
        self.assertEqual(entry.episode_type, 'bonus')
        self.assertEqual(entry.notes, 'keep me')

    def test_auto_create_populates_notes_from_description(self):
        ep = self._episode(clean_description='<p>Notes here</p>')
        entry = ensure_calendar_entry_for_episode(ep)
        self.assertEqual(entry.notes, 'Notes here')

    def test_ensure_rejects_cross_network_entry_id(self):
        other_network = Network.objects.create(name='Other', slug='othercal')
        foreign = CalendarEntry.objects.create(
            network=other_network, title='Foreign plan', scheduled_at=self.when)
        ep = self._episode()
        result = ensure_calendar_entry_for_episode(ep, calendar_entry_id=foreign.id)
        foreign.refresh_from_db()
        self.assertIsNone(foreign.episode)
        self.assertIsNone(foreign.podcast)
        self.assertNotEqual(result.id, foreign.id)
        self.assertEqual(result.network, self.network)


class PublishCalendarEntryWiringTests(TestCase):
    """ensure_calendar_entry_for_episode wiring: _handle_publish_post's
    schedule + immediate-publish branches, manage_episode's schedule +
    publish_now actions (with the explicit calendar_entry_id passthrough),
    and never on draft."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netcalwire')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='showcalwire')
        self.owner = User.objects.create_user('calowner', password='x')
        self.network.owners.add(self.owner)
        self.client.force_login(self.owner)

    def _post_publish(self, action, **extra):
        data = {'action': action, 'network_slug': self.network.slug,
                'podcast_id': self.podcast.id, 'title': 'New Ep',
                'tags_json': '[]', 'chapters_json': 'null'}
        data.update(extra)
        with mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            return self.client.post(reverse('publish_episode'), data)

    def _post_manage(self, ep, action, **extra):
        data = {'action': action}
        data.update(extra)
        with mock.patch('pod_manager.views.creator.publish.task_rebuild_episode_fragments'):
            return self.client.post(reverse('manage_episode', args=[ep.id]), data)

    def test_schedule_creates_and_links_entry(self):
        resp = self._post_publish('schedule', scheduled_at='2026-08-15T12:00')
        self.assertEqual(resp.status_code, 302)
        ep = Episode.objects.get(title='New Ep')
        entry = CalendarEntry.objects.get()
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.scheduled_at, ep.scheduled_at)

    def test_schedule_reconciles_preplanned_entry(self):
        planned = CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='New Ep',
            scheduled_at=timezone.now() + timedelta(days=3))
        self._post_publish('schedule', scheduled_at='2026-07-20T12:00')
        planned.refresh_from_db()
        self.assertEqual(planned.episode, Episode.objects.get(title='New Ep'))
        self.assertEqual(CalendarEntry.objects.count(), 1)

    def test_schedule_links_explicit_calendar_entry_id(self):
        freeform = CalendarEntry.objects.create(
            network=self.network, title='Live Watch',
            scheduled_at=timezone.now() + timedelta(days=3))
        self._post_publish('schedule', scheduled_at='2026-08-15T12:00',
                           calendar_entry_id=freeform.id)
        freeform.refresh_from_db()
        self.assertEqual(freeform.episode, Episode.objects.get(title='New Ep'))
        self.assertEqual(freeform.podcast, self.podcast)
        self.assertEqual(CalendarEntry.objects.count(), 1)

    def test_immediate_publish_creates_entry(self):
        self._post_publish('publish')
        ep = Episode.objects.get(title='New Ep')
        entry = CalendarEntry.objects.get()
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.scheduled_at, ep.pub_date)

    def test_draft_creates_no_entry(self):
        self._post_publish('draft')
        self.assertTrue(Episode.objects.filter(title='New Ep').exists())
        self.assertEqual(CalendarEntry.objects.count(), 0)

    def test_manage_schedule_creates_and_links_entry(self):
        ep = Episode.objects.create(
            podcast=self.podcast, title='Existing', pub_date=timezone.now(),
            raw_description='x', clean_description='x', is_published=True)
        self._post_manage(ep, 'schedule', scheduled_at='2026-08-20T18:00')
        ep.refresh_from_db()
        entry = CalendarEntry.objects.get()
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.scheduled_at, ep.scheduled_at)

    def test_manage_publish_now_creates_entry_keeping_planned_time(self):
        # A3: publish_now nulls scheduled_at but pub_date keeps the planned
        # time — the calendar shows the planned slot, not the go-live moment.
        planned_time = timezone.now() + timedelta(days=1)
        ep = Episode.objects.create(
            podcast=self.podcast, title='Existing', pub_date=planned_time,
            scheduled_at=planned_time, is_published=False,
            raw_description='x', clean_description='x')
        self._post_manage(ep, 'publish_now')
        ep.refresh_from_db()
        self.assertTrue(ep.is_published)
        entry = CalendarEntry.objects.get()
        self.assertEqual(entry.episode, ep)
        self.assertEqual(entry.scheduled_at, planned_time)


class CommitEpisodeCalendarLinkTests(TestCase):
    """commit_episode()'s A5 hook: link_calendar_entry_for_new_episode runs
    for NEW ingested episodes only, links a matching pre-planned entry with
    the [CALENDAR LINK] stdout line, and never auto-creates from ingest."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netcaling')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='showcaling')

    def _commit(self, pub_entry):
        from pod_manager.ingesters.default import commit_episode
        stdout = mock.Mock()
        with mock.patch('pod_manager.ingesters.default.task_rebuild_episode_fragments'):
            episode = commit_episode(self.podcast, pub_entry, None, 'Test', stdout)
        return episode, [c.args[0] for c in stdout.write.call_args_list]

    def test_new_episode_links_matching_entry(self):
        planned = CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Ep 1',
            scheduled_at=timezone.now() + timedelta(days=3))
        episode, lines = self._commit(_FakeEntry(id='guid-cal-1', title='Ep 1'))
        planned.refresh_from_db()
        self.assertEqual(planned.episode, episode)
        self.assertEqual(planned.scheduled_at, episode.pub_date)
        self.assertTrue(any('[CALENDAR LINK]' in line for line in lines))
        self.assertEqual(CalendarEntry.objects.count(), 1)

    def test_new_episode_never_auto_creates_entry(self):
        episode, lines = self._commit(_FakeEntry(id='guid-cal-2', title='Ep 2'))
        self.assertIsNotNone(episode.pk)
        self.assertEqual(CalendarEntry.objects.count(), 0)
        self.assertFalse(any('[CALENDAR LINK]' in line for line in lines))

    def test_existing_episode_update_does_not_link(self):
        Episode.objects.create(
            podcast=self.podcast, title='Ep 1', pub_date=timezone.now(),
            raw_description='x', clean_description='x', guid_public='guid-cal-3')
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Ep 1',
            scheduled_at=timezone.now())
        _, lines = self._commit(_FakeEntry(id='guid-cal-3', title='Ep 1'))
        self.assertFalse(CalendarEntry.objects.filter(episode__isnull=False).exists())
        self.assertFalse(any('[CALENDAR LINK]' in line for line in lines))


class CalendarFeedTests(TestCase):
    """generate_calendar_feed ICS output: A6 (DTSTAMP on every VEVENT,
    X-WR-CALNAME, timed zero-duration events, url only when linked+published
    else external_link, notes as DESCRIPTION) and A13 (a scheduled-unpublished
    entry appears WITHOUT a URL; the same entry carries the episode URL once
    is_published flips)."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='netcalfeed')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='showcalfeed')
        self.factory = RequestFactory()

    def _entry(self, **kw):
        base = dict(network=self.network, podcast=self.podcast, title='Planned Ep',
                    scheduled_at=timezone.now() + timedelta(days=3))
        base.update(kw)
        return CalendarEntry.objects.create(**base)

    def _ics(self):
        req = self.factory.get(reverse('calendar_feed', args=[self.network.slug]))
        resp = views.generate_calendar_feed(req, network_slug=self.network.slug)
        self.assertEqual(resp['Content-Type'], 'text/calendar')
        return resp.content.decode('utf-8')

    def test_calendar_shell_and_dtstamp_on_every_vevent(self):
        self._entry(title='One')
        self._entry(title='Two')
        ics = self._ics()
        self.assertIn('BEGIN:VCALENDAR', ics)
        self.assertIn('VERSION:2.0', ics)
        self.assertIn('PRODID:-//Vecto//Net Release Calendar//EN', ics)
        self.assertIn('X-WR-CALNAME:Net Releases', ics)
        self.assertEqual(ics.count('BEGIN:VEVENT'), 2)
        self.assertEqual(ics.count('DTSTAMP'), 2)
        self.assertEqual(ics.count('LAST-MODIFIED'), 2)

    def test_events_are_timed_and_zero_duration(self):
        self._entry()
        ics = self._ics()
        dtstart_lines = [l for l in ics.splitlines() if l.startswith('DTSTART')]
        self.assertEqual(len(dtstart_lines), 1)
        self.assertIn('T', dtstart_lines[0].split(':', 1)[1])  # timed, not all-day
        self.assertNotIn('VALUE=DATE:', dtstart_lines[0])
        self.assertNotIn('DTEND', ics)

    def test_freeform_entry_uses_external_link(self):
        self._entry(podcast=None, title='Live Watch',
                    external_link='https://example.com/live')
        ics = self._ics()
        self.assertIn('URL:https://example.com/live', ics)

    def test_unlinked_entry_without_external_link_has_no_url(self):
        self._entry()
        self.assertNotIn('URL', self._ics())

    def test_notes_become_description_with_podcast_header(self):
        self._entry(notes='Season finale — bring snacks', episode_type='full')
        ics = self._ics()
        self.assertIn('DESCRIPTION:Show · full', ics)
        self.assertIn('Season finale', ics)

    def test_freeform_description_is_just_notes(self):
        self._entry(podcast=None, notes='Come hang out')
        self.assertIn('DESCRIPTION:Come hang out', self._ics())

    def test_numbered_summary_includes_sxe(self):
        self._entry(title='Finale', season_number=2, episode_number=5)
        self.assertIn('SUMMARY:S2E5 · Finale', self._ics())

    def test_scheduled_unpublished_entry_appears_without_url_then_gains_it(self):
        # A13 is a requirement, not an accident: the URL is computed live from
        # is_published at render time, never stored on the entry.
        when = timezone.now() + timedelta(days=3)
        ep = Episode.objects.create(
            podcast=self.podcast, title='Scheduled Ep', pub_date=when,
            scheduled_at=when, is_published=False,
            raw_description='x', clean_description='x')
        entry = self._entry(title='Scheduled Ep', episode=ep, scheduled_at=when)
        ics = self._ics()
        self.assertIn('SUMMARY:Scheduled Ep', ics)
        self.assertNotIn('URL', ics)

        # Simulate the scheduled auto-publish task / publish_now flipping it.
        ep.is_published = True
        ep.save(update_fields=['is_published'])
        ics = self._ics()
        self.assertEqual(ics.count('BEGIN:VEVENT'), 1)
        self.assertIn(f'URL:http://testserver/episode/{ep.id}/', ics)


def _sched_ep(podcast, title, when, published=False):
    return Episode.objects.create(
        podcast=podcast, title=title, pub_date=when, scheduled_at=when,
        is_published=published, raw_description='x', clean_description='x')


@override_settings(ALLOWED_HOSTS=['*'])
class CalendarPageViewTests(TestCase):
    """Public /calendar page (Feature 4, A14): read-only for listeners, owner
    controls rendered only for owners, 404 off a network domain. Themed cases
    route through NetworkMiddleware via an HTTP_HOST matching custom_domain."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='CalNet', slug='calpage', custom_domain='calpage.example.test')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='calpageshow')
        self.owner = User.objects.create_user('calowner', password='x')
        self.network.owners.add(self.owner)
        self.host = 'calpage.example.test'

    def test_listener_sees_read_only_page(self):
        resp = self.client.get('/calendar/', HTTP_HOST=self.host)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Release Calendar')
        self.assertContains(resp, 'Subscribe')
        self.assertNotContains(resp, 'id="calAddBtn"')
        self.assertNotContains(resp, 'id="calEntryModal"')

    def test_owner_sees_add_controls(self):
        self.client.force_login(self.owner)
        resp = self.client.get('/calendar/', HTTP_HOST=self.host)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'id="calAddBtn"')
        self.assertContains(resp, 'id="calEntryModal"')

    def test_navbar_has_calendar_link(self):
        resp = self.client.get('/calendar/', HTTP_HOST=self.host)
        self.assertContains(resp, '>Calendar</a>')

    def test_subscribe_row_has_all_affordances(self):
        resp = self.client.get('/calendar/', HTTP_HOST=self.host)
        self.assertContains(resp, 'webcal://calpage.example.test/feed/calpage/calendar.ics')
        self.assertContains(resp, 'calendar.google.com/calendar/render')
        self.assertContains(resp, 'outlook.live.com/calendar/0/addfromweb')

    def test_404_off_network_domain(self):
        resp = self.client.get('/calendar/', HTTP_HOST='totally-unknown.example.test')
        self.assertEqual(resp.status_code, 404)


@override_settings(ALLOWED_HOSTS=['*'])
class CalendarEventsJsonTests(TestCase):
    """Public JSON event source (A7 + A14): scoped to request.network,
    editable = is_owner AND unlinked, url computed live from is_published (A13)."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='EvNet', slug='calev', custom_domain='calev.example.test')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='calevshow')
        self.owner = User.objects.create_user('evowner', password='x')
        self.network.owners.add(self.owner)
        self.other = Network.objects.create(
            name='Other', slug='calevother', custom_domain='calevother.example.test')
        CalendarEntry.objects.create(
            network=self.other, title='ForeignEntry', scheduled_at=timezone.now())
        self.host = 'calev.example.test'

    def _events(self):
        resp = self.client.get('/calendar/events/', HTTP_HOST=self.host)
        self.assertEqual(resp.status_code, 200)
        return resp.json()

    def test_scoped_to_request_network(self):
        CalendarEntry.objects.create(network=self.network, title='MineEntry', scheduled_at=timezone.now())
        titles = [e['title'] for e in self._events()]
        self.assertIn('MineEntry', titles)
        self.assertNotIn('ForeignEntry', titles)

    def test_editable_only_for_owner_and_unlinked(self):
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Free', scheduled_at=timezone.now())
        self.assertFalse(self._events()[0]['editable'])
        self.client.force_login(self.owner)
        self.assertTrue(self._events()[0]['editable'])

    def test_linked_entry_not_editable_for_owner(self):
        when = timezone.now()
        ep = _sched_ep(self.podcast, 'Ep', when)
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Ep', episode=ep, scheduled_at=when)
        self.client.force_login(self.owner)
        self.assertFalse(self._events()[0]['editable'])

    def test_numbered_title_and_a13_url_logic(self):
        when = timezone.now()
        ep = _sched_ep(self.podcast, 'S2E5', when)
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Finale',
            season_number=2, episode_number=5, episode=ep, scheduled_at=when)
        ev = self._events()[0]
        self.assertEqual(ev['title'], 'Finale')
        self.assertEqual(ev['extendedProps']['sxe'], 'S2E5')
        self.assertNotIn('url', ev)  # scheduled-unpublished: no url (A13)
        ep.is_published = True
        ep.save(update_fields=['is_published'])
        ev = self._events()[0]
        self.assertIn('url', ev)
        self.assertIn(f'/episode/{ep.id}/', ev['url'])

    def test_freeform_external_link_used_as_url(self):
        CalendarEntry.objects.create(
            network=self.network, title='Live', external_link='https://ex.com/live',
            scheduled_at=timezone.now())
        self.assertEqual(self._events()[0]['url'], 'https://ex.com/live')

    def test_notes_surfaced_in_extended_props(self):
        CalendarEntry.objects.create(
            network=self.network, title='WithNotes', notes='public copy here',
            scheduled_at=timezone.now())
        self.assertEqual(self._events()[0]['extendedProps']['notes'], 'public copy here')

    def test_start_rendered_in_eastern_wall_clock(self):
        # 16:00 UTC on 2026-08-01 is 12:00 EDT; FullCalendar runs in UTC mode so
        # we hand it the Eastern wall-clock with the tz stripped.
        when = datetime.datetime(2026, 8, 1, 16, 0, tzinfo=datetime.timezone.utc)
        CalendarEntry.objects.create(network=self.network, title='ET', scheduled_at=when)
        self.assertEqual(self._events()[0]['start'], '2026-08-01T12:00:00')


@override_settings(ALLOWED_HOSTS=['*'])
class CalendarManageTests(TestCase):
    """Owner mutations (A14): server-side ownership on every endpoint, freeform
    vs podcast add, network-scoped delete, drag-move UNLINKED entries only (A8)."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='MgNet', slug='calmg', custom_domain='calmg.example.test')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='calmgshow')
        self.owner = User.objects.create_user('mgowner', password='x')
        self.network.owners.add(self.owner)
        self.stranger = User.objects.create_user('mgstranger', password='x')
        self.host = 'calmg.example.test'

    def _post(self, data):
        return self.client.post('/calendar/manage/', data, HTTP_HOST=self.host)

    def test_add_podcast_entry_owner(self):
        self.client.force_login(self.owner)
        resp = self._post({
            'action': 'add', 'title': 'Planned', 'podcast_id': self.podcast.id,
            'date': '2026-08-01', 'time': '12:00', 'season_number': '2',
            'episode_number': '5', 'episode_type': 'full', 'notes': 'hi',
        })
        self.assertEqual(resp.status_code, 302)
        entry = CalendarEntry.objects.get(title='Planned')
        self.assertEqual(entry.podcast_id, self.podcast.id)
        self.assertEqual(entry.season_number, 2)
        self.assertEqual(entry.episode_number, 5)
        self.assertEqual(entry.episode_type, 'full')
        self.assertEqual(entry.created_by, self.owner)

    def test_add_freeform_entry(self):
        self.client.force_login(self.owner)
        self._post({
            'action': 'add', 'title': 'Live Watch', 'podcast_id': '',
            'date': '2026-08-01', 'time': '12:00', 'external_link': 'https://ex.com/live',
        })
        entry = CalendarEntry.objects.get(title='Live Watch')
        self.assertIsNone(entry.podcast_id)
        self.assertEqual(entry.external_link, 'https://ex.com/live')

    def test_add_rejected_for_non_owner(self):
        self.client.force_login(self.stranger)
        resp = self._post({'action': 'add', 'title': 'Nope', 'scheduled_at': '2026-08-01T12:00'})
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(CalendarEntry.objects.filter(title='Nope').exists())

    def test_add_rejected_for_anonymous(self):
        resp = self._post({'action': 'add', 'title': 'Nope', 'scheduled_at': '2026-08-01T12:00'})
        self.assertEqual(resp.status_code, 403)
        self.assertFalse(CalendarEntry.objects.filter(title='Nope').exists())

    def test_delete_owner(self):
        entry = CalendarEntry.objects.create(network=self.network, title='Del', scheduled_at=timezone.now())
        self.client.force_login(self.owner)
        resp = self._post({'action': 'delete', 'entry_id': entry.id})
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(CalendarEntry.objects.filter(id=entry.id).exists())

    def test_delete_scoped_to_network(self):
        other = Network.objects.create(name='O', slug='calmgo', custom_domain='calmgo.example.test')
        foreign = CalendarEntry.objects.create(network=other, title='Foreign', scheduled_at=timezone.now())
        self.client.force_login(self.owner)
        resp = self._post({'action': 'delete', 'entry_id': foreign.id})
        self.assertEqual(resp.status_code, 404)
        self.assertTrue(CalendarEntry.objects.filter(id=foreign.id).exists())

    def test_move_unlinked_entry_interprets_eastern(self):
        entry = CalendarEntry.objects.create(network=self.network, title='Move', scheduled_at=timezone.now())
        self.client.force_login(self.owner)
        # FullCalendar (UTC mode over an Eastern display) hands back the Eastern
        # wall-clock UTC-labeled: 15:00 on 2026-09-09 is EDT (-4) -> 19:00 UTC.
        resp = self._post({'action': 'move', 'entry_id': entry.id, 'start': '2026-09-09T15:00:00Z'})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()['ok'])
        entry.refresh_from_db()
        self.assertEqual(entry.scheduled_at.isoformat(), '2026-09-09T19:00:00+00:00')

    def test_add_defaults_to_eastern(self):
        self.client.force_login(self.owner)
        self._post({'action': 'add', 'title': 'ETdefault', 'date': '2026-08-01', 'time': '12:00'})
        entry = CalendarEntry.objects.get(title='ETdefault')
        # 12:00 EDT (-4) -> 16:00 UTC
        self.assertEqual(entry.scheduled_at.isoformat(), '2026-08-01T16:00:00+00:00')

    def test_add_respects_selected_timezone(self):
        self.client.force_login(self.owner)
        self._post({'action': 'add', 'title': 'PT', 'date': '2026-08-01', 'time': '12:00',
                    'tz': 'America/Los_Angeles'})
        entry = CalendarEntry.objects.get(title='PT')
        # 12:00 PDT (-7) -> 19:00 UTC
        self.assertEqual(entry.scheduled_at.isoformat(), '2026-08-01T19:00:00+00:00')

    def test_add_invalid_tz_falls_back_to_eastern(self):
        self.client.force_login(self.owner)
        self._post({'action': 'add', 'title': 'BadTz', 'date': '2026-08-01', 'time': '12:00',
                    'tz': 'Mars/Phobos'})
        entry = CalendarEntry.objects.get(title='BadTz')
        self.assertEqual(entry.scheduled_at.isoformat(), '2026-08-01T16:00:00+00:00')

    def test_add_missing_time_rejected(self):
        self.client.force_login(self.owner)
        self._post({'action': 'add', 'title': 'NoTime', 'date': '2026-08-01'})
        self.assertFalse(CalendarEntry.objects.filter(title='NoTime').exists())

    def test_edit_freeform_entry(self):
        entry = CalendarEntry.objects.create(
            network=self.network, title='Old', scheduled_at=timezone.now(),
            external_link='https://ex.com/old', notes='old notes')
        self.client.force_login(self.owner)
        resp = self._post({
            'action': 'edit', 'entry_id': entry.id, 'title': 'New Title',
            'date': '2026-08-01', 'time': '12:00', 'podcast_id': '',
            'external_link': 'https://ex.com/new', 'notes': 'new notes',
        })
        self.assertEqual(resp.status_code, 302)
        entry.refresh_from_db()
        self.assertEqual(entry.title, 'New Title')
        self.assertEqual(entry.external_link, 'https://ex.com/new')
        self.assertEqual(entry.notes, 'new notes')
        self.assertEqual(entry.scheduled_at.isoformat(), '2026-08-01T16:00:00+00:00')

    def test_edit_can_attach_podcast_fields(self):
        entry = CalendarEntry.objects.create(
            network=self.network, title='Freeform', scheduled_at=timezone.now())
        self.client.force_login(self.owner)
        self._post({
            'action': 'edit', 'entry_id': entry.id, 'title': 'Now Podcast',
            'date': '2026-08-01', 'time': '12:00', 'podcast_id': self.podcast.id,
            'season_number': '3', 'episode_number': '7', 'episode_type': 'full',
        })
        entry.refresh_from_db()
        self.assertEqual(entry.podcast_id, self.podcast.id)
        self.assertEqual(entry.season_number, 3)
        self.assertEqual(entry.episode_number, 7)
        self.assertEqual(entry.external_link, '')

    def test_edit_rejects_linked_entry(self):
        when = timezone.now()
        ep = _sched_ep(self.podcast, 'Ep', when)
        entry = CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Linked',
            episode=ep, scheduled_at=when)
        self.client.force_login(self.owner)
        self._post({'action': 'edit', 'entry_id': entry.id, 'title': 'Hacked',
                    'date': '2026-08-01', 'time': '12:00'})
        entry.refresh_from_db()
        self.assertEqual(entry.title, 'Linked')  # unchanged

    def test_edit_scoped_to_network(self):
        other = Network.objects.create(name='O', slug='calmgedit', custom_domain='calmgedit.example.test')
        foreign = CalendarEntry.objects.create(network=other, title='Foreign', scheduled_at=timezone.now())
        self.client.force_login(self.owner)
        resp = self._post({'action': 'edit', 'entry_id': foreign.id, 'title': 'X',
                           'date': '2026-08-01', 'time': '12:00'})
        self.assertEqual(resp.status_code, 404)
        foreign.refresh_from_db()
        self.assertEqual(foreign.title, 'Foreign')

    def test_edit_rejected_for_non_owner(self):
        entry = CalendarEntry.objects.create(network=self.network, title='NE', scheduled_at=timezone.now())
        self.client.force_login(self.stranger)
        resp = self._post({'action': 'edit', 'entry_id': entry.id, 'title': 'X',
                           'date': '2026-08-01', 'time': '12:00'})
        self.assertEqual(resp.status_code, 403)

    def test_move_linked_entry_rejected(self):
        when = timezone.now()
        ep = _sched_ep(self.podcast, 'Ep', when)
        entry = CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='Ep', episode=ep, scheduled_at=when)
        self.client.force_login(self.owner)
        resp = self._post({'action': 'move', 'entry_id': entry.id, 'start': '2026-09-09T15:00:00+00:00'})
        self.assertEqual(resp.status_code, 409)
        entry.refresh_from_db()
        self.assertEqual(entry.scheduled_at, when)

    def test_move_rejected_for_non_owner(self):
        entry = CalendarEntry.objects.create(network=self.network, title='NM', scheduled_at=timezone.now())
        self.client.force_login(self.stranger)
        resp = self._post({'action': 'move', 'entry_id': entry.id, 'start': '2026-09-09T15:00:00+00:00'})
        self.assertEqual(resp.status_code, 403)


class PublishFormCalendarSelectorTests(TestCase):
    """Publish form 'Link to Calendar Entry' selector (A9): renders unlinked
    upcoming entries with data-podcast-id; excludes linked/past entries."""

    def setUp(self):
        self.network = Network.objects.create(name='PbNet', slug='calpub')
        self.podcast = Podcast.objects.create(network=self.network, title='Show', slug='calpubshow')
        self.owner = User.objects.create_user('pbowner', password='x')
        self.network.owners.add(self.owner)
        self.client.force_login(self.owner)

    def _get(self):
        return self.client.get(reverse('publish_episode') + '?network=calpub')

    def test_selector_shows_unlinked_upcoming_entries(self):
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='ZzUpcomingEntry',
            scheduled_at=timezone.now() + timedelta(days=5))
        resp = self._get()
        self.assertContains(resp, 'id="calendarEntrySelect"')
        self.assertContains(resp, 'ZzUpcomingEntry')
        self.assertContains(resp, f'data-podcast-id="{self.podcast.id}"')

    def test_linked_and_past_entries_excluded(self):
        when = timezone.now() + timedelta(days=5)
        ep = _sched_ep(self.podcast, 'Ep', when)
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='ZzLinkedEntry',
            episode=ep, scheduled_at=when)
        CalendarEntry.objects.create(
            network=self.network, podcast=self.podcast, title='ZzPastEntry',
            scheduled_at=timezone.now() - timedelta(days=5))
        resp = self._get()
        self.assertNotContains(resp, 'id="calendarEntrySelect"')
        self.assertNotContains(resp, 'ZzLinkedEntry')
        self.assertNotContains(resp, 'ZzPastEntry')



# â”€â”€ 13. Transcript access gate (Section A/B/E1) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_ALLOWED_TX_EXTS = ('vtt', 'json', 'srt', 'html', 'words')

_R2_SERVE_SETTINGS = {
    **TRANSCRIPTION_SETTINGS,
    'R2_MEDIA_ENABLED': True,
    'R2_MEDIA_PUBLIC_HOST': 'https://cdn.example.com',
    'R2_MEDIA_KEY_PREFIX': '',
}


class TranscriptKeyShapeTests(SimpleTestCase):
    """transcript_r2_key(episode_id, ext, token=None) â€” the single derivation
    chokepoint. Legacy vs keyed shapes (Section E1)."""

    def test_legacy_key_when_no_token(self):
        self.assertEqual(transcript_r2_key(6354, 'vtt'), 'transcripts/6/6354.vtt')

    def test_keyed_key_when_token(self):
        self.assertEqual(transcript_r2_key(6354, 'vtt', 'abc123'),
                         'transcripts/6/6354.abc123.vtt')

    def test_bucket_math_matches_legacy(self):
        self.assertEqual(transcript_r2_key(999, 'srt', 'tok').split('/')[1], '0')
        self.assertEqual(transcript_r2_key(1000, 'srt', 'tok').split('/')[1], '1')

    def test_invalid_ext_raises_even_with_token(self):
        with self.assertRaises(ValueError):
            transcript_r2_key(1, 'exe', 'abc')


class TranscriptTokenDefaultTests(TestCase):
    """Section A2 / D4 â€” new transcripts are born keyed; existing rows (migrated
    as NULL) stay legacy. The field default fires on INSERT only."""

    def test_new_transcript_born_with_token(self):
        _, _, ep = _make_fixture()
        t = Transcript.objects.create(episode=ep)
        self.assertTrue(t.r2_key_token)
        self.assertEqual(len(t.r2_key_token), 22)  # secrets.token_urlsafe(16)

    def test_token_generator_is_random(self):
        from pod_manager.models import new_transcript_token
        self.assertNotEqual(new_transcript_token(), new_transcript_token())


@override_settings(**_R2_SERVE_SETTINGS)
class TranscriptServeAccessTests(TestCase):
    """Section B â€” serve_transcript gate across both audiences, all five exts,
    and the ?download branch. Also verifies the 302 target's key shape (Keys)."""

    def setUp(self):
        cache.clear()
        self.net = Network.objects.create(name='Net', slug='n-serve')
        self.tier = PatreonTier.objects.create(network=self.net, name='Premium', minimum_cents=500)
        self.pod = Podcast.objects.create(
            network=self.net, title='Show', slug='show-serve', required_tier=self.tier)
        self.ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self.token = 'tok0123456789abcdef012'
        self.transcript = Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=1,
            r2_key_token=self.token,
            vtt_file='m', json_file='m', srt_file='m', html_file='m', words_json_file='m',
        )

    # -- helpers --------------------------------------------------------------
    def _url(self, ext):
        return reverse('serve_transcript', kwargs={'episode_id': self.ep.id, 'ext': ext})

    def _set_flag(self, value):
        self.pod.allow_public_transcripts = value
        self.pod.save(update_fields=['allow_public_transcripts'])

    def _member(self, username, *, pledge=0, patron=False):
        user = User.objects.create_user(username=username)
        NetworkMembership.objects.create(
            user=user, network=self.net,
            is_active_patron=patron, patreon_pledge_cents=pledge)
        return user

    def _profile(self, user):
        return PatronProfile.objects.create(user=user, patreon_id=None)

    # -- unauthenticated / flag --------------------------------------------
    def test_unauthenticated_flag_on_redirects_all_exts(self):
        self._set_flag(True)
        for ext in _ALLOWED_TX_EXTS:
            resp = self.client.get(self._url(ext))
            self.assertEqual(resp.status_code, 302, ext)

    def test_unauthenticated_flag_off_404_all_exts(self):
        self._set_flag(False)
        for ext in _ALLOWED_TX_EXTS:
            resp = self.client.get(self._url(ext))
            self.assertEqual(resp.status_code, 404, ext)

    # -- session audience --------------------------------------------------
    def test_session_premium_flag_off_redirects(self):
        self._set_flag(False)
        self.client.force_login(self._member('patron', pledge=500, patron=True))
        self.assertEqual(self.client.get(self._url('vtt')).status_code, 302)

    def test_owner_flag_off_redirects(self):
        self._set_flag(False)
        owner = User.objects.create_user(username='owner-serve')
        self.net.owners.add(owner)
        self.client.force_login(owner)
        self.assertEqual(self.client.get(self._url('vtt')).status_code, 302)

    def test_superuser_flag_off_redirects(self):
        self._set_flag(False)
        su = User.objects.create_superuser(username='su-serve', email='su@e.com', password='x')
        self.client.force_login(su)
        self.assertEqual(self.client.get(self._url('vtt')).status_code, 302)

    def test_session_member_without_pledge_flag_off_404(self):
        self._set_flag(False)
        self.client.force_login(self._member('plain'))
        self.assertEqual(self.client.get(self._url('vtt')).status_code, 404)

    # -- podcast-app (?auth) audience --------------------------------------
    def test_auth_with_access_flag_off_redirects(self):
        self._set_flag(False)
        profile = self._profile(self._member('auth-yes', pledge=500, patron=True))
        resp = self.client.get(self._url('vtt'), {'auth': str(profile.feed_token)})
        self.assertEqual(resp.status_code, 302)

    def test_auth_without_access_flag_off_404(self):
        self._set_flag(False)
        profile = self._profile(self._member('auth-no'))
        resp = self.client.get(self._url('vtt'), {'auth': str(profile.feed_token)})
        self.assertEqual(resp.status_code, 404)

    def test_auth_without_access_flag_on_redirects(self):
        self._set_flag(True)
        profile = self._profile(self._member('auth-flagon'))
        resp = self.client.get(self._url('vtt'), {'auth': str(profile.feed_token)})
        self.assertEqual(resp.status_code, 302)

    def test_target_cross_publication_grants_access_flag_off(self):
        # Parent podcast is gated; TARGET-mode target is free, so a member with no
        # pledge (no parent access) gets in via the override loop.
        self._set_flag(False)
        target = Podcast.objects.create(network=self.net, title='Target', slug='target-serve')
        EpisodeCrossPublication.objects.create(
            episode=self.ep, podcast=target,
            access_mode=EpisodeCrossPublication.AccessMode.TARGET,
        )
        profile = self._profile(self._member('auth-target'))
        resp = self.client.get(self._url('vtt'), {'auth': str(profile.feed_token)})
        self.assertEqual(resp.status_code, 302)

    def test_inherit_cross_publication_does_not_grant_flag_off(self):
        # An INHERIT link (default) must NOT open the gate for a target-only member.
        self._set_flag(False)
        target = Podcast.objects.create(network=self.net, title='Target2', slug='target2-serve')
        EpisodeCrossPublication.objects.create(episode=self.ep, podcast=target)  # INHERIT
        profile = self._profile(self._member('auth-inherit'))
        resp = self.client.get(self._url('vtt'), {'auth': str(profile.feed_token)})
        self.assertEqual(resp.status_code, 404)

    # -- download branch ---------------------------------------------------
    def test_download_branch_enforces_gate(self):
        self._set_flag(False)
        resp = self.client.get(self._url('vtt'), {'download': '1'})
        self.assertEqual(resp.status_code, 404)

    # -- malformed auth ------------------------------------------------------
    def test_malformed_auth_treated_as_anonymous_not_500(self):
        # feed_token is a UUIDField; a junk value must read as "no profile"
        # (a bare filter would raise ValidationError -> 500).
        self._set_flag(False)
        resp = self.client.get(self._url('vtt'), {'auth': 'not-a-uuid'})
        self.assertEqual(resp.status_code, 404)
        self._set_flag(True)
        resp = self.client.get(self._url('vtt'), {'auth': 'not-a-uuid'})
        self.assertEqual(resp.status_code, 302)

    # -- key shape in the 302 target (Keys) --------------------------------
    def test_redirect_uses_keyed_key_when_tokened(self):
        self._set_flag(True)
        resp = self.client.get(self._url('vtt'))
        self.assertEqual(
            resp['Location'],
            f'https://cdn.example.com/transcripts/{self.ep.id // 1000}/'
            f'{self.ep.id}.{self.token}.vtt?v=1',
        )

    def test_redirect_uses_legacy_key_when_untokened(self):
        self._set_flag(True)
        self.transcript.r2_key_token = None
        self.transcript.save(update_fields=['r2_key_token'])
        resp = self.client.get(self._url('vtt'))
        self.assertEqual(
            resp['Location'],
            f'https://cdn.example.com/transcripts/{self.ep.id // 1000}/{self.ep.id}.vtt?v=1',
        )


# â”€â”€ 14. Episode-page transcript gate (Section D) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@override_settings(**TRANSCRIPTION_SETTINGS)
class EpisodePageTranscriptGateTests(TestCase):
    """Section D â€” episode_detail is a second content-delivery path (inline HTML +
    words JSON via read_transcript_bytes). The gate is real enforcement: a
    non-viewer's page must carry no transcript bytes and no data-words-url."""

    SECRET = 'SECRET_TRANSCRIPT_TEXT'

    def setUp(self):
        cache.clear()
        self.tmp = tempfile.mkdtemp()
        self._media = override_settings(MEDIA_ROOT=self.tmp)
        self._media.enable()
        self.net = Network.objects.create(name='Net', slug='n-page')
        self.tier = PatreonTier.objects.create(network=self.net, name='Premium', minimum_cents=500)
        # Gated show, but with BOTH public + subscriber audio so the tab stays
        # visible (raw_audio_url resolves to the public cut) while the transcript
        # itself is gated â€” the Bald Movies "First Run" shape.
        self.pod = Podcast.objects.create(
            network=self.net, title='Show', slug='show-page', required_tier=self.tier)
        self.ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )

    def tearDown(self):
        self._media.disable()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers --------------------------------------------------------------
    def _set_flag(self, value, pod=None):
        pod = pod or self.pod
        pod.allow_public_transcripts = value
        pod.save(update_fields=['allow_public_transcripts'])

    def _local_transcript(self, ep=None):
        ep = ep or self.ep
        hp = transcript_path(ep.id, 'html')
        hp.parent.mkdir(parents=True, exist_ok=True)
        hp.write_text(f'<div>{self.SECRET}</div>', encoding='utf-8')
        wp = transcript_path(ep.id, 'words')
        wp.write_text(json.dumps({'segments': []}), encoding='utf-8')
        return Transcript.objects.create(
            episode=ep, status=Transcript.Status.COMPLETED, version=0,
            html_file=str(hp.relative_to(self.tmp)),
            words_json_file=str(wp.relative_to(self.tmp)),
        )

    def _render(self, user, ep=None):
        ep = ep or self.ep
        req = _make_tenant_request(
            RequestFactory(), self.net, path=f'/episode/{ep.id}/', user=user)
        return views.episode_detail(req, ep.id).content.decode('utf-8')

    def _anon(self):
        from django.contrib.auth.models import AnonymousUser
        return AnonymousUser()

    # -- gated logged-out ---------------------------------------------------
    def test_logged_out_flag_off_gate_notice_no_content(self):
        self._set_flag(False)
        self._local_transcript()
        body = self._render(self._anon())
        # Tab visible, but no transcript bytes and no word-sync hook.
        self.assertIn('id="transcript-tab"', body)
        self.assertNotIn(self.SECRET, body)
        self.assertNotIn('data-words-url', body)
        # Gated show -> subscriber copy; download buttons hidden.
        self.assertIn('available to subscribers', body)
        vtt_url = reverse('serve_transcript', kwargs={'episode_id': self.ep.id, 'ext': 'vtt'})
        self.assertNotIn(f'{vtt_url}?download=1', body)

    def test_free_show_gate_notice_says_sign_in(self):
        free = Podcast.objects.create(
            network=self.net, title='Free', slug='free-page', allow_public_transcripts=False)
        ep = Episode.objects.create(
            podcast=free, title='FreeEp', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
            audio_url_subscriber='https://cdn.example.com/priv.mp3',
        )
        self._local_transcript(ep)
        body = self._render(self._anon(), ep=ep)
        self.assertIn('Sign in to view transcripts', body)
        self.assertNotIn('available to subscribers', body)
        self.assertNotIn(self.SECRET, body)

    def test_flag_on_public_visitor_sees_transcript(self):
        self._set_flag(True)
        self._local_transcript()
        body = self._render(self._anon())
        self.assertIn(self.SECRET, body)
        self.assertIn('data-words-url', body)

    # -- subscriber (keyed R2 transcript) ----------------------------------
    def test_subscriber_sees_inline_transcript_keyed(self):
        self._set_flag(False)
        user = User.objects.create_user(username='sub-page')
        NetworkMembership.objects.create(
            user=user, network=self.net, is_active_patron=True, patreon_pledge_cents=500)
        token = 'keyedtoken0123456789ab'
        Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=1,
            r2_key_token=token, html_file='m', words_json_file='m')
        store = {
            transcript_r2_key(self.ep.id, 'html', token): b'<div>' + self.SECRET.encode() + b'</div>',
            transcript_r2_key(self.ep.id, 'words', token): json.dumps({'segments': []}).encode(),
        }

        def fake_get(key):
            return store[key], 'text/html'

        with override_settings(R2_MEDIA_ENABLED=True,
                               R2_MEDIA_PUBLIC_HOST='https://cdn.example.com',
                               R2_MEDIA_KEY_PREFIX=''), \
             mock.patch('pod_manager.services.r2_storage.get_media_object', side_effect=fake_get):
            body = self._render(user)
        # Inline render pulled the bytes from the KEYED object (a miss on the legacy
        # key would KeyError in fake_get), and the word-sync hook is present.
        self.assertIn(self.SECRET, body)
        self.assertIn('data-words-url', body)

    # -- owner always full access ------------------------------------------
    def test_owner_flag_off_sees_content_and_retranscribe_panel(self):
        self._set_flag(False)
        owner = User.objects.create_user(username='owner-page')
        self.net.owners.add(owner)
        self._local_transcript()
        body = self._render(owner)
        self.assertIn(self.SECRET, body)
        self.assertIn('retranscribePanel', body)


# ── 15. Rekey churn, orphan routing, CDN purge (Sections E2-E5) ──────────────

from botocore.exceptions import ClientError as _BotoClientError

from pod_manager.services import cloudflare as cf
from pod_manager.services import r2_maintenance

_REKEY_SETTINGS = dict(
    CACHES=TEST_CACHES,
    R2_MEDIA_ENABLED=True,
    R2_MEDIA_PUBLIC_HOST='https://cdn.example.com',
    R2_MEDIA_KEY_PREFIX='',
    R2_MEDIA_BUCKET='vecto-cdn-test',
    R2_BUCKET='vecto-audio-test',
    R2_PUBLIC_HOST='https://audio.test',
    R2_KEY_PREFIX='',
    R2_ORPHAN_RETENTION_DAYS=90,
    R2_REKEY_GRACE_DAYS=7,
)


def _tx_fields(exts):
    return {('words_json_file' if e == 'words' else f'{e}_file'): 'm' for e in exts}


class CloudflarePurgeTests(SimpleTestCase):
    """purge_urls: 30-URL batching, Bearer auth, fail-closed semantics (E4)."""

    def _resp(self, status=200, success=True):
        resp = mock.Mock(status_code=status, text='{}')
        resp.json.return_value = {'success': success}
        return resp

    @override_settings(CLOUDFLARE_ZONE_ID='zone123', CLOUDFLARE_PURGE_TOKEN='cf-tok')
    def test_batches_of_30_with_zone_and_bearer(self):
        urls = [f'https://cdn.example.com/u{i}' for i in range(65)]
        with mock.patch.object(cf.requests, 'post', return_value=self._resp()) as post:
            self.assertTrue(cf.purge_urls(urls))
        self.assertEqual(post.call_count, 3)
        endpoint = post.call_args_list[0].args[0]
        self.assertIn('/zones/zone123/purge_cache', endpoint)
        sizes = [len(c.kwargs['json']['files']) for c in post.call_args_list]
        self.assertEqual(sizes, [30, 30, 5])
        self.assertEqual(post.call_args_list[0].kwargs['headers']['Authorization'],
                         'Bearer cf-tok')

    @override_settings(CLOUDFLARE_ZONE_ID='zone123', CLOUDFLARE_PURGE_TOKEN='cf-tok')
    def test_any_failed_batch_returns_false(self):
        with mock.patch.object(cf.requests, 'post',
                               side_effect=[self._resp(), self._resp(success=False)]):
            self.assertFalse(cf.purge_urls([f'u{i}' for i in range(31)]))

    @override_settings(CLOUDFLARE_ZONE_ID='zone123', CLOUDFLARE_PURGE_TOKEN='cf-tok')
    def test_network_error_returns_false(self):
        with mock.patch.object(cf.requests, 'post', side_effect=OSError('boom')):
            self.assertFalse(cf.purge_urls(['u1']))

    @override_settings(CLOUDFLARE_ZONE_ID='', CLOUDFLARE_PURGE_TOKEN='')
    def test_unconfigured_fails_closed_without_calling_api(self):
        with mock.patch.object(cf.requests, 'post') as post:
            self.assertFalse(cf.purge_urls(['u1']))
        post.assert_not_called()

    @override_settings(CLOUDFLARE_ZONE_ID='', CLOUDFLARE_PURGE_TOKEN='')
    def test_empty_list_is_trivially_true(self):
        with mock.patch.object(cf.requests, 'post') as post:
            self.assertTrue(cf.purge_urls([]))
        post.assert_not_called()


@override_settings(R2_MEDIA_ENABLED=True)
class WriteTranscriptFormatsKeyedTests(TestCase):
    """Keys checklist: a tokened write lands at the KEYED object location."""

    def test_put_targets_keyed_location_when_token_passed(self):
        put_calls = []
        with mock.patch('pod_manager.services.r2_storage.media_object_etag',
                        return_value=None), \
             mock.patch('pod_manager.services.r2_storage.put_media_object',
                        side_effect=lambda key, content, ct: put_calls.append(key)):
            markers, changed = write_transcript_formats(7, [('vtt', b'WEBVTT')], 'tok123')
        self.assertEqual(put_calls, ['transcripts/0/7.tok123.vtt'])
        self.assertEqual(markers['vtt'], 'transcripts/0/7.tok123.vtt')
        self.assertEqual(changed, ['vtt'])


@override_settings(**_REKEY_SETTINGS)
class RekeyTranscriptsTests(TestCase):
    """Section E2 — the churn: strict record->copy->token->delete->purge order,
    idempotency, scoping, --limit, and crash/purge-failure convergence."""

    def setUp(self):
        self.net = Network.objects.create(name='Net', slug='n-rekey')
        self.pod_a = Podcast.objects.create(network=self.net, title='A', slug='show-a')
        self.pod_b = Podcast.objects.create(network=self.net, title='B', slug='show-b')
        self.ep_a = self._episode(self.pod_a, 'A1')
        self.ep_b = self._episode(self.pod_b, 'B1')
        # Two legacy (untokened) R2-backed transcripts across two podcasts...
        self.t_a = Transcript.objects.create(
            episode=self.ep_a, status=Transcript.Status.COMPLETED, version=3,
            r2_key_token=None, **_tx_fields(('vtt', 'json', 'srt', 'html', 'words')))
        self.t_b = Transcript.objects.create(
            episode=self.ep_b, status=Transcript.Status.COMPLETED, version=1,
            r2_key_token=None, **_tx_fields(('vtt',)))
        # ...and rows the churn must never touch: already keyed / local-only / not done.
        self.t_keyed = Transcript.objects.create(
            episode=self._episode(self.pod_a, 'A2'), status=Transcript.Status.COMPLETED,
            version=2, r2_key_token='alreadykeyed0123456789', **_tx_fields(('vtt',)))
        self.t_local = Transcript.objects.create(
            episode=self._episode(self.pod_a, 'A3'), status=Transcript.Status.COMPLETED,
            version=0, r2_key_token=None, **_tx_fields(('vtt',)))
        self.t_pending = Transcript.objects.create(
            episode=self._episode(self.pod_a, 'A4'), status=Transcript.Status.PENDING,
            version=1, r2_key_token=None)

    def _episode(self, podcast, title):
        return Episode.objects.create(
            podcast=podcast, title=title, pub_date=timezone.now(),
            raw_description='x', clean_description='x')

    def _run(self, purge_ok=True, client=None, apply=True, **kwargs):
        client = client or mock.MagicMock()
        purge = mock.MagicMock(return_value=purge_ok)
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client), \
             mock.patch('pod_manager.services.r2_storage.get_r2_client',
                        return_value=client), \
             mock.patch.object(cf, 'purge_urls', purge):
            result = r2_maintenance.rekey_transcripts(apply=apply, **kwargs)
        return result, client, purge

    def _plain_key(self, ep, ext):
        return transcript_r2_key(ep.id, ext)

    def _counts(self, result):
        return {k: result[k] for k in ('rekeyed', 'retry_pending', 'errors')}

    # -- dry run (the safety-idiom default) -----------------------------------
    def test_dry_run_lists_candidates_and_writes_nothing(self):
        result, client, purge = self._run(apply=False)
        self.assertEqual(result['candidates'], [self.ep_a.id, self.ep_b.id])
        self.assertFalse(result['applied'])
        client.copy_object.assert_not_called()
        client.delete_object.assert_not_called()
        purge.assert_not_called()
        self.t_a.refresh_from_db()
        self.assertIsNone(self.t_a.r2_key_token)
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    # -- the full happy path, no args = every podcast ------------------------
    def test_no_args_rekeys_all_podcasts(self):
        result, client, purge = self._run()
        self.assertEqual(self._counts(result), {'rekeyed': 2, 'retry_pending': 0, 'errors': 0})

        self.t_a.refresh_from_db(); self.t_b.refresh_from_db()
        self.assertEqual(len(self.t_a.r2_key_token), 22)
        self.assertEqual(len(self.t_b.r2_key_token), 22)
        self.assertNotEqual(self.t_a.r2_key_token, self.t_b.r2_key_token)
        # Untouched rows keep their state.
        self.t_keyed.refresh_from_db(); self.t_local.refresh_from_db()
        self.assertEqual(self.t_keyed.r2_key_token, 'alreadykeyed0123456789')
        self.assertIsNone(self.t_local.r2_key_token)

        # Server-side copies, media bucket, old plain key -> keyed key.
        self.assertEqual(client.copy_object.call_count, 6)  # 5 exts + 1 ext
        for c in client.copy_object.call_args_list:
            self.assertEqual(c.kwargs['Bucket'], 'vecto-cdn-test')
            self.assertEqual(c.kwargs['CopySource']['Bucket'], 'vecto-cdn-test')
        b_copy = [c for c in client.copy_object.call_args_list
                  if c.kwargs['CopySource']['Key'] == self._plain_key(self.ep_b, 'vtt')]
        self.assertEqual(len(b_copy), 1)
        self.assertEqual(b_copy[0].kwargs['Key'],
                         transcript_r2_key(self.ep_b.id, 'vtt', self.t_b.r2_key_token))

        # Old plain objects deleted from the MEDIA bucket.
        self.assertEqual(client.delete_object.call_count, 6)
        deleted = {c.kwargs['Key'] for c in client.delete_object.call_args_list}
        self.assertIn(self._plain_key(self.ep_a, 'words'), deleted)
        self.assertIn(self._plain_key(self.ep_b, 'vtt'), deleted)
        for c in client.delete_object.call_args_list:
            self.assertEqual(c.kwargs['Bucket'], 'vecto-cdn-test')

        # Purge: bare URL + ?v=1..version per old key (E4).
        self.assertEqual(purge.call_count, 2)
        a_urls, b_urls = purge.call_args_list[0].args[0], purge.call_args_list[1].args[0]
        self.assertEqual(len(a_urls), 5 * (1 + 3))  # 5 exts x (bare + v1..v3)
        base_b = f'https://cdn.example.com/{self._plain_key(self.ep_b, "vtt")}'
        self.assertEqual(b_urls, [base_b, f'{base_b}?v=1'])

        # Fully converged: no retry ledger left behind.
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    def test_idempotent_rerun_is_a_no_op(self):
        self._run()
        result, client, _ = self._run()
        self.assertEqual(result['rekeyed'], 0)
        client.copy_object.assert_not_called()

    # -- scoping --------------------------------------------------------------
    def test_podcast_slug_scopes_the_churn(self):
        result, _, _ = self._run(podcast_slug='show-b')
        self.assertEqual(result['rekeyed'], 1)
        self.t_a.refresh_from_db(); self.t_b.refresh_from_db()
        self.assertIsNone(self.t_a.r2_key_token)
        self.assertIsNotNone(self.t_b.r2_key_token)

    def test_limit_stops_after_n(self):
        Transcript.objects.create(
            episode=self._episode(self.pod_b, 'B2'), status=Transcript.Status.COMPLETED,
            version=1, r2_key_token=None, **_tx_fields(('vtt',)))
        result, _, _ = self._run(limit=2)
        self.assertEqual(result['rekeyed'], 2)
        tokened = Transcript.objects.filter(
            r2_key_token__isnull=False).exclude(pk=self.t_keyed.pk).count()
        self.assertEqual(tokened, 2)

    # -- failure ordering ------------------------------------------------------
    def test_crash_between_copy_and_token_leaves_retryable_orphans(self):
        bad = mock.MagicMock()
        bad.copy_object.side_effect = _BotoClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'CopyObject')
        result, _, purge = self._run(client=bad, podcast_slug='show-a')
        self.assertEqual(self._counts(result), {'rekeyed': 0, 'retry_pending': 0, 'errors': 1})
        # Orphan rows were recorded BEFORE the copy — the durable retry record.
        keys = set(R2OrphanedObject.objects.values_list('key', flat=True))
        self.assertEqual(keys, {self._plain_key(self.ep_a, e)
                                for e in ('vtt', 'json', 'srt', 'html', 'words')})
        self.assertEqual(
            set(R2OrphanedObject.objects.values_list('reason', flat=True)),
            {R2OrphanedObject.Reason.MOVE_REKEY})
        # Token NOT set (the 302 must keep pointing at the live plain objects)...
        self.t_a.refresh_from_db()
        self.assertIsNone(self.t_a.r2_key_token)
        purge.assert_not_called()
        # ...and a rerun with a healthy client converges.
        result, _, _ = self._run(podcast_slug='show-a')
        self.assertEqual(result['rekeyed'], 1)
        self.t_a.refresh_from_db()
        self.assertIsNotNone(self.t_a.r2_key_token)
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    def test_purge_failure_keeps_orphan_rows_but_sets_token(self):
        result, client, _ = self._run(purge_ok=False, podcast_slug='show-b')
        self.assertEqual(self._counts(result), {'rekeyed': 0, 'retry_pending': 1, 'errors': 0})
        # The move itself completed: token set, old object deleted...
        self.t_b.refresh_from_db()
        self.assertIsNotNone(self.t_b.r2_key_token)
        client.delete_object.assert_called_once()
        # ...but the row survives as the purge retry ledger (cleared only after
        # delete AND purge succeed).
        self.assertEqual(
            list(R2OrphanedObject.objects.values_list('key', flat=True)),
            [self._plain_key(self.ep_b, 'vtt')])

    def test_purge_failure_rerecords_ledger_dropped_by_concurrent_cleanup(self):
        # A cleanup run racing the pre-token window classifies the plain-key
        # rows as re-adopted and drops them; if the purge then fails, the rekey
        # must re-assert the ledger or nothing ever retries the purge.
        def drop_rows_then_fail(urls):
            R2OrphanedObject.objects.all().delete()
            return False

        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client), \
             mock.patch('pod_manager.services.r2_storage.get_r2_client',
                        return_value=client), \
             mock.patch.object(cf, 'purge_urls', side_effect=drop_rows_then_fail):
            result = r2_maintenance.rekey_transcripts(apply=True, podcast_slug='show-b')
        self.assertEqual(result['retry_pending'], 1)
        self.assertEqual(
            list(R2OrphanedObject.objects.values_list('key', flat=True)),
            [self._plain_key(self.ep_b, 'vtt')])

    def test_r2_media_disabled_raises(self):
        with override_settings(R2_MEDIA_ENABLED=False):
            with self.assertRaises(RuntimeError):
                r2_maintenance.rekey_transcripts()

    # -- management command ----------------------------------------------------
    def test_command_dry_run_by_default(self):
        from io import StringIO
        from django.core.management import call_command
        out = StringIO()
        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client):
            call_command('rekey_transcripts', stdout=out)
        self.assertIn('2 transcript(s) would be rekeyed', out.getvalue())
        client.copy_object.assert_not_called()
        self.t_a.refresh_from_db()
        self.assertIsNone(self.t_a.r2_key_token)

    def test_command_apply_reports_counts(self):
        from io import StringIO
        from django.core.management import call_command
        out = StringIO()
        client = mock.MagicMock()
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client), \
             mock.patch('pod_manager.services.r2_storage.get_r2_client',
                        return_value=client), \
             mock.patch.object(cf, 'purge_urls', mock.MagicMock(return_value=True)):
            call_command('rekey_transcripts', '--apply', stdout=out)
        self.assertIn('2 transcript(s) rekeyed', out.getvalue())

    def test_command_unknown_slug_raises(self):
        from io import StringIO
        from django.core.management import call_command
        from django.core.management.base import CommandError
        with self.assertRaises(CommandError):
            call_command('rekey_transcripts', '--podcast', 'nope', stdout=StringIO())


@override_settings(**_REKEY_SETTINGS)
class CleanupOrphanTranscriptRoutingTests(TestCase):
    """Section E3 — cleanup_orphans routes transcripts/ rows to the MEDIA bucket
    (audio rows untouched), re-validates against live Transcript resolution,
    applies ZERO retention, and couples the hard-delete to the CDN purge."""

    def setUp(self):
        self.net = Network.objects.create(name='Net', slug='n-route')
        self.pod = Podcast.objects.create(network=self.net, title='Show', slug='show-route')
        self.ep = Episode.objects.create(
            podcast=self.pod, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x')

    def _run(self, purge_ok=True, apply=True):
        client = mock.MagicMock()
        purge = mock.MagicMock(return_value=purge_ok)
        with mock.patch.object(r2_maintenance, 'get_r2_client', return_value=client), \
             mock.patch('pod_manager.services.r2_storage.get_r2_client',
                        return_value=client), \
             mock.patch.object(cf, 'purge_urls', purge):
            result = r2_maintenance.cleanup_orphans(apply=apply)
        return result, client, purge

    def _tokened_transcript(self, version=2):
        return Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=version,
            r2_key_token='livetoken0123456789ab', **_tx_fields(('vtt',)))

    def test_routes_transcript_rows_to_media_bucket_audio_unaffected(self):
        self._tokened_transcript(version=2)
        plain = transcript_r2_key(self.ep.id, 'vtt')
        # FRESH transcript row (zero retention — must still be processed) vs the
        # audio rows' normal per-reason retention windows.
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        R2OrphanedObject.objects.create(
            key='9/9/old.mp3', reason=R2OrphanedObject.Reason.REVERSION,
            orphaned_at=timezone.now() - timedelta(days=100))
        R2OrphanedObject.objects.create(
            key='9/9/fresh.mp3', reason=R2OrphanedObject.Reason.REVERSION,
            orphaned_at=timezone.now() - timedelta(days=10))

        result, client, purge = self._run()

        # Audio: batch-deleted from the AUDIO bucket, expired row only.
        _, ckwargs = client.delete_objects.call_args
        self.assertEqual(ckwargs['Bucket'], 'vecto-audio-test')
        self.assertEqual({o['Key'] for o in ckwargs['Delete']['Objects']}, {'9/9/old.mp3'})
        # Transcript: single delete against the MEDIA bucket + coupled purge.
        client.delete_object.assert_called_once_with(Bucket='vecto-cdn-test', Key=plain)
        base = f'https://cdn.example.com/{plain}'
        purge.assert_called_once_with([base, f'{base}?v=1', f'{base}?v=2'])
        self.assertEqual(result['transcripts'], [plain])
        self.assertEqual(result['transcripts_retained'], 0)
        remaining = set(R2OrphanedObject.objects.values_list('key', flat=True))
        self.assertEqual(remaining, {'9/9/fresh.mp3'})

    def test_readopted_when_live_transcript_still_resolves_to_key(self):
        # Live transcript is UNTOKENED — the plain key is still its serve target
        # (the crash-before-token-set window), so the object must survive.
        Transcript.objects.create(
            episode=self.ep, status=Transcript.Status.COMPLETED, version=1,
            r2_key_token=None, **_tx_fields(('vtt',)))
        plain = transcript_r2_key(self.ep.id, 'vtt')
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        result, client, purge = self._run()
        self.assertEqual(result['readopted'], 1)
        client.delete_object.assert_not_called()
        purge.assert_not_called()
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    def test_stale_keyed_orphan_is_deleted(self):
        self._tokened_transcript()
        stale = transcript_r2_key(self.ep.id, 'vtt', 'oldstaletoken123456789')
        R2OrphanedObject.objects.create(
            key=stale, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        result, client, _ = self._run()
        client.delete_object.assert_called_once_with(Bucket='vecto-cdn-test', Key=stale)
        self.assertEqual(R2OrphanedObject.objects.count(), 0)

    def test_purge_failure_retains_row_for_retry(self):
        self._tokened_transcript()
        plain = transcript_r2_key(self.ep.id, 'vtt')
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        result, client, _ = self._run(purge_ok=False)
        client.delete_object.assert_called_once()  # origin delete happened
        self.assertEqual(result['transcripts_retained'], 1)
        self.assertTrue(R2OrphanedObject.objects.filter(key=plain).exists())

    def test_missing_row_purges_fallback_version_range(self):
        # Row deleted (delete-to-retranscribe / episode cascade): the real ?v
        # range is unknowable, so a generous fixed range is purged instead of
        # just the bare URL — the ?v=N entries are what the edge actually holds.
        plain = transcript_r2_key(self.ep.id, 'vtt')
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        _, _, purge = self._run()
        base = f'https://cdn.example.com/{plain}'
        purge.assert_called_once_with(
            [base] + [f'{base}?v={k}'
                      for k in range(1, r2_maintenance._FALLBACK_PURGE_VERSIONS + 1)])

    @override_settings(R2_MEDIA_KEY_PREFIX='dev/')
    def test_media_object_key_prefix_applied_in_dev(self):
        plain = transcript_r2_key(self.ep.id, 'vtt')
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        _, client, purge = self._run()
        client.delete_object.assert_called_once_with(
            Bucket='vecto-cdn-test', Key=f'dev/{plain}')
        # No live row here either -> fallback range, all dev-prefixed.
        urls = purge.call_args.args[0]
        self.assertEqual(urls[0], f'https://cdn.example.com/dev/{plain}')
        self.assertEqual(len(urls), 1 + r2_maintenance._FALLBACK_PURGE_VERSIONS)

    def test_dry_run_reports_but_touches_nothing(self):
        self._tokened_transcript()
        plain = transcript_r2_key(self.ep.id, 'vtt')
        R2OrphanedObject.objects.create(
            key=plain, reason=R2OrphanedObject.Reason.MOVE_REKEY,
            orphaned_at=timezone.now())
        result, client, purge = self._run(apply=False)
        self.assertEqual(result['transcripts'], [plain])
        client.delete_object.assert_not_called()
        purge.assert_not_called()
        self.assertEqual(R2OrphanedObject.objects.count(), 1)


class HandleUpdateShowRekeyDispatchTests(TestCase):
    """Section E5 — flipping allow_public_transcripts off auto-dispatches the
    rekey churn for that podcast (idempotent, so every such save may fire it)."""

    def setUp(self):
        self.network = Network.objects.create(name='Net', slug='n-dispatch')
        self.pod = Podcast.objects.create(
            network=self.network, title='Show', slug='show-dispatch')
        self.user = User.objects.create_user('creator-dispatch')

    def _post(self, flag_on):
        from pod_manager.views.creator import actions
        data = {'show_id': self.pod.id}
        if flag_on:
            data['allow_public_transcripts'] = 'on'
        req = RequestFactory().post('/', data)
        req.user = self.user
        with mock.patch.object(actions, 'messages'), \
             mock.patch.object(actions, 'task_rebuild_podcast_fragments'), \
             mock.patch.object(actions, 'task_rekey_podcast_transcripts') as rekey:
            actions.handle_update_show(req, self.network)
        return rekey

    @override_settings(R2_MEDIA_ENABLED=True)
    def test_flag_landing_false_dispatches_rekey(self):
        rekey = self._post(flag_on=False)
        rekey.delay.assert_called_once_with(self.pod.id)

    @override_settings(R2_MEDIA_ENABLED=True)
    def test_flag_on_does_not_dispatch(self):
        self.assertFalse(self._post(flag_on=True).delay.called)

    def test_r2_media_disabled_does_not_dispatch(self):
        # R2_MEDIA_ENABLED is forced False in tests — the guard must hold.
        self.assertFalse(self._post(flag_on=False).delay.called)


@override_settings(ALLOWED_HOSTS=['*'])
class EpisodeDetailBaseSwapTests(TestCase):
    """Stage 1 base-swap prototype (S1.7): episode_detail returns the skinny
    #boosted-region fragment on an htmx-boosted request and the full document on
    a normal request, chosen by HtmxBaseTemplateMiddleware. Vary: HX-Request is
    on every response so no cache crosses the two shapes. Requests route through
    the real middleware via an HTTP_HOST matching the network's custom_domain."""

    NAV_MARKER = 'id="navbarNav"'          # lives in base.html nav, outside the region
    PLAYER_MARKER = 'id="floatingPlayer"'  # persistent chrome, outside the region
    CONTENT_MARKER = 'BaseSwapEpisodeMarker'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='SwapNet', slug='swapnet', custom_domain='swapnet.example.test')
        self.podcast = Podcast.objects.create(
            network=self.network, title='Show', slug='swapnet-show')
        self.ep = Episode.objects.create(
            podcast=self.podcast, title=self.CONTENT_MARKER, pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        self.host = 'swapnet.example.test'
        self.url = reverse('episode_detail', args=[self.ep.id])

    def _get(self, *, boosted):
        extra = {'HTTP_HX_REQUEST': 'true'} if boosted else {}
        return self.client.get(self.url, HTTP_HOST=self.host, **extra)

    def test_boosted_request_returns_fragment(self):
        resp = self._get(boosted=True)
        self.assertEqual(resp.status_code, 200)
        body = resp.content.decode('utf-8')
        # Fragment: no document chrome — no <html>, no navbar, no floating player.
        self.assertNotIn('<html', body)
        self.assertNotIn(self.NAV_MARKER, body)
        self.assertNotIn(self.PLAYER_MARKER, body)
        # ...but it IS the #boosted-region swap unit wrapping the episode content
        # inside the shared .container so layout is preserved on swap.
        self.assertIn('id="boosted-region"', body)
        self.assertIn('class="container mt-3"', body)
        self.assertIn(self.CONTENT_MARKER, body)
        # The region carries hx-history-elt so that after a boosted outerHTML swap
        # the LIVE region keeps it — otherwise htmx's history element falls back
        # to <body> and the next browser-back overwrites the whole body (wiping
        # the nav + floating player). Regression guard for that bug.
        self.assertIn('hx-history-elt', body)
        # hx-boost is NOT on the fragment: it lives on the persistent outer
        # wrapper in base.html; the swapped-in region inherits it. A fragment
        # that carried hx-boost would create a nested boost context on swap.
        self.assertNotIn('hx-boost', body)

    def test_normal_request_returns_full_page(self):
        resp = self._get(boosted=False)
        self.assertEqual(resp.status_code, 200)
        body = resp.content.decode('utf-8')
        # Full document: doctype/html, navbar, floating player, and the content.
        self.assertIn('<html', body)
        self.assertIn(self.NAV_MARKER, body)
        self.assertIn(self.PLAYER_MARKER, body)
        self.assertIn('id="boosted-region"', body)
        self.assertIn('hx-history-elt', body)
        self.assertIn(self.CONTENT_MARKER, body)
        # The nav sits OUTSIDE #boosted-region but INSIDE the hx-boost wrapper, so
        # its links stay boosted (a full-page nav would destroy the out-of-region
        # floating player). Order: hx-boost wrapper -> navbar -> region. Regression
        # guard for the "nav links do a full reload / player dies" bug.
        boost_at = body.index('hx-boost="true"')
        nav_at = body.index(self.NAV_MARKER)
        region_at = body.index('id="boosted-region"')
        self.assertLess(boost_at, nav_at)
        self.assertLess(nav_at, region_at)

    def test_fragment_is_smaller_than_full_page(self):
        full = self._get(boosted=False).content
        frag = self._get(boosted=True).content
        # Base-swap only pays off if the boosted response actually sheds the
        # chrome — the fragment must be materially smaller than the full page.
        self.assertLess(len(frag), len(full))

    def test_vary_hx_request_on_boosted_response(self):
        resp = self._get(boosted=True)
        self.assertIn('HX-Request', resp.get('Vary', ''))

    def test_vary_hx_request_on_full_response(self):
        resp = self._get(boosted=False)
        self.assertIn('HX-Request', resp.get('Vary', ''))


class BaseSwapRolloutMixin:
    """Shared S1.7 assertions for each view converted to base-swap after the
    episode_detail prototype (see EpisodeDetailBaseSwapTests for the fuller
    guards on the region/nav/hx-boost shape, which are structural and only need
    asserting once). A subclass sets CONTENT_MARKER and defines setUp to build
    its fixtures, self.host and self.url.

    The three invariants per view: an HX request returns the skinny fragment,
    a normal request returns the full document, and both carry Vary: HX-Request
    so no cache can serve one shape to a request wanting the other."""

    NAV_MARKER = 'id="navbarNav"'          # base.html nav — outside the region
    PLAYER_MARKER = 'id="floatingPlayer"'  # persistent chrome — outside the region
    CONTENT_MARKER = None                  # subclass: a marker inside block content

    def _get(self, *, boosted):
        extra = {'HTTP_HX_REQUEST': 'true'} if boosted else {}
        return self.client.get(self.url, HTTP_HOST=self.host, **extra)

    def test_boosted_request_returns_fragment(self):
        resp = self._get(boosted=True)
        self.assertEqual(resp.status_code, 200)
        body = resp.content.decode('utf-8')
        self.assertNotIn('<html', body)
        self.assertNotIn(self.NAV_MARKER, body)
        self.assertNotIn(self.PLAYER_MARKER, body)
        self.assertIn('id="boosted-region"', body)
        self.assertIn('hx-history-elt', body)
        self.assertIn(self.CONTENT_MARKER, body)

    def test_normal_request_returns_full_page(self):
        resp = self._get(boosted=False)
        self.assertEqual(resp.status_code, 200)
        body = resp.content.decode('utf-8')
        self.assertIn('<html', body)
        self.assertIn(self.NAV_MARKER, body)
        self.assertIn(self.PLAYER_MARKER, body)
        self.assertIn('id="boosted-region"', body)
        self.assertIn(self.CONTENT_MARKER, body)

    def test_fragment_is_smaller_than_full_page(self):
        self.assertLess(len(self._get(boosted=True).content),
                        len(self._get(boosted=False).content))

    def test_vary_hx_request_on_both_shapes(self):
        self.assertIn('HX-Request', self._get(boosted=True).get('Vary', ''))
        self.assertIn('HX-Request', self._get(boosted=False).get('Vary', ''))


@override_settings(ALLOWED_HOSTS=['*'])
class HomeBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """home is the highest-traffic boosted view; converting it sheds the ~40 KB
    of fixed chrome from every boosted nav into it."""

    CONTENT_MARKER = 'id="episodes-container"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='HomeNet', slug='homenet', custom_domain='homenet.example.test')
        self.podcast = Podcast.objects.create(
            network=self.network, title='Home Show', slug='homenet-show')
        Episode.objects.create(
            podcast=self.podcast, title='HomeSwapEpisode', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/pub.mp3',
        )
        self.host = 'homenet.example.test'
        self.url = reverse('home')


@override_settings(ALLOWED_HOSTS=['*'])
class UserFeedsBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """user_feeds renders the authenticated listener dashboard; its content block
    carries a big <style> + the feed grid, all of which must ride along in the
    fragment while the chrome does not."""

    CONTENT_MARKER = 'id="feedGrid"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='FeedNet', slug='feednet', custom_domain='feednet.example.test')
        self.user = User.objects.create_user(username='feeduser', password='pw')
        PatronProfile.objects.create(user=self.user, patreon_id=None)
        self.client.force_login(self.user)
        self.host = 'feednet.example.test'
        self.url = reverse('user_feeds')


@override_settings(ALLOWED_HOSTS=['*'])
class UserProfileBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """user_profile is login_required, so this also covers a converted view whose
    content is gated — the fragment must still be the region, not a redirect."""

    CONTENT_MARKER = 'id="profileTabs"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='ProfNet', slug='profnet', custom_domain='profnet.example.test')
        self.user = User.objects.create_user(username='profuser', password='pw')
        PatronProfile.objects.create(user=self.user, patreon_id=None)
        self.client.force_login(self.user)
        self.host = 'profnet.example.test'
        self.url = reverse('user_profile')


@override_settings(ALLOWED_HOSTS=['*'])
class LogViewerBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """log_viewer is staff-gated and its content block carries live JS (the SSE
    log stream + the resource-monitor poller). The base-swap conversion is what
    keeps that script in the fragment: it lives in {% block content %}, so it
    rides along on a boosted swap exactly as it does on a full load."""

    CONTENT_MARKER = 'id="log-feed"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='LogNet', slug='lognet', custom_domain='lognet.example.test')
        self.user = User.objects.create_user(
            username='staffuser', password='pw', is_staff=True)
        self.client.force_login(self.user)
        self.host = 'lognet.example.test'
        self.url = reverse('log_viewer')

    def test_fragment_carries_the_in_content_scripts(self):
        # The live log stream + resource monitor only work if their in-content
        # script survives the swap. Guard that the fragment isn't just markup.
        body = self._get(boosted=True).content.decode('utf-8')
        self.assertIn('<script', body)
        self.assertIn('id="res-panel"', body)


@override_settings(ALLOWED_HOSTS=['*'])
class CreatorSettingsBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """The /creator shell is the biggest proportional win: it already ships a
    light shell (the tabs lazy-load), so nearly all of its boosted response was
    the chrome. Its lazy panes live inside the region and creator_tabs.js is
    written to re-run on a boosted region swap, so the shell converts like any
    other view."""

    CONTENT_MARKER = 'id="list-tab"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='CreatorNet', slug='creatornet',
            custom_domain='creatornet.example.test')
        self.user = User.objects.create_user(username='creatoruser', password='pw')
        self.network.owners.add(self.user)
        self.client.force_login(self.user)
        self.host = 'creatornet.example.test'
        self.url = reverse('creator_settings')


@override_settings(ALLOWED_HOSTS=['*'])
class AdminConsoleBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """admin_console is a superuser shell whose panes populate from JSON
    endpoints; its stylesheet + admin_console.js are inside the content block,
    so they ride along in the fragment."""

    CONTENT_MARKER = 'id="ac-sidebar"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='ConsoleNet', slug='consolenet',
            custom_domain='consolenet.example.test')
        self.user = User.objects.create_superuser(
            username='consoleroot', password='pw', email='root@example.test')
        self.client.force_login(self.user)
        self.host = 'consolenet.example.test'
        self.url = reverse('admin_console')

    def test_fragment_carries_the_console_assets(self):
        # The console is inert without its JS; the stylesheet + script live in
        # the content block precisely so a boosted swap brings them along.
        body = self._get(boosted=True).content.decode('utf-8')
        self.assertIn('admin_console.js', body)
        self.assertIn('admin_console.css', body)


@override_settings(ALLOWED_HOSTS=['*'])
class CalendarBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """calendar hosts FullCalendar, whose init script is in the content block."""

    CONTENT_MARKER = 'id="calendar"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='CalNet', slug='calnet', custom_domain='calnet.example.test')
        self.user = User.objects.create_user(username='caluser', password='pw')
        self.client.force_login(self.user)
        self.host = 'calnet.example.test'
        self.url = reverse('calendar')


@override_settings(ALLOWED_HOSTS=['*'])
class PublishEpisodeBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """publish_episode is the creator publish form (Quill editor + tabs)."""

    CONTENT_MARKER = 'id="pubTabs"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='PubNet', slug='pubnet', custom_domain='pubnet.example.test')
        self.user = User.objects.create_user(username='pubuser', password='pw')
        self.network.owners.add(self.user)
        Podcast.objects.create(network=self.network, title='P', slug='pubnet-show')
        self.client.force_login(self.user)
        self.host = 'pubnet.example.test'
        self.url = reverse('publish_episode')


@override_settings(ALLOWED_HOSTS=['*'])
class LoginRequestBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """login_request is a redirect target: a boosted nav to a gated page 302s
    here with HX-Request still set, so it must return the region fragment for
    the swap rather than a full document."""

    CONTENT_MARKER = 'name="email"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='LoginNet', slug='loginnet', custom_domain='loginnet.example.test')
        self.host = 'loginnet.example.test'
        # login_request.html is served by RecurlyLoginView, routed as
        # 'recurly_login' — 'patreon_login' is the OAuth bounce and only 302s.
        self.url = reverse('recurly_login')


@override_settings(ALLOWED_HOSTS=['*'])
class VerifyAuthenticatorBaseSwapTests(BaseSwapRolloutMixin, TestCase):
    """The TOTP enrolment page. Note verify_authenticator.html is rendered by
    generate_qr_code, not by the verify_authenticator view — that one only ever
    redirects (it POST-handles the code), so /auth/totp/setup/ is the URL that
    actually renders this template."""

    CONTENT_MARKER = 'id="manualSetup"'

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='TotpNet', slug='totpnet', custom_domain='totpnet.example.test')
        self.user = User.objects.create_user(username='totpuser', password='pw')
        self.client.force_login(self.user)
        self.host = 'totpnet.example.test'
        self.url = reverse('generate_qr_code')


@override_settings(ALLOWED_HOSTS=['*'])
class Custom404BaseSwapTests(TestCase):
    """The custom 404 renders through base-swap like any other view, but it is
    NOT part of the swap contract: htmx never swaps a non-2xx, and base.html's
    htmx:responseError handler forces a real navigation instead — which arrives
    as a normal request and gets the full page. These assert both halves:
    the direct hit still full-loads (the plan's S1.6 invariant), and the boosted
    404 keeps its 404 status so the error path stays triggered."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='NotFoundNet', slug='nfnet', custom_domain='nfnet.example.test')
        self.host = 'nfnet.example.test'
        self.url = '/this-path-does-not-exist/'

    @override_settings(DEBUG=False)
    def test_direct_hit_renders_the_full_404_page(self):
        resp = self.client.get(self.url, HTTP_HOST=self.host)
        self.assertEqual(resp.status_code, 404)
        body = resp.content.decode('utf-8')
        self.assertIn('<html', body)
        self.assertIn('id="navbarNav"', body)

    @override_settings(DEBUG=False)
    def test_boosted_404_keeps_its_status_so_the_error_path_fires(self):
        resp = self.client.get(self.url, HTTP_HOST=self.host, HTTP_HX_REQUEST='true')
        # Status must stay 404: htmx declines to swap it and base.html's
        # responseError handler does a real navigation to the full page.
        self.assertEqual(resp.status_code, 404)


@override_settings(ALLOWED_HOSTS=['*'])
class UpdateAvatarPreferenceTests(TestCase):
    """update_avatar_preference must accept every source the model offers and
    return the resulting avatar URL, which the profile page pushes into the
    navbar avatar — that element sits outside #boosted-region, so nothing else
    re-renders it after the POST."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='AvNet', slug='avnet', custom_domain='avnet.example.test')
        self.user = User.objects.create_user(username='avuser', password='pw')
        self.membership = NetworkMembership.objects.create(
            user=self.user, network=self.network, preferred_avatar_source='discord')
        self.client.force_login(self.user)
        self.host = 'avnet.example.test'
        self.url = reverse('update_avatar_preference')

    def _post(self, source):
        return self.client.post(self.url, {'source': source}, HTTP_HOST=self.host)

    def test_every_model_choice_is_accepted(self):
        # Regression guard: the view used to hardcode patreon/discord/custom, so
        # 'gravatar' 400'd even though the UI offers it and display_avatar
        # honours it. Drive the list off the model so they can't drift again.
        for source, _label in NetworkMembership.AVATAR_CHOICES:
            with self.subTest(source=source):
                resp = self._post(source)
                self.assertEqual(resp.status_code, 200)
                self.membership.refresh_from_db()
                self.assertEqual(self.membership.preferred_avatar_source, source)

    def test_response_carries_the_new_avatar_url(self):
        resp = self._post('gravatar')
        body = resp.json()
        self.assertEqual(body['status'], 'success')
        # Non-empty and the gravatar URL the new preference resolves to, so the
        # navbar push has something to use.
        self.assertIn('avatar_url', body)
        self.assertIn('gravatar.com', body['avatar_url'])

    def test_invalid_source_rejected(self):
        resp = self._post('myspace')
        self.assertEqual(resp.status_code, 400)
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.preferred_avatar_source, 'discord')

    def test_navbar_avatar_has_the_push_target_id(self):
        # The push is a no-op without this id on the nav <img>.
        resp = self.client.get(reverse('user_profile'), HTTP_HOST=self.host)
        self.assertIn('id="navbarAvatar"', resp.content.decode('utf-8'))


class LazyPaneBoostTargetTests(TestCase):
    """_lazy_pane must hand #boosted-region down to the forms/links in the loaded
    tab body, so a boosted action inside a creator tab (e.g. approving a
    community edit) does a clean #boosted-region swap instead of htmx's boosted
    <body> default — which innerHTML-swaps the whole body and destroys the
    out-of-region floating player (audio stops, player renders blank)."""

    def _render(self, active_tab='inbox'):
        from django.template.loader import render_to_string
        net = Network.objects.create(name='LP', slug='lp-pane')
        req = RequestFactory().get('/creator/settings/?tab=inbox')
        return render_to_string('pod_manager/creator_tabs/_lazy_pane.html', {
            'tab_name': 'inbox', 'pane_id': 'list-inbox', 'link_id': 'list-inbox-list',
            'active_tab': active_tab, 'current_network': net,
        }, request=req)

    def test_outer_pane_hands_region_target_to_children(self):
        html = self._render()
        # Children of the pane inherit the region swap target — a boosted form in
        # the loaded body swaps #boosted-region, preserving the floating player.
        self.assertIn('hx-target="#boosted-region"', html)
        self.assertIn('hx-select="#boosted-region"', html)

    def test_inner_loader_self_targets_the_pane(self):
        html = self._render()
        # The one-shot body GET loads into the pane itself and hides its own swap
        # attrs from the loaded content, so they never cascade to the body.
        self.assertIn('hx-target="#list-inbox"', html)
        self.assertIn('hx-select="unset"', html)
        self.assertIn('hx-disinherit="*"', html)

    def test_pane_does_not_disinherit_the_swap_target(self):
        html = self._render()
        # Regression guard: the old single-div pane disinherited hx-target/select,
        # which is exactly what forced boosted forms to fall back to <body>.
        self.assertNotIn('hx-disinherit="hx-target', html)



@override_settings(ALLOWED_HOSTS=['*'])
class ShowsAccordionPaginationTests(TestCase):
    """S2.1 — Manage Podcasts renders one SHOW_PAGE_SIZE window of collapsed
    headers per request instead of the network's whole roster, and "Load more"
    appends the next window. Filters/sort still run over the FULL set
    server-side, so a search always reflects every show rather than the slice
    already on screen."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='ShowPageNet', slug='showpagenet',
            custom_domain='showpagenet.example.test')
        self.user = User.objects.create_user(username='showowner', password='pw')
        self.network.owners.add(self.user)
        self.client.force_login(self.user)
        self.host = 'showpagenet.example.test'
        # One full window plus a partial second one.
        self.total = SHOW_PAGE_SIZE + 5
        for i in range(self.total):
            Podcast.objects.create(
                network=self.network, title='Show %03d' % i, slug='showpage-%d' % i)
        self.url = reverse('creator_tab_partial', args=['shows'])

    def _get(self, **params):
        params.setdefault('network', self.network.slug)
        return self.client.get(self.url, params, HTTP_HOST=self.host,
                               HTTP_HX_REQUEST='true')

    def _row_count(self, body):
        return body.count('class="accordion-item bg-black')

    def test_first_load_renders_only_one_window(self):
        body = self._get().content.decode('utf-8')
        self.assertEqual(self._row_count(body), SHOW_PAGE_SIZE)
        # ...and advertises the rest behind the load-more control.
        self.assertIn('Load more shows (5 remaining)', body)

    def test_load_more_returns_the_next_window_as_rows_only(self):
        body = self._get(show_page=2).content.decode('utf-8')
        self.assertEqual(self._row_count(body), 5)
        # An append must carry NONE of the tab's one-time chrome, or the swap
        # would duplicate the filter form / modal inside the live accordion.
        self.assertNotIn('id="manage-podcasts-form"', body)
        self.assertNotIn('id="addShowModal"', body)
        self.assertNotIn('id="showsAccordion"', body)
        # Last window: nothing left to load.
        self.assertNotIn('Load more shows', body)

    def test_load_more_button_self_targets_and_unsets_the_region_select(self):
        body = self._get().content.decode('utf-8')
        # outerHTML on itself: the button is REPLACED by the next window, so the
        # rows land in #showsAccordion and inherit the pane's region target
        # rather than anything of the button's.
        self.assertIn('hx-target="this" hx-swap="outerHTML"', body)
        # ...which is only true if the button has NO wrapper. outerHTML replaces
        # the button alone: a wrapper would survive and the appended rows would
        # render inside it. A .text-center wrapper here centre-aligned every
        # loaded-more show form, so the button must centre itself instead.
        self.assertNotIn('<div class="text-center py-3">', body)
        self.assertIn('d-block mx-auto', body)
        # Without unset, the inherited hx-select="#boosted-region" would look for
        # a region in a rows-only response and swap in nothing.
        self.assertIn('hx-select="unset"', body)
        self.assertIn('show_page=2', body)

    def test_filters_run_over_the_full_set_not_the_loaded_window(self):
        # 'Show 029' sorts past the first window; a search must still find it.
        body = self._get(show_q='Show 029').content.decode('utf-8')
        self.assertEqual(self._row_count(body), 1)
        self.assertIn('Show 029', body)
        self.assertNotIn('Load more shows', body)

    def test_load_more_preserves_the_active_filter(self):
        # The next window must page through the FILTERED set, not the roster.
        body = self._get(show_q='Show 0').content.decode('utf-8')
        self.assertIn('show_q=Show', body.replace('%20', ' ').replace('+', ' '))

    def test_paging_is_stable_across_windows(self):
        # Every sort carries an 'id' tiebreaker; without one, equal
        # latest_episode_date / episode_count values (all null/zero here) let the
        # DB reorder rows between requests, so a window drops or repeats shows.
        for sort in ('alpha', 'recent', 'oldest', 'count_desc'):
            with self.subTest(sort=sort):
                seen = []
                for page in (1, 2):
                    body = self._get(show_sort=sort, show_page=page).content.decode('utf-8')
                    seen += re.findall(r'id="heading-(\d+)"', body)
                self.assertEqual(len(seen), self.total)
                self.assertEqual(len(set(seen)), self.total)

    def test_show_form_loader_hands_the_region_down_to_the_loaded_form(self):
        # Divergence C: the show form is a boosted POST. It must inherit
        # #boosted-region from the OUTER div — when the loader's self-targeting
        # attrs sat on the pane itself, the form inherited hx-target="this"
        # (resolving to the loader) with hx-select unset, so saving a show
        # innerHTML'd the entire creator page into the accordion body.
        body = self._get().content.decode('utf-8')
        self.assertIn('hx-target="#boosted-region"', body)
        self.assertIn('hx-disinherit="*"', body)
        first = Podcast.objects.filter(network=self.network).order_by('title').first()
        self.assertIn('hx-target="#show-form-%d"' % first.id, body)


@override_settings(ALLOWED_HOSTS=['*'])
class AuditLogSummaryTests(TestCase):
    """S2.2 — the audit tab renders collapsed one-line summaries; each edit's
    before/after diff is fetched on expand by creator_audit_edit."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='AuditNet', slug='auditnet',
            custom_domain='auditnet.example.test')
        self.owner = User.objects.create_user(username='auditowner', password='pw')
        self.network.owners.add(self.owner)
        self.editor = User.objects.create_user(username='audieditor', password='pw')
        NetworkMembership.objects.create(
            user=self.editor, network=self.network, trust_score=9)
        self.podcast = Podcast.objects.create(
            network=self.network, title='Audit Show', slug='audit-show')
        self.episode = Episode.objects.create(
            podcast=self.podcast, title='Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/a.mp3',
        )
        self.edit = EpisodeEditSuggestion.objects.create(
            episode=self.episode, user=self.editor,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': 'BeforeTitleMarker', 'tags': ['old'], 'chapters': []},
            suggested_data={'title': 'AfterTitleMarker', 'tags': ['new'], 'chapters': []},
            points=2, counter_deltas={'edits_title': 1},
            resolved_at=timezone.now(),
        )
        self.client.force_login(self.owner)
        self.host = 'auditnet.example.test'
        self.tab_url = reverse('creator_tab_partial', args=['audit'])
        self.diff_url = reverse('creator_audit_edit', args=[self.edit.id])

    def _tab(self):
        return self.client.get(self.tab_url, {'network': self.network.slug},
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')

    def _diff(self, url=None):
        return self.client.get(url or self.diff_url, {'network': self.network.slug},
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')

    def test_tab_renders_the_summary_without_the_diff(self):
        body = self._tab().content.decode('utf-8')
        # Summary facts: who / what / when / status / net trust.
        self.assertIn('Audit Show', body)
        self.assertIn('audieditor', body)
        self.assertIn('Applied', body)
        # ...but none of the edit's actual before/after payload.
        self.assertNotIn('BeforeTitleMarker', body)
        self.assertNotIn('AfterTitleMarker', body)

    def test_tab_defers_each_diff_to_the_lazy_endpoint(self):
        body = self._tab().content.decode('utf-8')
        self.assertIn(self.diff_url, body)
        self.assertIn('shown.bs.collapse from:closest .accordion-collapse once', body)
        self.assertIn('hx-push-url="false"', body)

    def test_diff_endpoint_renders_the_before_and_after(self):
        body = self._diff().content.decode('utf-8')
        self.assertIn('BeforeTitleMarker', body)
        self.assertIn('AfterTitleMarker', body)
        self.assertIn('diff-wrapper', body)

    def test_diff_endpoint_carries_the_trust_math_and_revert_action(self):
        body = self._diff().content.decode('utf-8')
        # The revert control and its exact-wash math live in the diff body now.
        self.assertIn('rollback_single_edit', body)
        self.assertIn('Trust:', body)

    def test_diff_body_hands_boosted_forms_the_region(self):
        # Divergence C: the Revert / Bulk Rollback forms inside the loaded diff
        # must boost against #boosted-region, not htmx's <body> default — a body
        # swap destroys the out-of-region floating player mid-playback.
        body = self._tab().content.decode('utf-8')
        self.assertIn('hx-target="#boosted-region"', body)
        self.assertIn('hx-target="#auditBody%d"' % self.edit.id, body)
        self.assertIn('hx-disinherit="*"', body)
        self.assertNotIn('hx-disinherit="hx-target', body)

    def test_diff_endpoint_404s_for_an_edit_outside_the_network(self):
        other_net = Network.objects.create(name='Other', slug='othernet')
        other_pod = Podcast.objects.create(
            network=other_net, title='Other Show', slug='other-show')
        other_ep = Episode.objects.create(
            podcast=other_pod, title='Other Ep', pub_date=timezone.now(),
            raw_description='x', clean_description='x',
            audio_url_public='https://cdn.example.com/b.mp3',
        )
        foreign = EpisodeEditSuggestion.objects.create(
            episode=other_ep, user=self.editor,
            status=EpisodeEditSuggestion.Status.APPROVED,
            original_data={'title': 'a'}, suggested_data={'title': 'b'},
            points=1, resolved_at=timezone.now(),
        )
        resp = self._diff(url=reverse('creator_audit_edit', args=[foreign.id]))
        self.assertEqual(resp.status_code, 404)

    def test_diff_endpoint_403s_for_a_non_owner(self):
        self.client.force_login(self.editor)  # a member, but owns no network
        self.assertEqual(self._diff().status_code, 403)

    def test_diff_endpoint_requires_login(self):
        self.client.logout()
        resp = self._diff()
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp['Location'])

    def test_summary_render_does_not_annotate_the_diff(self):
        # The point of S2.2: gather_audit_log must not walk suggested_data or
        # query memberships for edits nobody expanded.
        from pod_manager.views.creator import data as creator_data
        req = RequestFactory().get('/creator/tab/audit/')
        req.user = self.owner
        ctx = creator_data.gather_audit_log(req, self.network)
        edit = list(ctx['audit_page_obj'])[0]
        self.assertEqual(edit.audit_points, 2)
        self.assertFalse(hasattr(edit, 'title_changed'))
        self.assertFalse(hasattr(edit, 'membership'))

    def test_rejected_and_rolled_back_summaries_score_without_the_diff(self):
        self.edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        self.edit.save()
        self.assertIn('+0', self._tab().content.decode('utf-8'))

        self.edit.status = EpisodeEditSuggestion.Status.REJECTED
        self.edit.save()
        self.assertIn('-%d' % REJECT_PENALTY, self._tab().content.decode('utf-8'))


class BaseTemplateContractTests(SimpleTestCase):
    """S1.5 removed hx-select="#boosted-region" from base.html's boost wrapper.
    That attribute was a no-op (an HX response IS the region), but it was also an
    accidental safety net: had any template still returned a whole document on an
    HX request, hx-select would have quietly pulled the region out of it. Without
    it, a boosted nav swaps the response WHOLE — so a template that renders the
    full page on an HX request would nest an entire <html> inside the region.

    These pin the contract that makes the removal safe, so it can't rot by
    someone adding a template the ordinary way."""

    @property
    def TEMPLATE_ROOT(self):
        from django.conf import settings as django_settings
        return Path(django_settings.BASE_DIR) / 'pod_manager' / 'templates'

    def _templates(self):
        return sorted(self.TEMPLATE_ROOT.rglob('*.html'))

    def test_no_template_hardcodes_the_full_base(self):
        # Every template in the app shell must go through base_template so an HX
        # request renders base_htmx.html (the region alone). Hardcoding base.html
        # would return a whole document to a boosted nav.
        offenders = []
        for path in self._templates():
            text = path.read_text(encoding='utf-8')
            for m in re.finditer(r'{%\s*extends\s+([^%]+?)\s*%}', text):
                arg = m.group(1).strip()
                if re.fullmatch(r"""['"]pod_manager/base(_htmx)?\.html['"]""", arg):
                    offenders.append(f'{path.relative_to(self.TEMPLATE_ROOT)}: {{% extends {arg} %}}')
        self.assertEqual(offenders, [], msg=(
            'These templates hardcode a base instead of '
            "{% extends base_template|default:'pod_manager/base.html' %}. "
            'A boosted nav to one would swap a whole <html> into #boosted-region '
            '(see S1.5 in planned_features_htmx_base_swap.txt):\n  '
            + '\n  '.join(offenders)))

    def test_every_extending_template_uses_base_template_with_a_default(self):
        # The |default is what keeps a normal (non-HX) request rendering the full
        # page, and what let the rollout land one view at a time.
        bad = []
        for path in self._templates():
            text = path.read_text(encoding='utf-8')
            m = re.search(r'{%\s*extends\s+([^%]+?)\s*%}', text)
            if not m:
                continue                      # partials / standalone pages
            arg = m.group(1).strip()
            if arg.startswith('"admin/') or arg.startswith("'admin/"):
                continue                      # Django admin, boost-opted-out
            if not re.match(r"""base_template\|default:['"]pod_manager/base\.html['"]""", arg):
                bad.append(f'{path.relative_to(self.TEMPLATE_ROOT)}: {arg}')
        self.assertEqual(bad, [], msg='Unexpected {% extends %} target(s):\n  ' + '\n  '.join(bad))

    def test_the_boost_wrapper_does_not_select(self):
        # The point of S1.5. If this comes back, either the removal was reverted
        # or someone re-added a no-op that implies the server returns full pages.
        base = (self.TEMPLATE_ROOT / 'pod_manager' / 'base.html').read_text(encoding='utf-8')
        wrapper = re.search(r'<div hx-boost="true"[^>]*>', base)
        self.assertIsNotNone(wrapper, 'base.html lost its hx-boost wrapper')
        self.assertNotIn('hx-select', wrapper.group(0))
        # ...but it must still target and swap the region.
        self.assertIn('hx-target="#boosted-region"', wrapper.group(0))
        self.assertIn('hx-swap="outerHTML show:window:top"', wrapper.group(0))

    def test_lazy_loaders_still_unset_the_select_they_no_longer_inherit(self):
        # hx-select="unset" on a partial loader is NOT made redundant by S1.5:
        # _lazy_pane and the other split loaders set hx-select="#boosted-region"
        # on their own outer element, so their children still inherit a select
        # that must be unset for a bare-fragment response.
        pane = (self.TEMPLATE_ROOT / 'pod_manager' / 'creator_tabs' / '_lazy_pane.html').read_text(encoding='utf-8')
        self.assertIn('hx-select="#boosted-region"', pane)
        self.assertIn('hx-select="unset"', pane)


@override_settings(ALLOWED_HOSTS=['*'])
class MixFormLazyLoadTests(TestCase):
    """S2.3 — the Mixes tab renders cards; each mix's edit form (and its show
    picker) loads on modal open via creator_mix_form. Rendering a curation form
    per mix up front was most of that tab's weight."""

    def setUp(self):
        cache.clear()
        self.network = Network.objects.create(
            name='MixNet', slug='mixnet', custom_domain='mixnet.example.test')
        self.owner = User.objects.create_user(username='mixowner', password='pw')
        self.network.owners.add(self.owner)
        self.pods = [
            Podcast.objects.create(network=self.network, title='MixShow %d' % i,
                                   slug='mixnet-show-%d' % i)
            for i in range(4)
        ]
        self.mix = NetworkMix.objects.create(
            network=self.network, name='My Mix', slug='my-mix')
        self.mix.selected_podcasts.set(self.pods[:3])
        self.client.force_login(self.owner)
        self.host = 'mixnet.example.test'
        self.tab_url = reverse('creator_tab_partial', args=['mixes'])
        self.form_url = reverse('creator_mix_form', args=[self.mix.id])

    def _tab(self):
        return self.client.get(self.tab_url, {'network': self.network.slug},
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')

    def _form(self, url=None):
        return self.client.get(url or self.form_url, {'network': self.network.slug},
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')

    def test_tab_renders_the_card_without_the_edit_form(self):
        body = self._tab().content.decode('utf-8')
        self.assertIn('My Mix', body)
        # The card's own facts stay eager...
        self.assertIn('3 Shows Included', body)
        # ...but the edit form does not render until the modal opens.
        self.assertNotIn('edit_network_mix', body)

    def test_card_count_survived_the_switch_off_selected_ids(self):
        # Regression guard: the card used {{ mix.selected_ids|length }}, and
        # gather_mixes no longer sets selected_ids. A missing attribute renders
        # as an empty string in a Django template, so |length would have shown
        # "0 Shows Included" silently rather than raising.
        body = self._tab().content.decode('utf-8')
        self.assertNotIn('0 Shows Included', body)
        self.assertIn('3 Shows Included', body)

    def test_tab_defers_the_form_to_the_modal_open(self):
        body = self._tab().content.decode('utf-8')
        self.assertIn(self.form_url, body)
        self.assertIn('show.bs.modal from:#editNetworkMixModal-%d once' % self.mix.id, body)
        self.assertIn('hx-select="unset"', body)

    def test_form_endpoint_renders_the_mix_with_its_selection(self):
        body = self._form().content.decode('utf-8')
        self.assertIn('edit_network_mix', body)
        self.assertIn('value="My Mix"', body)
        # The picker pre-checks the mix's three shows and not the fourth.
        self.assertEqual(body.count('checked'), 3)

    def test_form_loader_hands_boosted_children_the_region(self):
        # The mix form is hx-boost="false" today (multipart), so nothing inherits
        # — but the split is what keeps this correct if that opt-out ever goes.
        body = self._tab().content.decode('utf-8')
        self.assertIn('hx-target="#boosted-region"', body)
        self.assertIn('hx-target="#mix-form-%d"' % self.mix.id, body)
        self.assertIn('hx-disinherit="*"', body)

    def test_form_endpoint_404s_for_a_mix_outside_the_network(self):
        other = Network.objects.create(name='OtherMixNet', slug='othermixnet')
        foreign = NetworkMix.objects.create(network=other, name='Foreign', slug='foreign')
        resp = self._form(url=reverse('creator_mix_form', args=[foreign.id]))
        self.assertEqual(resp.status_code, 404)

    def test_form_endpoint_403s_for_a_non_owner(self):
        stranger = User.objects.create_user(username='mixstranger', password='pw')
        self.client.force_login(stranger)
        self.assertEqual(self._form().status_code, 403)

    def test_form_endpoint_requires_login(self):
        self.client.logout()
        resp = self._form()
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp['Location'])

    def test_tab_keeps_the_shared_options_template(self):
        # The picker clones its full option list from this; it must stay in the
        # tab body, since a lazily-loaded modal is not present at tab render.
        self.assertIn('data-sms-options-template', self._tab().content.decode('utf-8'))


@override_settings(ALLOWED_HOSTS=['*'])
class InTabFilterPaneSwapTests(TestCase):
    """S2.4 — the in-tab filters re-render only their own pane instead of doing a
    full round-trip through the settings shell.

    The two source-podcast selectors were worse than slow: they were
    onchange="window.location.href='?tab=move&source_pod_id='+value", a HARD
    navigation, which destroyed the out-of-region floating player mid-playback
    AND dropped ?network= from the URL — so on a multi-network account choosing a
    source silently bounced the owner to their first network."""

    def setUp(self):
        cache.clear()
        # Another network the owner also has. Network has no Meta.ordering, so
        # allowed_networks.first() is effectively lowest-PK — create this one
        # FIRST so it is what the resolver falls back to when ?network= is
        # missing, which is the bug the selectors used to cause.
        self.other = Network.objects.create(name='OtherPaneNet', slug='other-panenet')
        self.network = Network.objects.create(
            name='PaneNet', slug='panenet', custom_domain='panenet.example.test')
        self.owner = User.objects.create_user(username='paneowner', password='pw')
        self.other.owners.add(self.owner)
        self.network.owners.add(self.owner)
        self.podcast = Podcast.objects.create(
            network=self.network, title='PaneShow', slug='panenet-show')
        self.client.force_login(self.owner)
        self.host = 'panenet.example.test'

    def _tab(self, tab):
        return self.client.get(reverse('creator_tab_partial', args=[tab]),
                               {'network': self.network.slug},
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')

    def test_move_selector_swaps_its_own_pane_and_keeps_the_network(self):
        body = self._tab('move').content.decode('utf-8')
        self.assertNotIn('window.location.href', body)      # no hard navigation
        self.assertIn(reverse('creator_tab_partial', args=['move']), body)
        self.assertIn('network=%s' % self.network.slug, body)  # the dropped param
        self.assertIn('hx-target="#list-move"', body)
        self.assertIn('hx-select="unset"', body)
        self.assertIn('name="source_pod_id"', body)

    def test_crosspub_selector_swaps_its_own_pane_and_keeps_the_network(self):
        body = self._tab('crosspub').content.decode('utf-8')
        self.assertNotIn('window.location.href', body)
        self.assertIn(reverse('creator_tab_partial', args=['crosspub']), body)
        self.assertIn('network=%s' % self.network.slug, body)
        self.assertIn('hx-target="#list-crosspub"', body)
        self.assertIn('name="cross_source_id"', body)

    def test_audit_search_swaps_its_own_pane(self):
        body = self._tab('audit').content.decode('utf-8')
        self.assertIn(reverse('creator_tab_partial', args=['audit']), body)
        self.assertIn('hx-target="#list-audit"', body)
        self.assertIn('hx-push-url="false"', body)

    def test_audit_search_keeps_its_no_js_fallback(self):
        # htmx intercepts the submit when present; without JS the form must still
        # navigate to the full page ON THE AUDIT TAB, which the hidden tab does.
        body = self._tab('audit').content.decode('utf-8')
        self.assertIn('method="GET"', body)
        self.assertIn('name="tab" value="audit"', body)

    def test_the_selectors_actually_filter_through_the_router(self):
        # Drive the request the swap makes and prove the pane comes back scoped
        # to the chosen source — and to the RIGHT network.
        resp = self.client.get(
            reverse('creator_tab_partial', args=['move']),
            {'network': self.network.slug, 'source_pod_id': str(self.podcast.id)},
            HTTP_HOST=self.host, HTTP_HX_REQUEST='true')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['current_network'], self.network)
        self.assertEqual(resp.context['source_pod_id'], str(self.podcast.id))

    def test_dropping_the_network_param_would_pick_the_wrong_network(self):
        # Pins WHY the selectors must carry ?network=: without it the resolver
        # falls back to allowed_networks.first(), which is a different network.
        resp = self.client.get(reverse('creator_tab_partial', args=['move']),
                               HTTP_HOST=self.host, HTTP_HX_REQUEST='true')
        self.assertEqual(resp.context['current_network'], self.other)
        self.assertNotEqual(resp.context['current_network'], self.network)


class BoostOptOutContractTests(SimpleTestCase):
    """hx-boost="false" makes a form hard-navigate, which reloads the page and
    destroys the out-of-region floating player — audio stops mid-episode. That is
    the one thing the boosted-navigation system exists to prevent, so every
    remaining opt-out must be a deliberate, listed choice rather than a default.

    Uploads are NOT a reason to opt out: hx-encoding="multipart/form-data" makes
    htmx send the form as FormData over XHR, so it boosts like anything else.
    Modals are not a reason either: base.html disposes an open modal on region
    swap so its backdrop can't outlive it."""

    # Opt-outs that are correct, with the reason each is allowed to hard-navigate.
    # Currently empty: NO form needs to. Uploads use hx-encoding, and the modal
    # backdrop problem is solved centrally in base.html. Adding an entry here is
    # a deliberate decision to stop someone's audio — justify it.
    ALLOWED_FORM_OPT_OUTS = {}

    @property
    def TEMPLATE_ROOT(self):
        from django.conf import settings as django_settings
        return Path(django_settings.BASE_DIR) / 'pod_manager' / 'templates'

    def _opted_out_forms(self):
        found = []
        for path in sorted(self.TEMPLATE_ROOT.rglob('*.html')):
            text = path.read_text(encoding='utf-8')
            for m in re.finditer(r'<form[^>]*>', text):
                if 'hx-boost="false"' in m.group(0):
                    found.append((path.name, text[:m.start()].count('\n') + 1))
        return found

    def test_no_creator_form_hard_navigates(self):
        # The creator tabs are where this hurt most: saving a mix, merging
        # episodes and editing network settings all stopped playback.
        offenders = [
            (name, line) for name, line in self._opted_out_forms()
            if name.startswith('tab_') or name.startswith('_') or name == 'creator_settings.html'
        ]
        self.assertEqual(offenders, [], msg=(
            'These creator forms still hx-boost="false" and will hard-navigate, '
            'killing audio. Use hx-encoding="multipart/form-data" if it is an '
            f'upload: {offenders}'))

    def test_every_remaining_opt_out_is_a_listed_exception(self):
        unexpected = [
            (name, line) for name, line in self._opted_out_forms()
            if name not in self.ALLOWED_FORM_OPT_OUTS
        ]
        self.assertEqual(unexpected, [], msg=(
            'New hx-boost="false" form(s) that would hard-navigate and stop '
            'playback. If it is genuinely needed, add it to '
            f'ALLOWED_FORM_OPT_OUTS with a reason: {unexpected}'))

    def test_uploads_that_boost_declare_hx_encoding(self):
        # A multipart form that neither opts out NOR sets hx-encoding is the
        # broken middle: htmx would boost it and drop the file.
        broken = []
        for path in sorted(self.TEMPLATE_ROOT.rglob('*.html')):
            text = path.read_text(encoding='utf-8')
            for m in re.finditer(r'<form[^>]*>', text):
                t = m.group(0)
                if 'multipart/form-data' not in t:
                    continue
                if 'hx-boost="false"' in t or 'hx-encoding' in t:
                    continue
                broken.append((path.name, text[:m.start()].count('\n') + 1))
        self.assertEqual(broken, [], msg=(
            'Multipart form(s) that boost without hx-encoding — htmx would send '
            f'them url-encoded and silently drop the upload: {broken}'))

    def test_base_disposes_modals_before_a_region_swap(self):
        # Without this, boosting a form inside a modal strands its backdrop
        # (appended to <body>, outside the region) as a dead grey overlay.
        base = (self.TEMPLATE_ROOT / 'pod_manager' / 'base.html').read_text(encoding='utf-8')
        self.assertIn('closeOpenModals', base)
        self.assertIn('.modal-backdrop', base)
        self.assertIn("classList.remove('modal-open')", base)
