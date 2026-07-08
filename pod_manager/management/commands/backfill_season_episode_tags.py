"""Backfill itunes:season / itunes:episode / itunes:episodeType from RSS onto
already-ingested episodes.

The normal feed sync (pod_manager.ingesters.default) now pulls these fields
on every ingest going forward, gated by the same is_metadata_locked lock as
title/description/etc. This command re-fetches feeds and applies the same
extraction to the EXISTING back catalog immediately, instead of waiting for
each podcast's next natural resync.

Only touches season_number / episode_number / episode_type — no other field.
Preview is the default; pass --apply to persist. Locked episodes are skipped
by default (matching normal ingest behavior) — the ingest log lines already
say "not applied" for these with the episode ID; pass --bypass-lock (usually
combined with --episode=<id>) to force one:

    python manage.py backfill_season_episode_tags                       # preview, all networks
    python manage.py backfill_season_episode_tags --network=baldmove --apply
    python manage.py backfill_season_episode_tags --podcast=watchmen --apply --force
    python manage.py backfill_season_episode_tags --episode=1234 --bypass-lock --apply
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from pod_manager.admin_console.summary import emit_summary
from pod_manager.ingesters.default import extract_season_episode, get_feed
from pod_manager.models import Episode, Network, Podcast


class Command(BaseCommand):
    help = ("Backfill season_number/episode_number/episode_type from RSS onto existing "
            "episodes. Preview by default; pass --apply to save.")

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Target every podcast.")
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--episode", type=int, help="Restrict to a single episode id.")
        parser.add_argument("--force", action="store_true",
                             help="Also reprocess episodes that already have both "
                                  "season_number and episode_number set (default: fill in "
                                  "missing values only).")
        parser.add_argument("--bypass-lock", action="store_true",
                             help="Overwrite even is_metadata_locked episodes. Normal ingest "
                                  "and this command's default both skip locked episodes.")
        parser.add_argument("--apply", action="store_true",
                             help="Persist changes (default is a preview that lists them only).")

    def handle(self, *args, **options):
        episode_id = options["episode"]
        if episode_id:
            episode = Episode.objects.select_related('podcast').filter(pk=episode_id).first()
            if not episode:
                raise CommandError(f"No episode with id {episode_id}")
            podcasts = Podcast.objects.filter(pk=episode.podcast_id)
        elif options["podcast"]:
            podcasts = Podcast.objects.filter(slug=options["podcast"])
            if not podcasts.exists():
                raise CommandError(f"No podcast with slug '{options['podcast']}'")
        elif options["network"]:
            network = Network.objects.filter(slug=options["network"]).first()
            if not network:
                raise CommandError(f"No network with slug '{options['network']}'")
            podcasts = Podcast.objects.filter(network=network)
        elif options["all"]:
            podcasts = Podcast.objects.all()
        else:
            raise CommandError("Specify --episode, or a scope: --all / --network=<slug> / --podcast=<slug>.")

        apply_ = options["apply"]
        force = options["force"]
        bypass_lock = options["bypass_lock"]
        preview = not apply_

        self.stdout.write(
            f"{podcasts.count()} podcast(s) selected "
            f"(mode={'preview' if preview else 'apply'}, force={force}, bypass_lock={bypass_lock})."
        )

        updated = skipped_locked = skipped_unchanged = 0

        for podcast in podcasts:
            entries = []
            for url, feed_type in ((podcast.public_feed_url, "PUBLIC"), (podcast.subscriber_feed_url, "PRIVATE")):
                if not url:
                    continue
                data = get_feed(url, feed_type, podcast.id, self.stdout, force_fetch=True)
                if data and data != 304 and hasattr(data, 'entries'):
                    entries.extend(data.entries)

            if not entries:
                continue

            seen_episode_ids = set()
            for entry in entries:
                guid = getattr(entry, 'id', None)
                if not guid:
                    continue
                ep_qs = Episode.objects.filter(podcast=podcast).filter(
                    Q(guid_public=guid) | Q(guid_private=guid)
                )
                if episode_id:
                    ep_qs = ep_qs.filter(pk=episode_id)
                ep = ep_qs.first()
                if not ep or ep.id in seen_episode_ids:
                    continue

                season, episode_num, episode_type = extract_season_episode(entry)
                if season is None and episode_num is None and not episode_type:
                    continue
                seen_episode_ids.add(ep.id)

                if ep.is_metadata_locked and not bypass_lock:
                    self.stdout.write(
                        f"  [LOCKED] ep {ep.id} '{ep.title[:50]}' — "
                        f"season {ep.season_number}->{season}, episode {ep.episode_number}->{episode_num}, "
                        f"skipped (--bypass-lock to force)"
                    )
                    skipped_locked += 1
                    continue

                if not force and ep.season_number is not None and ep.episode_number is not None:
                    skipped_unchanged += 1
                    continue

                new_type = episode_type or ep.episode_type
                if (season, episode_num, new_type) == (ep.season_number, ep.episode_number, ep.episode_type):
                    skipped_unchanged += 1
                    continue

                if preview:
                    self.stdout.write(
                        f"  would update ep {ep.id} '{ep.title[:50]}': "
                        f"season {ep.season_number}->{season}, episode {ep.episode_number}->{episode_num}, "
                        f"type '{ep.episode_type}'->'{new_type}'"
                    )
                else:
                    ep.season_number = season
                    ep.episode_number = episode_num
                    ep.episode_type = new_type
                    ep.save(update_fields=['season_number', 'episode_number', 'episode_type'])
                    from django.core.cache import cache
                    cache.delete(f"ep_frag_public_{ep.id}")
                    cache.delete(f"ep_frag_private_{ep.id}")
                    self.stdout.write(f"  updated ep {ep.id} '{ep.title[:50]}'")
                updated += 1

        self.stdout.write(self.style.SUCCESS(
            f"\n{'Would update' if preview else 'Updated'} {updated} episode(s). "
            f"Skipped: {skipped_locked} locked, {skipped_unchanged} unchanged/already-set."
        ))
        emit_summary(self.stdout, {
            "mode": "preview" if preview else "apply",
            "updated": updated, "skipped_locked": skipped_locked, "skipped_unchanged": skipped_unchanged,
        })
