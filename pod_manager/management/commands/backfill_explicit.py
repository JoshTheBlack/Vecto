"""Backfill itunes:explicit (content rating) from RSS onto shows and already-ingested episodes.

Until now ingestion could not read this tag: feedparser only understands the legacy
'yes'/'clean' spellings and returns None for the 'true'/'false' that real feeds use, so no
episode ever received a rating and every generated feed fell back to a hardcoded
explicit=true. The normal sync now reads the raw XML on every ingest; this command applies the
same reading to the EXISTING catalogue immediately instead of waiting for each feed's next
natural resync.

It sets:
  * Podcast.feed_explicit  - the channel-level rating the source feed declares;
  * Episode.explicit       - each item's own rating, where the feed states one.

An episode whose rating an owner set by hand (explicit_locked) is never touched. Where a feed
is silent the existing value is left alone. Preview is the default; --apply persists, then
rebuilds the affected shows' feed fragments so the new ratings are published.

    python manage.py backfill_explicit --all                       # preview, every show
    python manage.py backfill_explicit --network=baldmove --apply
    python manage.py backfill_explicit --podcast=watchmen --apply
    python manage.py backfill_explicit --episode=1234 --apply
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from pod_manager.admin_console.summary import emit_summary
from pod_manager.ingesters.default import _network_base_url, extract_explicit, get_feed
from pod_manager.models import Episode, Network, Podcast


class Command(BaseCommand):
    help = ("Backfill itunes:explicit from RSS onto shows (feed_explicit) and existing episodes. "
            "Preview by default; pass --apply to save.")

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Target every podcast.")
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--episode", type=int, help="Restrict to a single episode id.")
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
        self.stdout.write(f"{podcasts.count()} podcast(s) selected (mode={'apply' if apply_ else 'preview'}).")

        shows_changed = episodes_changed = skipped_locked = unchanged = silent = 0

        for podcast in podcasts.select_related('network'):
            feeds = []
            for url, feed_type in ((podcast.public_feed_url, "PUBLIC"), (podcast.subscriber_feed_url, "PRIVATE")):
                if url:
                    data = get_feed(url, feed_type, podcast.id, self.stdout, force_fetch=True)
                    if data and data != 304 and hasattr(data, 'entries'):
                        feeds.append(data)
            if not feeds:
                continue
            touched = False

            # channel-level rating: the first feed (public preferred) that states one
            declared = next((f.feed.get('vecto_explicit') for f in feeds
                             if f.feed.get('vecto_explicit') is not None), None)
            if declared is not None and podcast.feed_explicit != declared:
                self.stdout.write(f"  {'would set' if not apply_ else 'set'} show '{podcast.title[:50]}' "
                                  f"source rating {podcast.feed_explicit} -> {declared}")
                if apply_:
                    Podcast.objects.filter(pk=podcast.pk).update(feed_explicit=declared)
                shows_changed += 1
                touched = True

            seen = set()
            for data in feeds:                      # public feed first: its value wins for a paired episode
                for entry in data.entries:
                    guid = getattr(entry, 'id', None)
                    if not guid:
                        continue
                    ep_qs = Episode.objects.filter(podcast=podcast).filter(Q(guid_public=guid) | Q(guid_private=guid))
                    if episode_id:
                        ep_qs = ep_qs.filter(pk=episode_id)
                    ep = ep_qs.first()
                    if not ep or ep.id in seen:
                        continue
                    rating = extract_explicit(entry)
                    if rating is None:
                        silent += 1                  # the feed says nothing: leave whatever is there
                        continue
                    seen.add(ep.id)
                    if ep.explicit_locked:
                        skipped_locked += 1
                        continue
                    if ep.explicit == rating:
                        unchanged += 1
                        continue
                    self.stdout.write(f"  {'would set' if not apply_ else 'set'} ep {ep.id} '{ep.title[:50]}' "
                                      f"rating {ep.explicit} -> {rating}")
                    if apply_:
                        Episode.objects.filter(pk=ep.pk).update(explicit=rating)
                    episodes_changed += 1
                    touched = True

            if touched and apply_:
                from pod_manager.tasks import task_rebuild_podcast_fragments
                task_rebuild_podcast_fragments.delay(podcast.id, _network_base_url(podcast.network))

        verb = "Updated" if apply_ else "Would update"
        self.stdout.write(self.style.SUCCESS(
            f"\n{verb} {shows_changed} show rating(s) and {episodes_changed} episode rating(s); "
            f"{unchanged} already correct, {skipped_locked} locked by an owner, "
            f"{silent} where the feed states no rating."
            + ("" if apply_ else " Preview only: re-run with --apply.")))
        emit_summary(self.stdout, {
            "applied": apply_, "shows": shows_changed, "episodes": episodes_changed,
            "locked": skipped_locked, "unchanged": unchanged, "silent": silent,
        })
