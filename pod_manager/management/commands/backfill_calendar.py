"""Backfill the release calendar from already-published episodes.

The live calendar only gains entries going forward (on schedule / publish /
ingest). This command seeds it from the EXISTING back catalog: every selected
episode that isn't already on the calendar gets an entry placed at the moment
it was published (its pub_date), so the calendar and its ICS feed reflect the
show's real release history.

Reuses services.release_calendar.ensure_calendar_entry_for_episode per episode,
so it ADOPTS a matching pre-planned entry when one exists (no duplicate) and is
idempotent — re-running never double-creates. Published episodes only by
default (the ones with a real release date); pass --include-unpublished to also
seed scheduled/draft episodes.

Preview is the default; pass --apply to persist.

    python manage.py backfill_calendar --network=baldmove              # preview
    python manage.py backfill_calendar --network=baldmove --apply
    python manage.py backfill_calendar --podcast=silo --days=90 --apply
    python manage.py backfill_calendar --all --since=2025-01-01 --until=2025-06-30 --apply
    python manage.py backfill_calendar --all --limit=50 --apply         # start small
"""

from datetime import datetime, time, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from pod_manager.admin_console.summary import emit_summary
from pod_manager.models import Episode, Network, Podcast
from pod_manager.services.release_calendar import ensure_calendar_entry_for_episode


class Command(BaseCommand):
    help = ("Seed the release calendar from existing episodes at their pub_date. "
            "Preview by default; pass --apply to save.")

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Target every podcast.")
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--days", type=int,
                             help="Only episodes published within the last N days.")
        parser.add_argument("--since", help="Only episodes with pub_date on/after this date (YYYY-MM-DD).")
        parser.add_argument("--until", help="Only episodes with pub_date on/before this date (YYYY-MM-DD).")
        parser.add_argument("--limit", type=int,
                             help="Cap the number of episodes processed (newest first) — start small.")
        parser.add_argument("--include-unpublished", action="store_true",
                             help="Also seed scheduled/draft episodes (default: published only).")
        parser.add_argument("--apply", action="store_true",
                             help="Persist changes (default is a preview that lists them only).")

    def _parse_date(self, raw, label):
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            raise CommandError(f"--{label} must be a date in YYYY-MM-DD form, got '{raw}'.")

    def handle(self, *args, **options):
        if options["podcast"]:
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
            raise CommandError("Specify a scope: --all / --network=<slug> / --podcast=<slug>.")

        apply_ = options["apply"]
        preview = not apply_

        # Only episodes not already on the calendar are candidates.
        episodes = (
            Episode.objects.filter(podcast__in=podcasts, calendar_entry__isnull=True)
            .select_related('podcast')
        )
        if not options["include_unpublished"]:
            episodes = episodes.filter(is_published=True)

        if options["days"] is not None:
            episodes = episodes.filter(pub_date__gte=timezone.now() - timedelta(days=options["days"]))
        if options["since"]:
            since = self._parse_date(options["since"], "since")
            episodes = episodes.filter(pub_date__gte=timezone.make_aware(datetime.combine(since, time.min)))
        if options["until"]:
            until = self._parse_date(options["until"], "until")
            episodes = episodes.filter(pub_date__lte=timezone.make_aware(datetime.combine(until, time.max)))

        episodes = episodes.order_by('-pub_date')
        if options["limit"] is not None:
            episodes = episodes[:options["limit"]]

        self.stdout.write(
            f"{podcasts.count()} podcast(s) selected; {episodes.count()} candidate episode(s) "
            f"(mode={'preview' if preview else 'apply'}, "
            f"{'published only' if not options['include_unpublished'] else 'incl. unpublished'})."
        )

        created = adopted = skipped = 0

        for ep in episodes:
            target = ep.scheduled_at or ep.pub_date
            if target is None:
                skipped += 1
                continue
            if preview:
                self.stdout.write(
                    f"  would add ep {ep.id} '{ep.title[:50]}' "
                    f"[{ep.podcast.title[:30]}] @ {target:%Y-%m-%d %H:%M} UTC"
                )
                created += 1
                continue
            entry = ensure_calendar_entry_for_episode(ep)
            if entry is None:
                skipped += 1
                continue
            # A pre-existing entry the episode adopted rather than a fresh row.
            if entry.created_at and (timezone.now() - entry.created_at).total_seconds() > 5:
                adopted += 1
            else:
                created += 1
            self.stdout.write(f"  added ep {ep.id} '{ep.title[:50]}' @ {entry.scheduled_at:%Y-%m-%d %H:%M} UTC")

        self.stdout.write(self.style.SUCCESS(
            f"\n{'Would add' if preview else 'Added'} {created} calendar entr(ies)."
            + ("" if preview else f" Adopted {adopted} pre-planned. Skipped {skipped}.")
        ))
        if preview:
            self.stdout.write("Re-run with --apply to perform the changes.")
        emit_summary(self.stdout, {
            "mode": "preview" if preview else "apply",
            "created": created, "adopted": adopted, "skipped": skipped,
        })
