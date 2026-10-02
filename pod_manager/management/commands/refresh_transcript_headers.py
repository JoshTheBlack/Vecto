"""Bring stale .words recovery headers up to date with their episodes.

Every transcript's .words object starts with a header (episode_id, title, guid_public,
guid_private, audio_url) that `recover_transcripts` matches on. Titles, GUIDs and audio
URLs change after a transcript is made (edits, approvals, feed polls, merges), so this
refreshes the header — and ONLY the header — of each transcript whose episode data no
longer matches what was last written. It is not a re-transcription and does not bump the
version.

Finding the stale ones is a database-only comparison against Transcript.header_stamp; R2
is touched only for those (one read, plus one write only if the header really differs).
A nightly Celery task runs the same sweep (500 per night); this command is for running it
on demand, e.g. after a merge session or to drain the first sweep over the existing catalog.

    python manage.py refresh_transcript_headers                    # dry run: how many are stale
    python manage.py refresh_transcript_headers --apply            # refresh them all
    python manage.py refresh_transcript_headers --apply --limit 200
    python manage.py refresh_transcript_headers --apply --network <slug>

Requires R2_MEDIA_ENABLED=True.
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.transcript_headers import refresh_transcript_headers


class Command(BaseCommand):
    help = ("Refresh the .words recovery header of transcripts whose episode title/GUIDs/audio "
            "URL changed. Database-only to find them; R2 only for the stale ones. Dry run by default.")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Perform the refresh (default is a dry run that only counts).")
        parser.add_argument("--network", help="Network slug to scope to.")
        parser.add_argument("--podcast", help="Podcast slug to scope to.")
        parser.add_argument("--limit", type=int, help="Refresh at most N transcripts.")

    def handle(self, *args, **options):
        from pod_manager import models
        for flag, model_name, label in ((options["network"], "Network", "network"),
                                        (options["podcast"], "Podcast", "podcast")):
            if flag and not getattr(models, model_name).objects.filter(slug=flag).exists():
                raise CommandError(f"No {label} with slug '{flag}'.")
        try:
            report = refresh_transcript_headers(
                network_slug=options["network"], podcast_slug=options["podcast"],
                limit=options["limit"], apply=options["apply"])
        except RuntimeError as exc:
            raise CommandError(str(exc))

        w = self.stdout.write
        w(f"{report['checked']} R2-resident transcript(s) checked against their episodes; "
          f"{report['stale']} stale (stamp missing or out of date).")
        from pod_manager.admin_console.summary import emit_summary
        if not report["applied"]:
            w(f"Would refresh {report['batch']}"
              + (f" (of {report['stale']}; --limit {options['limit']})" if report["batch"] != report["stale"] else "")
              + ". Each costs one R2 read, plus one write only if its header actually differs.")
            w(self.style.WARNING("Dry run — nothing changed. Re-run with --apply."))
            emit_summary(self.stdout, {"applied": False, "checked": report["checked"],
                                       "stale": report["stale"], "batch": report["batch"]})
            return
        w(self.style.SUCCESS(
            f"Refreshed {report['refreshed']} header(s); {report['stamped']} were already correct "
            f"(stamp recorded, no write); {report['unchanged']} already current; "
            f"{report['skipped']} skipped; {report['errors']} error(s)."))
        if report["errors"]:
            w(self.style.WARNING("Errors leave the stamp untouched, so the next run retries them."))
        emit_summary(self.stdout, {"applied": True, "checked": report["checked"], "stale": report["stale"],
                                   "refreshed": report["refreshed"], "stamped": report["stamped"],
                                   "errors": report["errors"]})
