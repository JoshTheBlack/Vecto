"""Churn legacy transcripts to keyed R2 locations (transcript plan, section E2).

Every new transcription is born with a random r2_key_token; this command moves
the existing catalog off the deterministic (fuzzable) keys. Per transcript,
strict order: record-orphan -> copy -> set token -> delete old objects -> purge
their CDN URLs — a crash at any point leaves a durable orphan-row retry record
and a rerun converges. Idempotent: tokened rows are skipped. Not destructive:
the "delete" removes the old duplicate only after a byte-identical copy is live
at the keyed location.

Run --podcast <slug> immediately after flipping a feed's allow_public_transcripts
off — until churned, an untokened transcript is still fuzzable at its plain key.

    python manage.py rekey_transcripts                            # dry run: list candidates
    python manage.py rekey_transcripts --apply                    # churn everything
    python manage.py rekey_transcripts --podcast <slug> --apply   # one feed (flag flip)
    python manage.py rekey_transcripts --limit 200 --apply        # batched churn
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import rekey_transcripts

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = ("Move legacy transcripts to keyed (non-derivable) R2 object keys, "
            "deleting + CDN-purging the old plain keys once the copy is live. "
            "Idempotent; dry run by default.")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Perform the rekey (default is a dry run that "
                                 "only lists candidates).")
        parser.add_argument("--podcast",
                            help="Podcast slug to scope the churn to.")
        parser.add_argument("--limit", type=int,
                            help="Stop after N transcripts rekeyed.")

    def handle(self, *args, **options):
        apply = options["apply"]
        slug = options["podcast"]
        if slug:
            from pod_manager.models import Podcast
            if not Podcast.objects.filter(slug=slug).exists():
                raise CommandError(f"No podcast with slug '{slug}'.")

        try:
            result = rekey_transcripts(
                podcast_slug=slug, limit=options["limit"], apply=apply)
        except RuntimeError as exc:
            raise CommandError(str(exc))

        from pod_manager.admin_console.summary import emit_summary
        if not apply:
            ids = result["candidates"]
            self.stdout.write(f"{len(ids)} transcript(s) would be rekeyed.")
            for episode_id in ids[:50]:
                self.stdout.write(f"  episode {episode_id}")
            if len(ids) > 50:
                self.stdout.write(f"  ... and {len(ids) - 50} more")
            self.stdout.write(self.style.WARNING(
                "Dry run — nothing moved. Re-run with --apply to perform the churn."))
            emit_summary(self.stdout, {"applied": False, "candidates": len(ids)})
            return

        self.stdout.write(
            f"{result['rekeyed']} transcript(s) rekeyed; "
            f"{result['retry_pending']} moved with delete/purge pending "
            f"(orphan rows retained for r2_cleanup_orphans); "
            f"{result['errors']} error(s) (untokened, will retry on rerun)."
        )
        if result["retry_pending"] or result["errors"]:
            self.stdout.write(self.style.WARNING(
                "Some transcripts did not fully converge — rerun this command "
                "and/or r2_cleanup_orphans --apply --yes."
            ))
        else:
            self.stdout.write(self.style.SUCCESS("All scanned transcripts converged."))
        emit_summary(self.stdout, {
            "applied": True,
            "rekeyed": result["rekeyed"],
            "retry_pending": result["retry_pending"],
            "errors": result["errors"],
        })
