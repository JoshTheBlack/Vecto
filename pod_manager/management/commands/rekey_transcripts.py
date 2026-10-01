"""Move transcript objects to where the row should keep them (transcript plan, section E2).

Each transcript stores WHERE its R2 objects live (Transcript.r2_key_stem), so a
move is: copy the objects, flip the stored stem, delete + CDN-purge the old ones.
Per transcript, strict order: record-orphan -> copy -> flip -> delete old objects ->
purge their CDN URLs — a crash at any point leaves a durable orphan-row retry record
and a rerun converges. Not destructive: the "delete" removes the old duplicate only
after a byte-identical copy is live at the new location.

Three modes (mutually exclusive):

  (default)        Legacy -> tokened. Every new transcription is born with a random
                   r2_key_token; this moves the old catalog off the deterministic
                   (fuzzable) keys. Run --podcast <slug> right after flipping a
                   feed's allow_public_transcripts off.
  --normalize      Bring stored stems back to canonical: the owner's current id and
                   the network's transcript_key_pattern (Django admin > Network).
                   Needed after a merge repointed a transcript to another episode
                   (its objects stayed at the old id's path), or after changing a
                   network's pattern. Keeps each transcript's token, so URLs stay
                   secret; only the path changes.
  --rotate-token   Give transcripts a NEW random token and move them. The old object
                   URLs are deleted and CDN-purged — use it if a URL leaked. Needs a
                   scope (--podcast / --network) or --all.

    python manage.py rekey_transcripts                                  # dry run: legacy candidates
    python manage.py rekey_transcripts --apply                          # churn everything legacy
    python manage.py rekey_transcripts --podcast <slug> --apply         # one feed (flag flip)
    python manage.py rekey_transcripts --normalize                      # dry run: old -> new stems
    python manage.py rekey_transcripts --normalize --network <slug> --apply
    python manage.py rekey_transcripts --rotate-token --podcast <slug> --apply
    python manage.py rekey_transcripts --limit 200 --apply              # batched (any mode)
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import rekey_transcripts

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = ("Move transcript R2 objects (legacy -> tokened, --normalize to the canonical "
            "stem, --rotate-token for new URLs), flipping each row's stored stem and "
            "deleting + CDN-purging the old objects once the copy is live. Idempotent; "
            "dry run by default.")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Perform the move (default is a dry run that "
                                 "only lists candidates).")
        parser.add_argument("--normalize", action="store_true",
                            help="Move transcripts whose stored stem differs from the "
                                 "canonical one (owner id / network pattern). Keeps tokens.")
        parser.add_argument("--rotate-token", action="store_true",
                            help="Give transcripts a NEW token and move them (revokes the "
                                 "old URLs). Needs --podcast, --network or --all.")
        parser.add_argument("--podcast", help="Podcast slug to scope to.")
        parser.add_argument("--network", help="Network slug to scope to.")
        parser.add_argument("--all", action="store_true",
                            help="Acknowledge an unscoped --rotate-token over every transcript.")
        parser.add_argument("--limit", type=int,
                            help="Stop after N transcripts moved.")

    def handle(self, *args, **options):
        apply = options["apply"]
        slug = options["podcast"]
        network = options["network"]
        normalize = options["normalize"]
        rotate = options["rotate_token"]

        if normalize and rotate:
            raise CommandError("--normalize and --rotate-token are mutually exclusive.")
        if rotate and not (slug or network or options["all"]):
            raise CommandError("--rotate-token revokes transcript URLs; scope it with "
                               "--podcast / --network, or pass --all to rotate everything.")
        if slug:
            from pod_manager.models import Podcast
            if not Podcast.objects.filter(slug=slug).exists():
                raise CommandError(f"No podcast with slug '{slug}'.")
        if network:
            from pod_manager.models import Network
            if not Network.objects.filter(slug=network).exists():
                raise CommandError(f"No network with slug '{network}'.")

        try:
            result = rekey_transcripts(
                podcast_slug=slug, network_slug=network, limit=options["limit"],
                apply=apply, normalize=normalize, rotate_token=rotate)
        except RuntimeError as exc:
            raise CommandError(str(exc))

        verb = "rotated" if rotate else "normalized" if normalize else "rekeyed"
        from pod_manager.admin_console.summary import emit_summary
        if not apply:
            ids = result["candidates"]
            self.stdout.write(f"{len(ids)} transcript(s) would be {verb}.")
            if result["changes"]:
                for episode_id, old, new in result["changes"][:50]:
                    self.stdout.write(f"  episode {episode_id}: {old} -> {new}")
                if len(result["changes"]) > 50:
                    self.stdout.write(f"  ... and {len(result['changes']) - 50} more")
            else:
                for episode_id in ids[:50]:
                    self.stdout.write(f"  episode {episode_id}")
                if len(ids) > 50:
                    self.stdout.write(f"  ... and {len(ids) - 50} more")
            self.stdout.write(self.style.WARNING(
                "Dry run — nothing moved. Re-run with --apply to perform the move."))
            emit_summary(self.stdout, {"applied": False, "mode": result["mode"], "candidates": len(ids)})
            return

        self.stdout.write(
            f"{result['rekeyed']} transcript(s) {verb}; "
            f"{result['retry_pending']} moved with delete/purge pending "
            f"(orphan rows retained for r2_cleanup_orphans); "
            f"{result['errors']} error(s) (not moved, will retry on rerun)."
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
            "mode": result["mode"],
            "rekeyed": result["rekeyed"],
            "retry_pending": result["retry_pending"],
            "errors": result["errors"],
        })
