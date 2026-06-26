"""Rename existing local WHISPER_KEEP_SOURCE_AUDIO files to the R2 naming scheme.

The local source-audio cache used to live at the slug-based legacy path
``source_audio/{network_slug}/{podcast_slug}/{original_filename}``. It now mirrors
the R2 object key exactly: ``source_audio/{network_id}/{podcast_id}/{stem}-{shorthash}.{ext}``.
This one-shot backfill relocates pre-existing files onto the new scheme so the
on-disk copy regains parity with R2 (and with the rekey-on-move relocation).

    python manage.py rename_source_audio_to_r2                 # dry-run (default)
    python manage.py rename_source_audio_to_r2 --apply         # actually move files
    python manage.py rename_source_audio_to_r2 --network=baldmove --apply
    python manage.py rename_source_audio_to_r2 --podcast=watchmen --apply

The canonical destination name carries the content hash. For an already-mirrored
episode it's read straight from ``episode.r2_url`` (no work); otherwise the found
file is hashed to derive the same ``{stem}-{shorthash}`` R2 would assign. Moved
episodes whose file still sits under an old parent's slug folder are located by
matching the original filename across the tree (skipped, with a warning, when the
name is ambiguous across shows).
"""

from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Q

from pod_manager.models import Episode
from pod_manager.services.transcription import (
    _hash_file,
    _source_audio_root,
    source_audio_filename,
    source_audio_path,
)


class Command(BaseCommand):
    help = "Relocate legacy local source-audio files onto the R2 (id/hash) naming scheme."

    def add_arguments(self, parser):
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--limit", type=int, default=None, help="Process at most N episodes.")
        parser.add_argument("--apply", action="store_true",
                            help="Actually move files (default is a dry-run that only reports).")

    def handle(self, *args, **options):
        apply = options["apply"]
        root = _source_audio_root()
        if not root.exists():
            self.stdout.write(self.style.WARNING(f"No source-audio directory at {root} — nothing to do."))
            return

        # Index every on-disk file by basename so a moved episode's file can be
        # found even when it still sits under its old parent's slug folder.
        files_by_name: dict[str, list[Path]] = defaultdict(list)
        for path in root.rglob("*"):
            if path.is_file():
                files_by_name[path.name].append(path)

        qs = (Episode.objects.select_related("podcast", "podcast__network")
              .filter(audio_url_subscriber__isnull=False)
              .exclude(audio_url_subscriber=""))
        if options["network"]:
            qs = qs.filter(podcast__network__slug=options["network"])
        if options["podcast"]:
            qs = qs.filter(podcast__slug=options["podcast"])

        moved = already = missing = ambiguous = 0
        processed = 0
        for ep in qs.iterator():
            if options["limit"] is not None and processed >= options["limit"]:
                break
            processed += 1

            target = source_audio_path(ep)  # None until we know the content hash
            if target is not None and target.exists():
                already += 1
                continue

            src = self._locate_legacy_file(ep, root, files_by_name)
            if src is None:
                missing += 1
                continue
            if src is _AMBIGUOUS:
                ambiguous += 1
                self.stdout.write(self.style.WARNING(
                    f"  AMBIGUOUS ep {ep.id}: multiple files named "
                    f"{source_audio_filename(ep)!r}; skipping."))
                continue

            if target is None:  # episode not mirrored — derive the R2 name by hashing.
                target = source_audio_path(ep, content_hash=_hash_file(src))
            if target is None or src == target:
                already += 1
                continue
            if target.exists():
                already += 1
                continue

            self.stdout.write(f"  {'MOVE' if apply else 'would move'} ep {ep.id}: {src} -> {target}")
            if apply:
                target.parent.mkdir(parents=True, exist_ok=True)
                src.replace(target)
            moved += 1

        verb = "Moved" if apply else "Would move"
        self.stdout.write(self.style.SUCCESS(
            f"{verb} {moved}; already-canonical {already}; "
            f"no file found {missing}; ambiguous {ambiguous}."))
        if not apply and moved:
            self.stdout.write("Re-run with --apply to perform the moves.")

    def _locate_legacy_file(self, ep, root: Path, files_by_name):
        """Find ep's existing source-audio file. Prefer the legacy path under the
        episode's CURRENT slugs; fall back to a unique tree-wide basename match
        (for episodes moved before the rekey-on-move relocation existed). Returns
        the Path, None when not found, or _AMBIGUOUS on a multi-show name clash."""
        legacy_name = source_audio_filename(ep)
        current_legacy = root / ep.podcast.network.slug / ep.podcast.slug / legacy_name
        if current_legacy.exists():
            return current_legacy

        candidates = [p for p in files_by_name.get(legacy_name, []) if p.exists()]
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        return _AMBIGUOUS


_AMBIGUOUS = object()
