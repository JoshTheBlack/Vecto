"""Extract chapters from episode descriptions and apply them where missing.

Many feeds list chapter markers as plain text in the episode show notes rather
than as a structured ``<podcast:chapters>`` tag. This command scans the chosen
episodes and, for any episode that has **no** chapters yet but whose description
*contains* a recognizable chapter list, extracts those markers and writes them as
Podcasting-2.0 chapters (``chapters_public`` / ``chapters_private``).

Recognized description formats live in
``pod_manager/services/chapter_extraction.py`` and are easy to extend — add a new
``ChapterStyle`` there to teach the extractor a new layout. Today it recognizes
``Title (00:12:56)`` (time in trailing parens) and ``(00:12:56) Title`` (time
first). When the first marker isn't at ``00:00:00`` an "Intro" chapter is prepended.

Scope it to a network (required) and, optionally, specific podcasts and/or
episodes. Omit --podcast to process every podcast in the network. Preview is the
default — it reports what it *would* write and changes nothing; pass --apply to
save. Episodes that already have chapters are left alone unless you pass
--overwrite.

    # preview every podcast in the network
    python manage.py extract_description_chapters --network=baldmove
    # one podcast, save
    python manage.py extract_description_chapters --network=baldmove --podcast=watching --apply
    # a couple of specific episodes only
    python manage.py extract_description_chapters --network=baldmove --episode 1234 --episode 1240 --apply
    # cap the run at 25 episodes for a quick sample
    python manage.py extract_description_chapters --network=baldmove --limit=25
    # replace existing chapters too (clobbers curated chapters — use with care)
    python manage.py extract_description_chapters --network=baldmove --overwrite --apply
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.admin_console.summary import emit_summary
from pod_manager.models import Episode, Network
from pod_manager.services.chapter_extraction import extract_chapters_from_html


def _has_chapters(value):
    """True if a chapters JSON field holds at least one chapter (handles both the
    canonical dict shape and the legacy bare list)."""
    if isinstance(value, dict):
        return bool(value.get("chapters"))
    return bool(value)


class Command(BaseCommand):
    help = ("Extract chapters from episode descriptions and apply them to episodes "
            "that have none. Preview by default; pass --apply to save.")

    def add_arguments(self, parser):
        parser.add_argument(
            "--network", required=True,
            help="Network slug to process (required).",
        )
        parser.add_argument(
            "--podcast", action="append", dest="podcasts", metavar="SLUG",
            help="Restrict to this podcast slug. Repeatable; omit to process every "
                 "podcast in the network.",
        )
        parser.add_argument(
            "--episode", action="append", dest="episode", type=int, metavar="ID",
            help="Restrict to this episode id. Repeatable; omit to process every episode.",
        )
        parser.add_argument(
            "--limit", type=int, default=None,
            help="Process at most N episodes, then stop (applied after filtering).",
        )
        parser.add_argument(
            "--overwrite", action="store_true",
            help="Replace chapters on episodes that already have them "
                 "(default leaves existing chapters untouched).",
        )
        parser.add_argument(
            "--apply", action="store_true",
            help="Save extracted chapters (default is a preview that writes nothing).",
        )

    def handle(self, *args, **options):
        apply = options["apply"]
        overwrite = options["overwrite"]
        limit = options["limit"]

        try:
            network = Network.objects.get(slug=options["network"])
        except Network.DoesNotExist:
            raise CommandError(f"Network '{options['network']}' not found.")

        episodes = (
            Episode.objects.filter(podcast__network=network).select_related("podcast")
        )

        if options["podcasts"]:
            podcasts = list(network.podcasts.filter(slug__in=options["podcasts"]))
            found_slugs = {p.slug for p in podcasts}
            missing = [s for s in options["podcasts"] if s not in found_slugs]
            if missing:
                raise CommandError(
                    f"Podcast slug(s) not found in network '{network.slug}': "
                    f"{', '.join(missing)}"
                )
            episodes = episodes.filter(podcast__in=podcasts)

        if options["episode"]:
            episodes = episodes.filter(id__in=options["episode"])

        if limit is not None:
            episodes = episodes[:limit]

        examined = updated = skipped_existing = no_chapters = 0

        for ep in episodes:
            examined += 1

            has_public = _has_chapters(ep.chapters_public)
            has_private = _has_chapters(ep.chapters_private)
            if (has_public or has_private) and not overwrite:
                skipped_existing += 1
                continue

            # Prefer the full original; fall back to the cleaned copy (a description
            # cut trigger may have trimmed the chapter list out of clean_description).
            extracted = (
                extract_chapters_from_html(ep.raw_description)
                or extract_chapters_from_html(ep.clean_description)
            )
            if not extracted:
                no_chapters += 1
                continue

            # Fill only the empty side(s) unless overwriting, so we never clobber one
            # feed's curated chapters while filling the other.
            targets = []
            if overwrite or not has_public:
                targets.append("public")
            if overwrite or not has_private:
                targets.append("private")
            if not targets:
                skipped_existing += 1
                continue

            n = len(extracted["chapters"])
            verb = "Applied" if apply else "Would apply"
            self.stdout.write(self.style.SUCCESS(
                f"[{ep.id}] {verb} {n} chapter(s) to {'+'.join(targets)} — "
                f"{ep.podcast.title} | {ep.title[:60]}"
            ))

            if apply:
                if "public" in targets:
                    ep.chapters_public = extracted
                if "private" in targets:
                    ep.chapters_private = extracted
                ep.save(update_fields=["chapters_public", "chapters_private"])
            updated += 1

        self.stdout.write("")
        mode = "Applied" if apply else "Preview"
        self.stdout.write(self.style.SUCCESS(
            f"{mode}: examined {examined}, "
            f"{'updated' if apply else 'would update'} {updated}, "
            f"skipped {skipped_existing} (already have chapters), "
            f"{no_chapters} with no chapters in description."
        ))
        if not apply and updated:
            self.stdout.write("Re-run with --apply to save the extracted chapters.")

        emit_summary(self.stdout, {
            "applied": apply,
            "examined": examined,
            "updated": updated,
            "skipped_existing": skipped_existing,
            "no_chapters_found": no_chapters,
        })
