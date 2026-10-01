"""Re-attach transcripts that are in the R2 bucket but not (correctly) in the database.

After a DB loss/rebuild or a re-import, episode ids change — so Transcript rows can be
missing, or point at objects that no longer exist, while every transcript object is
still in the vecto-cdn bucket. Each ``.words`` object embeds a recovery header
(episode_id, title, guid_public, guid_private, audio_url, language, model,
transcribed_at). This command reads those headers, matches each to an episode, and
records WHERE the objects already are (Transcript.r2_key_stem). Nothing in the bucket
is moved, renamed or deleted.

Matching, strongest evidence first (the first level with any candidate decides; more
than one candidate there is AMBIGUOUS and is never auto-linked):
    exact   guid_private / guid_public equals the episode's
    high    audio_url equals the episode's subscriber audio, or the header's
            episode_id is a real episode whose title agrees
    medium  the normalized title is unique among the episodes in scope
The default threshold is --min-confidence high.

An episode that already has a live transcript (its objects are in the bucket) is a
conflict and is left alone; a placeholder row or one whose objects are gone is filled
or repointed. Stems on the orphan ledger (about to be deleted by cleanup) are skipped.

    python manage.py recover_transcripts                              # dry run: what would be linked
    python manage.py recover_transcripts --apply                      # link the exact/high matches
    python manage.py recover_transcripts --network <slug> --apply     # only match this network's episodes
    python manage.py recover_transcripts --min-confidence medium --apply   # also unique-title matches
    python manage.py recover_transcripts --apply --purge-cdn          # also purge past ?v=N URLs at the edge
    python manage.py recover_transcripts --show 100                   # list more of each category

Afterwards: `backfill_transcripts_to_r2 --all --verify` confirms every row's objects
exist; `rekey_transcripts --normalize` (optional) moves recovered transcripts to the
canonical stem. Requires R2_MEDIA_ENABLED=True.
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.transcript_recovery import CONFIDENCE_RANK, recover_transcripts


class Command(BaseCommand):
    help = ("Re-attach transcripts found in the R2 bucket to their episodes using the "
            "recovery header in each .words file. Dry run by default; never moves or "
            "deletes bucket objects.")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Create/fill/repoint the Transcript rows (default is a dry run).")
        parser.add_argument("--network", help="Only match this network's episodes (network slug).")
        parser.add_argument("--podcast", help="Only match this podcast's episodes (podcast slug).")
        parser.add_argument("--min-confidence", choices=sorted(CONFIDENCE_RANK, key=CONFIDENCE_RANK.get),
                            default="high",
                            help="Weakest evidence to auto-link: exact (GUID), high (audio URL or id+title), "
                                 "medium (unique title). Default: high.")
        parser.add_argument("--limit", type=int, help="Stop after N matches.")
        parser.add_argument("--purge-cdn", action="store_true",
                            help="With --apply: also purge the recovered objects' ?v=N URLs from the "
                                 "Cloudflare edge cache.")
        parser.add_argument("--show", type=int, default=20,
                            help="How many entries to list per category (default 20).")

    def handle(self, *args, **options):
        apply = options["apply"]
        for flag, model_name, label in ((options["network"], "Network", "network"),
                                        (options["podcast"], "Podcast", "podcast")):
            if flag:
                from pod_manager import models
                if not getattr(models, model_name).objects.filter(slug=flag).exists():
                    raise CommandError(f"No {label} with slug '{flag}'.")
        if options["purge_cdn"] and not apply:
            raise CommandError("--purge-cdn only applies together with --apply.")

        try:
            report = recover_transcripts(
                network_slug=options["network"], podcast_slug=options["podcast"],
                min_confidence=options["min_confidence"], limit=options["limit"],
                apply=apply, purge_cdn=options["purge_cdn"])
        except RuntimeError as exc:
            raise CommandError(str(exc))

        show = options["show"]
        w = self.stdout.write

        def listing(title, rows, fmt, style=None):
            if not rows:
                return
            w((style or (lambda s: s))(f"\n{title} ({len(rows)}):"))
            for row in rows[:show]:
                w("  " + fmt(row))
            if len(rows) > show:
                w(f"  ... and {len(rows) - show} more (--show N to list more)")

        w(f"{report['scanned']} transcript(s) in the bucket; {report['already_linked']} already linked "
          f"to a Transcript row.")
        verb = "linked" if apply else "would link"
        listing(f"{verb.capitalize()}", report["matches"],
                lambda r: f"episode {r['episode_id']} <- {r['stem']}  [{r['confidence'].upper()}: {r['reason']}; "
                          f"{r['action']}]  {r['title'] or ''}", self.style.SUCCESS)
        listing("Ambiguous — more than one candidate; resolve by hand", report["ambiguous"],
                lambda r: f"{r['stem']}  episodes {r['episode_ids']}  {r.get('title') or ''}"
                          f"{'  (' + r['note'] + ')' if r.get('note') else ''}", self.style.WARNING)
        listing("Below --min-confidence (not linked)", report["skipped_low_confidence"],
                lambda r: f"episode {r['episode_id']} <- {r['stem']}  [{r['confidence'].upper()}: {r['reason']}]  "
                          f"{r['title'] or ''}", self.style.WARNING)
        listing("Conflicts — the episode already has a live transcript; left alone", report["conflicts"],
                lambda r: f"episode {r['episode_id']} has {r['live_stem']}; bucket also has {r['stem']}",
                self.style.WARNING)
        listing("Unmatched — no episode in scope fits", report["unmatched"],
                lambda r: f"{r['stem']}  {r['title'] or ''}  gp={r['guid_public']} gx={r['guid_private']}")
        listing("Unreadable .words header", [{"stem": s} for s in report["unreadable"]],
                lambda r: r["stem"], self.style.ERROR)
        listing("No .words object (cannot be matched)", [{"stem": s} for s in report["no_words"]],
                lambda r: r["stem"])
        listing("Skipped — on the orphan ledger (cleanup will delete)", [{"stem": s} for s in report["ledger_skipped"]],
                lambda r: r["stem"])

        w("")
        if apply:
            errors = f", {report['errors']} error(s)" if report["errors"] else ""
            w(self.style.SUCCESS(f"Recovered {report['recovered']} transcript(s){errors}."))
            if report["purged"] is not None:
                w("CDN purge: " + ("ok" if report["purged"] else "FAILED (see log; re-run with --purge-cdn or purge manually)"))
        else:
            w(self.style.WARNING("Dry run — nothing changed. Re-run with --apply to link the matches above."))

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "applied": apply, "scanned": report["scanned"], "already_linked": report["already_linked"],
            "matched": len(report["matches"]), "recovered": report["recovered"],
            "ambiguous": len(report["ambiguous"]), "unmatched": len(report["unmatched"]),
            "conflicts": len(report["conflicts"]), "below_threshold": len(report["skipped_low_confidence"]),
            "errors": report["errors"],
        })
