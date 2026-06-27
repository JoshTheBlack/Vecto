"""Mirror subscriber audio to Cloudflare R2.

Mirror a single episode inline, or bulk-backfill episodes whose audio is not yet
on R2.

    # one episode, inline (no Celery), surfaces errors — for IDE testing
    python manage.py mirror_audio_to_r2 --episode 1234
    python manage.py mirror_audio_to_r2 --episode 1234 --force

    # bulk backfill — preview by default; --apply dispatches task_mirror_episode_audio (staggered)
    python manage.py mirror_audio_to_r2 --all                    # preview: list targets, dispatch nothing
    python manage.py mirror_audio_to_r2 --all --apply            # dispatch mirror tasks to Celery
    python manage.py mirror_audio_to_r2 --network=baldmove --origins=gdrive --apply
    python manage.py mirror_audio_to_r2 --podcast=watchmen --stagger=5 --apply
    python manage.py mirror_audio_to_r2 --all --apply --force    # re-mirror even if present
    python manage.py mirror_audio_to_r2 --network=baldmove --apply --sync  # run inline, not Celery
    python manage.py mirror_audio_to_r2 --origins=gdrive --limit=3 --apply --sync  # prod smoke test

The single-episode mode (--episode) is a single-target action and runs inline
immediately — it takes neither --apply nor --sync.

Selection: premium subscriber episodes matching the filters. By default
already-mirrored episodes (r2_url set) are SKIPPED — that's what makes a
quota-throttled GDrive run safely resumable: just re-run it. --force includes
them and re-mirrors. --origins restricts by Episode.audio_origin() class
(gdrive,libsyn,other,...); dead-S3 is always excluded (unfetchable).
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from pod_manager.models import Episode
from pod_manager.services.r2_mirror import MirrorSkipped, mirror_episode_audio

# Origins we never try to fetch: the dead S3 bucket has no source, and public
# Megaphone is never mirrored.
_UNFETCHABLE_ORIGINS = {'s3_dead', 'megaphone', 'none'}


class Command(BaseCommand):
    help = ("Mirror premium subscriber audio to Cloudflare R2 (single episode or bulk backfill). "
            "Bulk runs preview by default; pass --apply to dispatch.")

    def add_arguments(self, parser):
        # Single-episode mode
        parser.add_argument("--episode", type=int, help="Mirror just this episode id (inline, no Celery).")
        parser.add_argument("--local-path", default=None,
                            help="With --episode: upload this local file instead of downloading.")
        # Bulk selection
        parser.add_argument("--all", action="store_true", help="Target every premium episode.")
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--origins", help="Comma list of audio_origin classes to include (e.g. gdrive,libsyn).")
        # Behaviour
        parser.add_argument("--force", action="store_true",
                            help="Re-mirror even if r2_url is already set / source unchanged.")
        parser.add_argument("--only-missing", action="store_true",
                            help="Explicitly skip episodes that already have r2_url (this is the default).")
        parser.add_argument("--stagger", type=float, default=0.0,
                            help="Seconds between dispatches (Celery countdown), to respect source rate limits.")
        parser.add_argument("--limit", type=int, default=None,
                            help="Sample latch: process at most N episodes (applied after filtering). "
                                 "Use for a small prod smoke test before a full backfill.")
        parser.add_argument("--apply", action="store_true",
                            help="Dispatch the mirror work (default is a preview that lists targets only).")
        parser.add_argument("--sync", action="store_true",
                            help="With --apply: mirror inline in this process (no Celery), surfacing per-episode results.")

    def handle(self, *args, **options):
        if options["episode"]:
            return self._single(options)
        return self._bulk(options)

    # ------------------------------------------------------------------
    def _single(self, options):
        episode_id = options["episode"]
        try:
            result = mirror_episode_audio(
                episode_id, local_path=options["local_path"], force=options["force"],
            )
        except MirrorSkipped as exc:
            self.stdout.write(self.style.WARNING(f"SKIPPED ep {episode_id}: {exc}"))
            return
        except Exception as exc:
            raise CommandError(f"mirror failed for ep {episode_id}: {exc}") from exc
        self._print_result(episode_id, result)

    # ------------------------------------------------------------------
    def _bulk(self, options):
        if not (options["all"] or options["network"] or options["podcast"] or options["origins"]):
            raise CommandError(
                "Specify --episode, or a bulk scope: --all / --network=<slug> / "
                "--podcast=<slug> / --origins=<classes>."
            )

        targets = self._select(options)
        force = options["force"]
        stagger = options["stagger"]
        apply = options["apply"]
        sync = options["sync"]
        preview = not apply

        self.stdout.write(
            f"{len(targets)} episode(s) selected "
            f"(force={force}, stagger={stagger}s, mode={'preview' if preview else 'sync' if sync else 'celery'})."
        )

        dispatched = mirrored = skipped = failed = 0
        for i, ep in enumerate(targets):
            if preview:
                self.stdout.write(f"  would mirror ep {ep.id} [{ep.audio_origin()}] {ep.title[:60]}")
                continue
            if sync:
                try:
                    res = mirror_episode_audio(ep.id, force=force)
                    self._print_result(ep.id, res, indent="  ")
                    if res["status"] in ("mirrored", "deduped"):
                        mirrored += 1
                    else:
                        skipped += 1
                except MirrorSkipped as exc:
                    self.stdout.write(self.style.WARNING(f"  SKIPPED ep {ep.id}: {exc}"))
                    skipped += 1
                except Exception as exc:
                    self.stdout.write(self.style.ERROR(f"  FAILED ep {ep.id}: {exc}"))
                    failed += 1
            else:
                from pod_manager.tasks import task_mirror_episode_audio
                task_mirror_episode_audio.apply_async(
                    args=[ep.id], kwargs={"force": force}, countdown=int(i * stagger),
                )
                dispatched += 1

        from pod_manager.admin_console.summary import emit_summary
        if preview:
            self.stdout.write(self.style.SUCCESS(
                f"Preview: {len(targets)} episode(s) would be dispatched. Re-run with --apply to mirror."))
            emit_summary(self.stdout, {"mode": "preview", "selected": len(targets)})
        elif sync:
            self.stdout.write(self.style.SUCCESS(
                f"Done: {mirrored} mirrored/deduped, {skipped} skipped, {failed} failed."))
            emit_summary(self.stdout, {
                "mode": "sync", "mirrored": mirrored, "skipped": skipped, "failed": failed,
            })
        else:
            self.stdout.write(self.style.SUCCESS(f"Dispatched {dispatched} mirror task(s) to Celery."))
            emit_summary(self.stdout, {"mode": "celery", "dispatched": dispatched})

    # ------------------------------------------------------------------
    def _select(self, options):
        qs = Episode.objects.select_related("podcast", "podcast__network").filter(
            audio_url_subscriber__isnull=False,
        ).exclude(audio_url_subscriber="")
        if options["network"]:
            qs = qs.filter(podcast__network__slug=options["network"])
        if options["podcast"]:
            qs = qs.filter(podcast__slug=options["podcast"])
        if not options["force"]:
            # Default + --only-missing: skip already-mirrored episodes (resumable).
            qs = qs.filter(Q(r2_url__isnull=True) | Q(r2_url=""))

        origins = None
        if options["origins"]:
            origins = {o.strip().lower() for o in options["origins"].split(",") if o.strip()}
        limit = options["limit"]

        targets = []
        for ep in qs.iterator():
            if not ep.is_premium:
                continue
            origin = ep.audio_origin()
            if origin in _UNFETCHABLE_ORIGINS:
                continue
            if origins is not None and origin not in origins:
                continue
            targets.append(ep)
            if limit is not None and len(targets) >= limit:
                break
        return targets

    # ------------------------------------------------------------------
    def _print_result(self, episode_id, result, indent=""):
        status = result["status"]
        style = self.style.SUCCESS if status in ("mirrored", "deduped") else self.style.WARNING
        self.stdout.write(style(f"{indent}{status.upper()} ep {episode_id}"))
        if result.get("key"):
            self.stdout.write(f"{indent}  key    : {result['key']}")
        if result.get("r2_url"):
            self.stdout.write(f"{indent}  r2_url : {result['r2_url']}")
        if result.get("reason"):
            self.stdout.write(f"{indent}  reason : {result['reason']}")
