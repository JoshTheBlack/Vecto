"""Audit mirrored R2 audio objects to confirm they are really audio.

Reads the first bytes of every Episode.r2_url object straight from R2 (a ranged
GET, not the whole file) and confirms an audio magic-byte signature. Flags any
object that is missing, suspiciously small, or whose header looks like an HTML
error page (the failure the download guard now prevents, but that older mirrors
may have stored: a 200-but-HTML GDrive/Patreon page saved as a bogus .mp3).

Read-only by default. --fix --apply re-downloads and re-mirrors the flagged
episodes (the new audio gets a fresh content-hash key; the bogus object is
recorded as an orphan for the GC). An episode whose source is still bad (still
404/HTML) is reported as unfixable and left untouched.

    # report only (default)
    python manage.py audit_r2_audio --all
    python manage.py audit_r2_audio --network=baldmove
    python manage.py audit_r2_audio --podcast=watchmen --limit=50

    # tune the "too small to be audio" floor (bytes; default 102400 = 100 KB)
    python manage.py audit_r2_audio --all --min-bytes=51200

    # attempt to re-mirror everything flagged (irreversible-ish: bumps r2_url)
    python manage.py audit_r2_audio --all --fix --apply
"""

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from pod_manager.models import Episode
from pod_manager.services.r2_client import (get_r2_client, key_from_public_url)
from pod_manager.services.r2_mirror import (MirrorSkipped, looks_like_audio,
                                            mirror_episode_audio)

# HTML/XML document signatures — an object starting with one of these is an error
# page stored as audio, the high-signal bad case.
_HTML_SNIFFS = (b'<!doctype', b'<html', b'<head', b'<?xml')
_SNIFF_BYTES = 512


class Command(BaseCommand):
    help = ("Audit R2 audio objects: confirm each Episode.r2_url is really audio, "
            "flag bad/missing/tiny ones, optionally re-mirror with --fix --apply.")

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Every episode that has an r2_url.")
        parser.add_argument("--network", help="Restrict to a network slug.")
        parser.add_argument("--podcast", help="Restrict to a podcast slug.")
        parser.add_argument("--limit", type=int, default=None, help="Inspect at most N (sample latch).")
        parser.add_argument("--min-bytes", type=int, default=100 * 1024,
                            help="Flag objects smaller than this many bytes (default 102400 = 100 KB).")
        parser.add_argument("--fix", action="store_true",
                            help="Re-download + re-mirror each flagged episode (needs --apply to act).")
        parser.add_argument("--apply", action="store_true",
                            help="With --fix: actually re-mirror (default previews the fix list).")

    def handle(self, *args, **options):
        if not (options["all"] or options["network"] or options["podcast"]):
            raise CommandError("Specify a scope: --all / --network=<slug> / --podcast=<slug>.")

        targets = self._select(options)
        min_bytes = options["min_bytes"]
        self.stdout.write(f"Auditing {len(targets)} R2 audio object(s) (min-bytes={min_bytes})...")

        client = get_r2_client()
        bucket = settings.R2_BUCKET

        ok = 0
        flagged = []  # (episode, reason)
        for ep in targets:
            reason = self._inspect(client, bucket, ep, min_bytes)
            if reason is None:
                ok += 1
            else:
                flagged.append((ep, reason))
                self.stdout.write(self.style.ERROR(
                    f"  FLAG ep {ep.id} [{ep.audio_origin()}] {reason}  {ep.title[:45]}"))

        self.stdout.write(self.style.SUCCESS(
            f"\nAudited {len(targets)}: {ok} look like audio, {len(flagged)} flagged."))
        if flagged:
            self.stdout.write("Flagged episode ids: " + ",".join(str(ep.id) for ep, _ in flagged))

        if options["fix"] and flagged:
            self._fix(flagged, apply=options["apply"])

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "mode": "audit", "audited": len(targets), "ok": ok, "flagged": len(flagged),
        })

    # ------------------------------------------------------------------
    def _inspect(self, client, bucket, ep, min_bytes):
        """Return None if the object looks like audio, else a short reason string."""
        from botocore.exceptions import ClientError

        key = key_from_public_url(ep.r2_url)
        try:
            obj = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{_SNIFF_BYTES - 1}")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return "MISSING in R2"
            return f"R2 error: {code or exc}"

        head = obj["Body"].read()
        total = self._total_size(obj)

        if total is not None and total < min_bytes:
            return f"too small ({total} B)"
        sniff = head[:_SNIFF_BYTES].lstrip().lower()
        if sniff.startswith(_HTML_SNIFFS):
            return "HTML body, not audio"
        if not looks_like_audio(head):
            return "no audio signature"
        return None

    @staticmethod
    def _total_size(obj):
        """Full object size from a ranged GET ('bytes 0-511/12345678'), with a
        ContentLength fallback."""
        cr = obj.get("ContentRange", "")
        if "/" in cr:
            tail = cr.rsplit("/", 1)[-1]
            if tail.isdigit():
                return int(tail)
        return obj.get("ContentLength")

    # ------------------------------------------------------------------
    def _fix(self, flagged, apply):
        if not apply:
            self.stdout.write(self.style.WARNING(
                f"\n--fix preview: would re-mirror {len(flagged)} episode(s). Re-run with --apply to act."))
            return

        self.stdout.write(f"\nRe-mirroring {len(flagged)} flagged episode(s)...")
        fixed = unfixable = errored = 0
        for ep, _ in flagged:
            try:
                res = mirror_episode_audio(ep.id, force=True)
                fixed += 1
                self.stdout.write(self.style.SUCCESS(
                    f"  FIXED ep {ep.id}: {res['status']} -> {res.get('r2_url', '')}"))
            except MirrorSkipped as exc:
                unfixable += 1
                self.stdout.write(self.style.WARNING(
                    f"  UNFIXABLE ep {ep.id}: source still bad ({exc})"))
            except Exception as exc:
                errored += 1
                self.stdout.write(self.style.ERROR(f"  ERROR ep {ep.id}: {exc}"))
        self.stdout.write(self.style.SUCCESS(
            f"Fix: {fixed} re-mirrored, {unfixable} unfixable (source still bad), {errored} errored."))

    # ------------------------------------------------------------------
    def _select(self, options):
        qs = (Episode.objects
              .select_related("podcast", "podcast__network")
              .exclude(Q(r2_url__isnull=True) | Q(r2_url="")))
        if options["network"]:
            qs = qs.filter(podcast__network__slug=options["network"])
        if options["podcast"]:
            qs = qs.filter(podcast__slug=options["podcast"])
        qs = qs.order_by("id")

        limit = options["limit"]
        targets = []
        for ep in qs.iterator():
            targets.append(ep)
            if limit is not None and len(targets) >= limit:
                break
        return targets
