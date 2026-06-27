"""Smoke test: put / get / delete a dummy object under the dev/ prefix.

Confirms the R2 credentials, endpoint, checksum config and key prefixing all
work end to end. Writes only under R2_KEY_PREFIX (dev/ in the IDE), so it never
touches prod audio.

    python manage.py r2_smoke_test
    python manage.py r2_smoke_test --keep   # leave the object for manual checks
"""

import uuid

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_client import get_r2_client, prefixed_key, public_url


class Command(BaseCommand):
    help = "Round-trip a dummy object through R2 (put/get/delete) under the dev/ prefix."

    def add_arguments(self, parser):
        parser.add_argument(
            "--keep",
            action="store_true",
            help="Don't delete the test object (verify it via the public host).",
        )

    def handle(self, *args, **options):
        try:
            client = get_r2_client()
        except RuntimeError as exc:
            raise CommandError(str(exc)) from exc

        bucket = settings.R2_BUCKET
        key = prefixed_key(f"_smoke_test/{uuid.uuid4().hex}.txt")
        body = b"vecto r2 smoke test\n"

        self.stdout.write(f"endpoint : {settings.R2_ENDPOINT}")
        self.stdout.write(f"bucket   : {bucket}")
        self.stdout.write(f"key      : {key}")
        self.stdout.write(f"prefix   : {settings.R2_KEY_PREFIX!r}")

        from pod_manager.admin_console.summary import emit_summary

        # PUT
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="text/plain",
            CacheControl="no-store",
        )
        self.stdout.write(self.style.SUCCESS("PUT  ok"))

        # GET + verify bytes round-trip intact
        got = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if got != body:
            raise CommandError(f"GET mismatch: wrote {body!r}, read {got!r}")
        self.stdout.write(self.style.SUCCESS("GET  ok (bytes match)"))

        if options["keep"]:
            self.stdout.write(
                f"--keep set; object left at {public_url(key)} "
                "(public host serves it only if the bucket is mapped to that domain)."
            )
            emit_summary(self.stdout, {"ok": True, "kept": True})
            return

        # DELETE
        client.delete_object(Bucket=bucket, Key=key)
        self.stdout.write(self.style.SUCCESS("DEL  ok"))
        self.stdout.write(self.style.SUCCESS("R2 smoke test passed."))
        emit_summary(self.stdout, {"ok": True, "kept": False})
