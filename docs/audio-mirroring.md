# Audio Mirroring to Cloudflare R2

Vecto mirrors subscriber audio into [Cloudflare R2](https://developers.cloudflare.com/r2/) — S3-compatible object storage with **zero egress fees** — served through a cached custom domain (`audio.joshtheblack.com`). The mirror gives the custom player CORS-clean, seekable bytes (so playback speed and transcript word-sync "just work"), unblocks browser playback for Google-Drive-recovered episodes, and provides a full off-platform backup of audio Vecto otherwise doesn't control.

Mirroring is built **on top of the transcription pipeline**: it reuses the single audio download transcription already performs, so most episodes are mirrored with no extra download. When transcription is disabled, a standalone path mirrors independently.

> **Subscriber audio only.** Public audio (`audio_url_public`, typically Megaphone) has dynamic per-listener ad insertion and is **never** mirrored or rewritten. This matches the transcription rule and is enforced structurally (see [Subscriber-Only Guarantee](#subscriber-only-guarantee)).

---

## How It Works

1. A premium episode is saved (or transcribed). The subscriber MP3 is downloaded **once**.
2. That same file is streamed to R2 under a content-addressed key, with long-lived immutable cache headers.
3. The episode's `r2_url` is set. The original subscriber URL is **never** overwritten — `r2_url` is a *new* field and the serving layer chooses between them at request time.
4. All feed enclosures already point at `/play/<id>`, which 302-redirects to `Episode.playback_url()`. The R2-vs-origin decision lives entirely there — no feed changes, reversible, and the future "always serve from R2" switch is a one-line policy change.

The key never changes in place: a corrected re-upload produces different bytes → a different content hash → a different key/URL. The old object keeps its immutable cache entry harmlessly (nobody holds the old URL), and the new URL is fetched fresh. This is why every object is marked `immutable` forever.

---

## Why R2

| Host | Status |
|---|---|
| **Libsyn** | Works, has its own CDN. Monthly upload caps mean the recovery backlog can't be pushed here. **Stays on Libsyn**; R2 is backup-only. |
| **Google Drive** | Where GDrive-recovery episodes point. **Not browser-playable** (no CORS, Firefox ORB blocks it, large-file interstitials). Mirrored → served from R2. |
| **Dead S3 bucket** | Gone. Episodes still pointing here are withheld from feeds. No source to mirror. |
| **Patreon** | Subscriber audio with ephemeral/signed enclosure URLs. Mirrored → served from R2 so the ephemeral URL stops mattering. |
| **Megaphone** | **Public** audio with dynamic ads. Out of scope — never touched. |

**Cost** (validated against real samples, ~43 MB/episode avg): ~173 GB for 4,000 episodes ≈ **$2.45/mo** storage (R2 = $0.015/GB-mo). Egress to listeners: **$0**, any volume. All-in ~$3–5/mo, dominated by storage — versus ~$3,600/mo egress on S3 for the same traffic.

---

## Configuration

All settings live in `.env` and are read via `config/settings.py` (same pattern as `WHISPER_*`).

| Variable | Default | Description |
|---|---|---|
| `R2_ENDPOINT` | — | Account endpoint: `https://<account_id>.r2.cloudflarestorage.com`. |
| `R2_ACCESS_KEY_ID` | — | R2 API token (Object Read & Write). |
| `R2_SECRET_ACCESS_KEY` | — | Secret for the above. |
| `R2_BUCKET` | `vecto-audio` | Single bucket for all environments; dev is isolated by key prefix, not a second bucket. |
| `R2_PUBLIC_HOST` | — | Public custom domain mapped to the bucket, no trailing slash (e.g. `https://audio.joshtheblack.com`). |
| `R2_KEY_PREFIX` | `""` prod / `"dev/"` IDE | Prepended to every key so dev objects are namespaced and bulk-purgeable. **Defaults to `dev/` under `IS_IDE`** even if `.env` omits it, so a forgotten entry can't write into the prod keyspace. |
| `R2_MIRROR_ENABLED` | `True` | Master kill-switch (mirrors `WHISPER_ENABLED`). Forced `False` under tests. |
| `R2_FORCE_SERVE` | `False` | The future **global** "always serve private audio from R2" switch (cutover). |
| `R2_ORPHAN_RETENTION_DAYS` | `90` | Hold window for reversion/reconciliation orphans (old audio may need recovery). |
| `R2_REKEY_GRACE_DAYS` | `7` | Short hold for move-rekey orphans (a byte-identical copy already exists at the new key). |

`region_name` is always `"auto"` for R2 — set automatically in the client; do not configure it.

> **boto3 checksum gotcha.** boto3 ≥ 1.36 sends default integrity checksums that some S3-compatible providers reject with opaque 400s. The R2 client (`services/r2_client.py`) sets request/response checksum calculation to `when_required`, which keeps put/get/delete and multipart uploads clean. No boto3 version pin is needed.

---

## Object Naming (R2 keys)

```
{R2_KEY_PREFIX}{network_id}/{podcast_id}/{stem}-{shorthash}.{ext}
```

- **`network_id` / `podcast_id`** are immutable primary keys, **not slugs**. A slug rename is therefore *free*: the key never references the slug, so nothing is renamed and re-mirroring lands on the same key. (Object storage has no real rename — it's CopyObject + Delete per object — so slug-based folders would orphan objects on every rename.)
- **`stem`** is the original audio filename with all trailing audio extensions stripped (many sources end in `.mp3.mp3`), then slugified. Derived via transcription's `source_audio_filename()`, so GDrive `/uc?...` links resolve to `episode_{pk}` instead of a garbage `uc` stem. The readable name lives in the **filename**, not the folders.
- **`shorthash`** is the first 16 hex chars (64 bits) of the **SHA-256 of the audio bytes**, computed during the streamed download. This is the cache-bust token (changed bytes → new key), gives natural dedupe, and distinguishes real content change from URL churn. 64 bits ≈ zero collision probability at our scale. R2's multipart ETag is *not* a usable content hash, which is why we compute our own.
- **`ext`** is the outermost real audio extension (`.m4a`/`.m4b` keep `Content-Type: audio/mp4`), defaulting to `mp3`.

Folders are illusory in object storage (just key prefixes); nothing is pre-created. The full public URL is persisted to `Episode.r2_url`, which is the **only** canonical pointer — keys are never reconstructed from current slugs/IDs at serve time.

### Upload headers (set per-object; R2 has no bucket-level default)

- `Content-Type` — derived from the extension (default `audio/mpeg`).
- `Cache-Control: public, max-age=31536000, immutable` — the browser half of the caching story. The Cloudflare Cache Rule on the custom domain is the edge half.

---

## Serving Decision

`Episode.playback_url(has_access)` is the single source of truth for both playback and feeds. Precedence:

| # | Condition | Serves |
|---|---|---|
| 1 | No access | `audio_url_public` (Megaphone, untouched — R2 never applies to public audio) |
| 2a | Access + `r2_url` + `settings.R2_FORCE_SERVE` | `r2_url` (global switch) |
| 2b | Access + `r2_url` + `podcast.force_r2_serve` | `r2_url` (per-feed override) |
| 2c | Access + `r2_url` + origin is **GDrive** | `r2_url` (GDrive can't serve a browser) |
| 2d | Access + origin is **Libsyn** | `audio_url_subscriber` (durable CDN; R2 is backup-only) |
| 2e | Access + `r2_url` (Patreon, recovered dead-S3, other hosts) | `r2_url` (ephemeral/unreliable origin → prefer the mirror) |
| 2f | Access, otherwise | `audio_url_subscriber` (legacy — no mirror yet) |

`Episode.audio_origin()` classifies the subscriber host into `{megaphone, libsyn, gdrive, s3_dead, other, none}` by pure host detection. **Only Libsyn stays origin-served by default**; every other host prefers the mirror once one exists.

### Feed withholding — handled for free

`serves_s3_audio()` is derived entirely from `playback_url()`. Once an episode has an `r2_url`, `playback_url()` no longer returns the dead-S3 host, so the episode is automatically **un-withheld** from feeds at every call site — no feed code changes. This is the single-chokepoint design paying off.

### On-site player

The website player and download links also route through `playback_url()`, not the raw `audio_url_subscriber`. The episode-detail and home views set `ep.raw_audio_url = ep.playback_url(ep.user_has_access)`, so a mirrored GDrive episode streams **inline from R2** on the site (the "this file is on Google Drive and can't be streamed inline" warning only shows when there's no `r2_url`). The per-page reachability probe (`/api/check-audio/`) is skipped for R2-served audio — R2 is reliable, and skipping it avoids a `HeadObject` (Class B) on every page view.

### Per-feed override (Creator Settings)

`Podcast.force_r2_serve` (precedence 2b) is surfaced as a toggle in **Creator Settings → Podcasts → [Show] → Audio Delivery**. Turn it on to force a whole feed onto R2 (e.g. an ephemeral-URL Patreon feed) without admin/code access. Off = serve from origin, R2 backup-only.

---

## The Mirror Pipeline

The core service is `services/r2_mirror.py`:

```python
mirror_episode_audio(episode_id, local_path=None, force=False)
```

- **`local_path`** — if given (transcription's temp MP3), the file is uploaded directly with **no re-download**. The mirror only *reads* it; it never moves or deletes it, so transcription's retention behavior is unchanged. Otherwise the subscriber MP3 is downloaded first.
- Computes the content hash, builds the key, uploads with headers, sets `r2_url` / `r2_uploaded_at` / `r2_source_signature`.
- **Idempotent** — skips if `r2_url` is set and the source signature is unchanged (unless `force`).
- Raises `MirrorSkipped` (a normal no-op, not an error) for non-applicable episodes.

### Two triggers, coordinated to download once

1. **Inline (transcription enabled).** `run_transcription()` calls the mirror right after the audio download and **before** the Whisper POST — so a backup exists even if ASR fails, times out, or Whisper is unreachable. Best-effort: a mirror failure never fails the transcription. When a re-transcription explicitly selects the **subscriber** source (the episode-detail [Audio source](transcription.md#audio-source-override) picker), this inline call runs with `force=True`, so the content hash is recompared even on mirror-once hosts (GDrive) — identical bytes still dedupe, changed bytes re-version. Re-transcriptions from the **R2** or **Public** source skip the mirror entirely; public audio is never mirrored.
2. **Standalone (transcription disabled).** The `queue_r2_mirror_on_episode_save` signal dispatches `task_mirror_episode_audio` — but **only** when `R2_MIRROR_ENABLED and created and is_premium and NOT WHISPER_ENABLED`. The `WHISPER_ENABLED` gate prevents a second download when transcription would already carry the mirror.

> Scheduled episodes are mirrored **on creation** (the signal ignores `is_published`), so the backup and transcript can be ready before the episode is published. Feeds only expose `is_published=True` episodes, so nothing leaks early.

### Subscriber-Only Guarantee

`mirror_episode_audio` refuses (raises `MirrorSkipped`) when:

- there is no `audio_url_subscriber`,
- the subscriber URL equals the public URL (`is_premium` is False) — the structural guard against ever mirroring Megaphone/public audio,
- the origin is `s3_dead` (the dead bucket has no fetchable source).

Subscriber URLs are publisher-controlled, so every fetch is SSRF-validated with `utils.validate_public_url` (the same guard the chapter fetch uses) before download or HEAD.

### Change Detection

Ingest runs frequently, so unchanged audio must not be re-mirrored. Before re-mirroring an episode that already has an `r2_url`, the mirror issues an HTTP `HEAD` and compares a cheap signature `"{etag}:{content_length}"` to the stored `r2_source_signature`. Only a proven change triggers a re-download + re-upload. Hosts without usable HEAD/ETag (Google Drive) fall back to **mirror-once** — they only re-mirror via `--force`. (Libsyn does support HEAD/ETag, so change-detection works there.)

### Dedupe & Re-adoption

After computing the key, if the object already exists in R2, the upload is skipped, `r2_url` is set, and any orphan row for that key is cleared (re-adoption — the key is live again).

---

## Orphan Lifecycle (Garbage Collection)

An **orphan** is an R2 object no `Episode.r2_url` points at. They arise from (1) re-versioning (new hash → new key, old key abandoned) and (2) partial failures (upload succeeded but the DB commit was interrupted).

> **`Episode.r2_url` is the single source of truth.** The `R2OrphanedObject` table is only a deletion-*candidate* list; recording and cleanup both re-validate against live `r2_url` first. Content-hash keying intentionally dedupes, so a key may be referenced by more than one episode — a shared key is never orphaned while another episode still points at it.

### Why objects stay in place

Feed enclosures are the stable `/play/<id>` URL. A streaming client resolves `/play` → the *current* `r2_url` and pulls byte ranges; the edge only has the ranges already played. If an object stops resolving **at its original key** mid-session, the next range 404s and playback stalls. So an orphan must keep resolving at its original key until in-flight sessions end. **Objects are left in place for a retention window**, never moved or deleted immediately.

### The three layers

| Layer | When | What |
|---|---|---|
| **Record-on-reversion** | inside `mirror_episode_audio`, deterministic | Upload new → verify → commit `r2_url` → if the old key isn't referenced elsewhere, insert `R2OrphanedObject(reason='reversion')`. Old object left in place. |
| **Reconciliation sweep** | `task_r2_reconcile` — **weekly** (Mon 03:30) | Paginated `ListObjectsV2` over the entire prod keyspace; records unreferenced objects older than 7 days (not already orphaned, not under `dev/`) as `reason='reconciliation'`. Records only — never deletes. |
| **Cleanup** | `task_r2_orphan_cleanup` — **daily** (04:00) | The **only hard-delete**. For rows past their per-reason retention, re-validates against live `r2_url`: referenced → drop the row (keep object); unreferenced → `DeleteObjects` (batched 1000) + drop row. |

Per-reason retention: `move_rekey` → `R2_REKEY_GRACE_DAYS` (7); everything else → `R2_ORPHAN_RETENTION_DAYS` (90). The last-moment re-check is what makes deletion safe against state that changed during the window.

> **The cleanup runs unattended with delete enabled.** This is intentional and safe behind the 90-day window + last-moment revalidation. The reconciliation sweep never deletes. Both run **only in prod** — in the IDE there's no Beat, so invoke the commands by hand.

### Recovery

Within the window the object is untouched at its original key — restore by re-pointing `episode.r2_url` and deleting the orphan row. The bytes never moved.

---

## Moving Episodes Between Feeds

**Cross-publishing** (an episode appearing in additional feeds) has **zero R2 impact** — the episode stays one entity with one `r2_url`; only feed membership changes.

**Moving** (the parent podcast is reassigned) **re-keys on move** so the bucket layout always reflects current parentage (for disaster-recovery accuracy — playback never depends on the prefix). `handle_move_episodes` does a bulk `.update()`, then dispatches `task_rekey_episode_audio` for each moved episode that has an `r2_url` (gated on `R2_MIRROR_ENABLED`). `rekey_episode_audio`:

1. Computes the new key from the **current** parent — preserving the byte-identical `{stem}-{hash}.{ext}` filename, swapping only the `network_id/podcast_id` folder.
2. No-op if it already matches.
3. Server-side `CopyObject` (no egress) → verify → commit `r2_url` → clear any new-key orphan → record the old key as `reason='move_rekey'` (short 7-day grace, unless still referenced).
4. **Relocates the local `WHISPER_KEEP_SOURCE_AUDIO` copy too** (best-effort), `shutil.move`-ing it from the old-key path to the new-key path under `MEDIA_ROOT/source_audio/` so the on-disk mirror keeps parity with R2. No-op in prod (retention is dev-only) and whenever the file isn't present; a local FS hiccup never fails the R2 rekey (`r2_url` is the source of truth).

The mirror's content idempotency **never** re-keys on a prefix mismatch — only the explicit move action does, so a routine re-ingest after a move doesn't churn.

> **Local source-audio parity.** The retained dev copy mirrors the R2 object key exactly — `source_audio/{network_id}/{podcast_id}/{stem}-{shorthash}.{ext}` (bucket prefix stripped) — so it relocates on move via the same key delta. Files written before this naming existed are migrated with `manage.py rename_source_audio_to_r2` (dry-run by default; `--apply` to move).

---

## Management Commands

### Mirror / Backfill — `mirror_audio_to_r2`

```bash
# Single episode (inline, surfaces errors — for testing)
python manage.py mirror_audio_to_r2 --episode <id> [--force]

# Bulk backfill — preview by default; --apply dispatches task_mirror_episode_audio
python manage.py mirror_audio_to_r2 --all                  # preview
python manage.py mirror_audio_to_r2 --all --apply
python manage.py mirror_audio_to_r2 --network=<slug> --origins=gdrive --apply
python manage.py mirror_audio_to_r2 --podcast=<slug> --stagger=5 --apply
```

| Option | Effect |
|---|---|
| `--episode <id>` | Single episode, run inline (no Celery). |
| `--all` / `--network=<slug>` / `--podcast=<slug>` | Bulk scope (one required). |
| `--origins=gdrive,libsyn,...` | Restrict by `audio_origin()` class. Dead-S3/Megaphone always excluded. |
| `--only-missing` | **Default** — skip episodes that already have `r2_url`. Makes a quota-throttled run resumable: just re-run. |
| `--force` | Re-mirror even if present / source unchanged. |
| `--stagger=N` | Seconds (Celery countdown) between dispatches — respects source rate limits. |
| `--limit=N` | **Sample latch** — process at most N (after filtering). Stops scanning early; for a small prod smoke test before a full run. |
| `--apply` | Dispatch the mirror work. **Default is a preview** that lists targets and dispatches nothing. |
| `--sync` | With `--apply`: mirror inline in this process, surfacing per-episode results. |

> **GDrive backfill is quota-aware.** Drive throttles by per-file download quota, not just the account ceiling. The backfill is resumable (already-mirrored episodes are skipped), so a throttled run can simply be re-run. The task retries with back-off on transient errors.

### Episode admin action

Django admin → **Episodes** → select → **Mirror audio to R2**. Inline in the IDE; staggered Celery dispatch in prod; skips non-premium.

### Maintenance commands

```bash
python manage.py r2_smoke_test [--keep]        # put/get/delete a dummy object under dev/
python manage.py r2_gc [--apply] [--age-days=7]  # reconciliation sweep (records orphans)
python manage.py r2_cleanup_orphans [--apply --yes]  # delete expired, unreferenced orphans
python manage.py purge_r2_dev [--apply --yes]    # hard-delete everything under dev/
python manage.py rename_source_audio_to_r2 [--apply]  # migrate local source-audio files to the R2 naming scheme (dev)
```

All default to **preview**; pass `--apply` to act. The irreversible deletions (`r2_cleanup_orphans`, `purge_r2_dev`) additionally require `--yes` — i.e. `--apply --yes`. See [management-command-conventions.md](management-command-conventions.md) for the uniform safety idiom.

---

## Celery Tasks & Beat Schedule

| Task | Trigger | Purpose |
|---|---|---|
| `task_mirror_episode_audio` | save signal (transcription off) + backfill | Mirror one episode. Default queue, idempotent, retrying. |
| `task_rekey_episode_audio` | move action | Relocate an object to its current-parent key. |
| `task_r2_reconcile` | Beat — **weekly**, Mon 03:30 | Record partial-failure orphans. |
| `task_r2_orphan_cleanup` | Beat — **daily**, 04:00 | Delete expired, unreferenced orphans. |

Beat entries live in `config/celery.py`. The 90-day / 7-day retention windows make the exact cadence non-critical.

---

## Dev Isolation

One prod bucket; the IDE/dev environment writes under the `dev/` key prefix. Dev objects never collide with prod keys (which have an empty prefix) and are bulk-purgeable via `purge_r2_dev --apply --yes`. The reconciliation sweep and cleanup **never touch `dev/`** — it's disposable test data with no retention or orphan tracking. (Tradeoff accepted: dev objects share the public host; fine since they're disposable.)

---

## Security Posture

The bucket is served **public** via the custom domain. Keys carry a 64-bit content hash, so URLs are **not** enumerable/guessable — but they are durable and shareable: a subscriber who extracts the 302 target from `/play/<id>` could repost the raw URL and bypass the paywall. **This is accepted for now.**

Because feeds only ever contain `/play/<id>` (never the raw R2 URL — single chokepoint), signed/expiring URLs can be added later with **no feed change and no re-mirroring**. Evaluated options:

- **Preferred:** a Cloudflare Worker validating an HMAC token (`/play` emits `?exp=&sig=`; a cache rule keyed on path-only preserves edge caching + free egress). ~**$5/mo flat** at ~104k plays/mo (Workers Paid; token check is sub-ms; R2 reads + egress ~$0 with immutable caching). Expiry must outlast a streaming session (~12–24h).
- **Cheaper fallback:** S3 presigned GET URLs from `/play` ($0 Worker fee, but bypasses the custom-domain cache rules and exposes the account endpoint).
- **Rejected:** signed cookies / Cloudflare Access — podcast-app HTTP clients drop cookies across the 302, breaking native players.

> **Multipart hygiene.** The orphan GC only sees completed objects (`ListObjectsV2`). A large upload killed mid-transfer (worker OOM, deploy restart) can strand billable multipart parts that nothing reaps. boto3's transfer manager usually aborts its own parts on a normal failure, but not on a hard kill. Add an R2 **lifecycle rule to abort incomplete multipart uploads after ~1 day** as a backstop — it only ever touches in-progress uploads, never completed objects.

---

## Backup / Disaster Recovery

Keys use immutable IDs, so the bucket alone isn't self-describing — `episode.r2_url` is the authoritative map from episode → object, and the Postgres backup is what makes the audio backup interpretable. **Back up both together.** Re-keying on move keeps the layout accurate so a future recovery is deterministic (files map to episodes by location + the DB index, not guesswork).

R2 egress is free, so copying the bucket out is cheap (you pay only destination storage): `rclone sync` to a second R2 bucket / Backblaze B2 / Glacier-class archive, or R2 event-driven replication. For a pristine snapshot, sync the live-referenced set (keys present in `Episode.r2_url`) so short-lived orphans don't pollute the backup. Audio objects are immutable, so an incremental sync only ever adds new keys. *(Concrete backup tooling is future work, not built yet.)*

---

## Deployment Checklist

When deploying to PROD for the first time:

- [ ] Create the R2 bucket and an Object Read & Write API token.
- [ ] Map the public custom domain (e.g. `audio.joshtheblack.com`) to the bucket, with a Cache Rule (Edge TTL = use cache-control, Browser TTL = respect origin, strong ETags on).
- [ ] Set the `R2_*` variables in `.env` (leave `R2_KEY_PREFIX` **empty** in prod).
- [ ] Confirm `boto3` is in the image (`requirements.txt`) — it lands on the next rebuild.
- [ ] Apply migration `0082` (adds `r2_url`, `r2_uploaded_at`, `r2_source_signature`, `force_r2_serve`, `R2OrphanedObject`).
- [ ] Run `python manage.py r2_smoke_test --keep` and curl the printed URL to confirm the custom-domain mapping serves (the smoke test alone only proves the S3 API path).
- [ ] Add an R2 lifecycle rule to abort incomplete multipart uploads after ~1 day.
- [ ] Single-episode test: `python manage.py mirror_audio_to_r2 --episode <id> --sync`, confirm `/play/<id>` redirects to R2 with working seek + transcript sync.
- [ ] Sample backfill: `mirror_audio_to_r2 --origins=gdrive --limit=5`, then the full run with `--stagger`.
- [ ] Confirm the Beat schedule (`r2-reconcile-weekly`, `r2-orphan-cleanup-daily`) is registered on the Beat container.

---

## Status Reference

| Field (on `Episode`) | Meaning |
|---|---|
| `r2_url` | Public URL of the mirror. Presence == "a backup exists." Indexed; the field the serving layer reads and the source of truth for orphan checks. |
| `r2_uploaded_at` | When the mirror was last written. |
| `r2_source_signature` | `"{etag}:{content_length}"` of the source the mirror was made from — the cheap change-detection gate. Empty for hosts without usable HEAD/ETag (mirror-once). |

| `R2OrphanedObject.reason` | Source |
|---|---|
| `reversion` | Re-mirrored with new content → old key abandoned. 90-day hold. |
| `move_rekey` | Re-keyed on move; byte-identical copy at the new key. 7-day hold. |
| `reconciliation` | Found unreferenced by the weekly sweep. 90-day hold. |
| `manual` | Recorded by hand. 90-day hold. |
