# User Image Assets on Cloudflare R2 (cdn)

Vecto stores uploaded **avatars** and **mix covers** in a dedicated [Cloudflare R2](https://developers.cloudflare.com/r2/) bucket (`vecto-cdn`), served through a cached custom domain (`cdn.joshtheblack.com`). This moves the only irreplaceable user uploads off the app container's local disk onto durable off-box storage, edge-serves them, and is a step toward a **stateless** container.

This reuses the account-level R2 plumbing built for [audio mirroring](audio-mirroring.md) (the `r2_client.py` credentials, the boto3 checksum workaround, the `dev/` prefix convention) but is a much **simpler** machine: stable keys + in-place overwrite + a version-int cache-bust mean **no content hashing, no orphan table, and no garbage collector**.

> **The driver here is durability + statelessness, not cost.** These files are tiny (text-free WebP images); egress on them is a rounding error. Storage is well under $0.10/mo. The point is that losing/recreating the container no longer loses user uploads, and it unblocks running more than one app container.

---

## What Moves (and What Doesn't)

**Stored on R2 (`vecto-cdn`):**

| Asset | Field | Processed to |
|---|---|---|
| Custom avatar | `NetworkMembership.custom_image_upload` | 256px square WebP |
| User-mix cover | `UserMix.image_upload` | 500px square WebP |
| Network-mix cover | `NetworkMix.image_upload` | 500px square WebP |

**Never stored — pass-through external URLs** (Vecto never hosted these): Discord, Patreon, and Gravatar avatars, plus `NetworkMembership.custom_image_url` (a user-pasted link). `display_avatar` returns these live remote URLs unchanged. The *only* stored avatar kind is `custom_image_upload`.

**Stays on local/default storage — deliberately:**

- `InvoiceProfile.pdf_file` — **PII**. Lives on the `FileSystemStorage` default and must never enter the public cdn bucket.
- Recovery runs and `source_audio/` — operational/temp, regenerable, not user-facing.

> **Never swap Django's global `default` storage.** Doing so would silently sweep invoices (PII) and recovery files into the public cdn. Only the three per-field storages above are changed. (See [Storage Backend](#storage-backend).)

---

## How It Works

1. A user uploads an avatar or cover. The model `save()` detects a **fresh upload**, processes the bytes to a 256/500px square **WebP in memory**, and writes them to R2 **once** at a stable key.
2. The DB row's `image_version` is bumped. The URL helpers append `?v=<image_version>` when building the public URL.
3. The browser/edge see `?v=3` vs `?v=2` as distinct cache entries — that **is** the cache-bust. The object key itself never changes; the bytes are overwritten in place.

Because the key is stable and the extension is always `.webp`, a re-upload **overwrites** the same object — nothing is ever stranded, so none of the audio bucket's orphan/GC machinery is needed here.

> **Put without the cache key; get with it.** `?v=N` is a **query string**, not part of the R2 object key. The object lives at the bare key (`avatars/8-1.webp`); `image_version` lives in the DB and is appended only when building the URL. This is why the Cloudflare cache rule **must keep the query string in its cache key** (see [Cloudflare Setup](#cloudflare-setup)).

---

## Why a Separate Bucket from `vecto-audio`

An R2 custom domain maps to exactly one bucket, and we want a clean `cdn.joshtheblack.com`. More importantly: the **audio** bucket runs a destructive orphan GC — a weekly sweep lists the entire keyspace and records anything not referenced by an `Episode.r2_url` as an orphan, and a daily job hard-deletes orphans after 90 days. Dropping avatars/covers into `vecto-audio` would get them **deleted**. A separate bucket makes that cleanup structurally unable to touch these assets.

Within `vecto-cdn`, asset types are separated by key prefix only (`avatars/`, `covers/`, and later `transcripts/`). Neither has a GC, so there's nothing to isolate them from each other.

---

## Configuration

New settings live in `.env`, read via `config/settings.py` (same pattern as the `R2_*` / `WHISPER_*` blocks). They **reuse** `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` from the audio mirror — the same account/token, scoped to **both** buckets.

| Variable | Default | Description |
|---|---|---|
| `R2_MEDIA_BUCKET` | `vecto-cdn` | The user-asset bucket. Separate from `R2_BUCKET` (`vecto-audio`). |
| `R2_MEDIA_PUBLIC_HOST` | — | Custom domain, no trailing slash (e.g. `https://cdn.joshtheblack.com`). |
| `R2_MEDIA_KEY_PREFIX` | `""` prod / `"dev/"` IDE | django-storages `location`. Namespaces dev objects so they're bulk-purgeable. **Defaults to `dev/` under `IS_IDE`** even if `.env` omits it, so a forgotten entry can't write into the prod keyspace. |
| `R2_MEDIA_ENABLED` | `True` | Master switch. When `False`, the per-field storage falls back to the local `OverwriteStorage` so the app runs without R2. Forced `False` under tests (mirrors `R2_MIRROR_ENABLED`). |

> **The R2 API token must grant Object Read & Write on `vecto-cdn`.** The audio mirror's token was scoped to `vecto-audio` only; reusing it without widening the scope yields `AccessDenied` on `PutObject`. Scope the token to both buckets (or all buckets in the account).

> **Changing `R2_MEDIA_ENABLED` requires a server restart.** The storage backend binds when the model field is constructed (at import), so the flag is read once at startup. Flipping it in `.env` has no effect until the process restarts.

---

## Object Naming (R2 keys)

```
{R2_MEDIA_KEY_PREFIX}avatars/{user_id}-{network_id}.webp
{R2_MEDIA_KEY_PREFIX}covers/{usermix_or_networkmix_unique_id}.webp
```

- IDs, not slugs — keys are stable across renames (object storage has no real rename).
- `custom_image_upload` is unique per `(user, network)`, so the id-pair key is unique and stable. Mix covers key off the model's existing non-guessable `unique_id` UUID.
- **The extension is always `.webp`.** All processed images are normalized to a single format so the key's extension is constant. If a user replaced a `.jpg` with a `.png`, a format-derived extension would change the key and strand the old object (the exact orphan we avoid). One format = one key = a true overwrite.

### Upload headers (set per-object; R2 has no bucket-level default)

- `Content-Type: image/webp` — set by django-storages from the key.
- `Cache-Control: public, max-age=31536000, immutable` — set via the storage's `object_parameters`. Required: the Cloudflare cache rule bypasses cache for objects with no `Cache-Control`. Immutable is safe because the `?v=N` query string busts on update.

---

## Image Processing

`services/images.py::process_image_field(field, max_px)`:

1. Centre-crop to a square, resize down to `max_px` (256 avatars / 500 covers).
2. Normalize mode for WebP: preserve alpha (`RGBA`) where the source has it (`RGBA`/`LA`/`PA`/palette-with-transparency); otherwise `RGB`.
3. Encode as WebP (`quality=85, method=6`).

Output is one normalized WebP per asset, returned as bytes.

### Single-write save()

Each model's `save()` does **one** write on a fresh upload (vs. the old two-write "raw PUT then processed re-save"):

```python
if self.image_upload and not self.image_upload._committed:   # fresh upload only
    data = process_image_field(self.image_upload, 500)        # process in memory
    self.image_upload.save('cover.webp', ContentFile(data), save=False)  # ONE PUT at the stable key
    self.image_version = (self.image_version or 0) + 1
super().save(*args, **kwargs)
```

`FieldFile._committed` is `False` only for a just-assigned upload, so plain row re-saves never reprocess or re-PUT. The `upload_to` callable returns the stable key regardless of the passed filename; storage `file_overwrite` (R2) / `OverwriteStorage` (local) replaces any existing object.

> **No content comparison — the gate is "did a file get uploaded?", not "did the bytes change?"** Re-uploading the *same* image is a fresh upload, so it re-processes and re-PUTs identical bytes (overwriting the stable key, bumping the version). That redundant write is intentional and harmless — a tiny file, a sub-cent Class A op — and is the deliberate trade for skipping content hashing. (The **audio** mirror hashes precisely because its files are large; here the simplification wins.) The backfill is likewise state-based: `--only-missing` skips on R2 *presence*, not byte-equality, so it's a run-once migration, not a content-sync.

---

## Storage Backend

`services/r2_storage.py` defines `R2MediaStorage` (a django-storages `S3Boto3Storage`) and a per-field `select_media_storage()` callable:

- Pointed at `R2_MEDIA_BUCKET` / `R2_MEDIA_PUBLIC_HOST`, `region_name="auto"`, the **same** `when_required` checksum config as `r2_client` (or R2 can 400 the PUT).
- `file_overwrite = True` — the crux of the no-GC design.
- `querystring_auth = False` — objects are public; URLs stay clean and `?v=N` is appended by the model, not boto signing.
- `object_parameters` set the immutable `Cache-Control` on every PUT.
- `select_media_storage()` returns the R2 backend when `R2_MEDIA_ENABLED`, else the local `OverwriteStorage` (so dev re-uploads overwrite in place just like R2). It's a **callable**, so Django records it by reference in migrations — the backend choice is never baked into a migration file.

It is attached **per field** (`storage=select_media_storage`). The global `default` storage stays `FileSystemStorage`.

---

## URL Helpers (where `?v=N` is applied)

`models.py::_versioned_image_url(fieldfile, version)` is the single place that builds a cache-busted URL:

- New stable webp keys → `fieldfile.url` + `?v=<version>` (cdn, or local `/media` when R2 is off).
- **Legacy** pre-cutover keys (`custom_avatars/`, `mix_covers/`) → served from `/media` unchanged, so un-migrated rows keep working until the backfill re-keys them. **Delete this branch once every row is migrated.**

Consumers:

| Helper | Used by | Returns |
|---|---|---|
| `NetworkMembership.display_avatar` | nav avatar, member lists | preferred source; custom branch is versioned |
| `NetworkMembership.custom_avatar_url` | profile avatar **picker** (renders the upload directly) | versioned custom-upload URL |
| `UserMix.display_image` / `NetworkMix.display_image` | mix artwork | versioned cover URL or the external `image_url` |

> **Render the upload through a helper, not `.url`.** A bare `field.url` has no `?v`, so a re-upload serves the stale edge/browser copy. Django **admin** uses `.url` directly (unavoidable, staff-only), so re-uploads can look stale there until a hard refresh.

---

## Deletion / Replacement (no GC)

- Replacing an image overwrites the same stable key — nothing is orphaned.
- The `post_delete` signals call `field.storage.delete(name)`, which now deletes from R2.
- The avatar upload view (`upload_custom_avatar`) does **not** pre-delete before a new upload (the overwrite PUT handles it, and avoids a momentary 404 gap). It **does** delete when the user switches from an upload to an external `custom_image_url` — there's no overwrite there, so the object must be explicitly removed.

---

## Backfill — `backfill_media_to_r2`

Uploads existing local images to R2 and rewrites the DB so the new stable keys are authoritative. Clones the `mirror_audio_to_r2` ergonomics: idempotent, resumable, **preview by default** (pass `--apply` to act).

```bash
# rehearse — list what would move, change nothing (preview is the default)
python manage.py backfill_media_to_r2 --all

# migrate everything (avatars + covers)
python manage.py backfill_media_to_r2 --all --apply

# one asset class, sample first
python manage.py backfill_media_to_r2 --avatars --limit 5 --apply

# re-process rows already migrated (bumps image_version)
python manage.py backfill_media_to_r2 --all --apply --force

# VERIFY: HEAD every migrated object in R2 (read-only prune gate)
python manage.py backfill_media_to_r2 --all --verify

# PRUNE: delete the local copy of each stable-key image confirmed in R2
python manage.py backfill_media_to_r2 --all --prune             # preview
python manage.py backfill_media_to_r2 --all --prune --apply --yes   # delete
```

For each row with a local file it reads the bytes, processes to WebP, writes once to R2 at the stable key, and sets the field `.name` + bumps `image_version`. The old local file is left in place until the post-cutover prune.

> **`--only-missing` (default) checks actual R2 presence, not just the name.** A stable-key name alone isn't proof the bytes reached R2 — e.g. a row written to local disk while `R2_MEDIA_ENABLED=False`, then re-enabled, holds a stable name with no R2 object. The skip does a HEAD and only skips when the object truly exists, so `--all` fills in any gap. (`--force` re-uploads regardless.)

**MIGRATE → VERIFY → PRUNE:** run the backfill, `--verify` (HEADs every expected object) confirms presence, then `--prune` deletes the local copies. The verify/HEAD gate — not a time delay — is what makes the prune safe.

> **`--prune` re-confirms R2 presence per file before deleting**, so it can never remove something not safely mirrored. It only touches **stable-key** rows; **legacy-keyed** rows are skipped (they're still served from `/media` via the transition branch until that's removed). It previews unless `--apply`; the irreversible delete needs `--apply --yes`. In prod, images were written straight to R2, so there's often nothing local to prune — the leftover legacy directories (`media/custom_avatars/`, `media/mix_covers/`) are removed when the transition branch is deleted.

---

## Cloudflare Setup

**Bucket + domain:** create `vecto-cdn`, map `cdn.joshtheblack.com`.

**Cache Rule** (Caching → Cache Rules), scoped to the cdn host so it doesn't touch `audio.joshtheblack.com`:

- Expression: `(http.host eq "cdn.joshtheblack.com")`
- Cache eligibility: **Eligible for cache**
- **Edge TTL: "Use cache-control header if present, bypass cache if not."** Caches exactly when our object says to. The "bypass if absent" fallback is the safe one — it never caches a negative/`404` response under a default TTL. (This is why image objects **must** carry `Cache-Control`.)
- **Browser TTL: Respect origin TTL.** Passes the immutable `max-age` to the browser; `?v=N` busts it on update.
- **Cache key → Ignore query string: OFF.** The one setting that matters — the query string must stay in the cache key or `?v=N` won't invalidate the edge copy.

**CORS:** add a bucket CORS rule if images are ever Web-Audio/canvas-read cross-origin (transcripts will need it for the JSON/VTT fetches — see [transcription docs](transcription.md)).

**Lifecycle:** abort incomplete multipart uploads after ~1 day (cheap backstop; consistent with the audio bucket).

### Smoke test

```bash
curl -sI "https://cdn.joshtheblack.com/dev/avatars/<u>-<n>.webp?v=1" | grep -i cf-cache-status
# first hit MISS, second HIT; bump to ?v=2 -> fresh MISS (proves query-string busting)
```

---

## Dev Isolation

Dev/IDE writes under the `dev/` prefix (`R2_MEDIA_KEY_PREFIX`), sharing the prod bucket like audio. Dev objects are namespaced and bulk-purgeable. The local fallback (`R2_MEDIA_ENABLED=False`) writes the same stable keys to `MEDIA_ROOT` via `OverwriteStorage`, so dev behaves like prod (overwrite-in-place) without touching R2.

> Toggling `R2_MEDIA_ENABLED` mid-stream is a dev-only hazard: images written under one backend aren't visible to the other (local bytes vs. R2 object). The backfill's presence-aware skip recovers from this (`--all` re-pushes anything missing from R2). In prod the flag is set once and left.

---

## Deployment Checklist

1. `pip install` — `django-storages` is in `requirements.txt` (boto3 already present).
2. Create `vecto-cdn` + map `cdn.joshtheblack.com`; add the Cache Rule + CORS above.
3. Widen the R2 API token to Object Read & Write on `vecto-cdn` (keep `vecto-audio`).
4. Set the `R2_MEDIA_*` block in the prod `.env` (prefix empty in prod).
5. Apply migrations (`image_version` + the per-field storage changes).
6. Restart the app so the storage backend binds to R2.
7. `backfill_media_to_r2 --all`, then `--all --verify`.
8. After verify passes, prune local image files and remove the legacy URL-helper branch.

---

## What This Does *Not* Do

- No content-addressed keys, no orphan table, no GC for this bucket — deliberately avoided (stable keys overwrite in place).
- No egress optimization — not the point; these files are tiny.
- Does **not** move invoices (PII), recovery runs, or `source_audio/`. Full container statelessness (invoices → a **private** bucket, separate from `vecto-cdn`) is a distinct future effort.
- No signed/expiring URLs — these assets are public.
