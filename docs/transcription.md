# Podcast Transcription

Vecto automatically transcribes podcast episodes using [WhisperX](https://github.com/learnedmachine/whisperx-asr-service), an ASR (automatic speech recognition) service built on OpenAI Whisper. Transcripts are generated asynchronously, stored per-episode, exposed in RSS feeds via the [Podcasting 2.0](https://podcastindex.org/namespace/1.0) `podcast:transcript` tag, and displayed inline on each episode's detail page.

---

## How It Works

1. A new episode is saved with a subscriber audio URL → a background Celery task is queued automatically.
2. The Celery worker downloads the subscriber MP3, sends it to the Whisper ASR service, and receives back timestamped segments with speaker labels.
3. Transcripts are written to disk in five formats: VTT, SRT, HTML, Podcast Index JSON, and a word-level JSON file.
4. The RSS feed for that podcast is updated to include `<podcast:transcript>` tags pointing to each format.
5. The episode detail page gains a **Transcript** tab showing the transcript inline with download links.

Transcription runs in dedicated Celery queues (`transcription` and `transcription_heavy`) and never blocks the request cycle. Large models are routed to the heavy queue so they only run on a GPU that can fit them — see [Queue Routing & Priority](#queue-routing--priority). For the IDE environment (no Celery), transcription can be triggered synchronously via the Django admin or a management command.

---

## Requirements

### Whisper ASR Service

The transcription service requires a running instance of [`learnedmachine/whisperx-asr-service`](https://github.com/learnedmachine/whisperx-asr-service). This container exposes a `/asr` HTTP endpoint that Vecto calls with the subscriber audio file.

**Minimum Docker run (no diarization):**

```yaml
whisper:
  image: learnedmachine/whisperx-asr-service:latest
  restart: unless-stopped
  environment:
    - ASR_MODEL=medium.en
    - ASR_ENGINE=faster_whisper
    - SERVE_MODE=ray
    - COMPUTE_TYPE=int8      # required for Pascal GPUs (GTX 10xx); use float16 on Turing+
  volumes:
    - whisper_cache:/root/.cache
```

**With speaker diarization (recommended):**

Add `HF_TOKEN` to the environment. A free [Hugging Face](https://huggingface.co) account and acceptance of the pyannote model license agreements are required.

```yaml
    - HF_TOKEN=hf_your_token_here
```

> **Hardware note:** `COMPUTE_TYPE=int8` is confirmed working on Pascal-generation GPUs (GTX 1080, etc.). `float16` will fail on Pascal with a CTranslate2 error — use `int8` or `float32` on older hardware.

### Celery Worker (production/DEV)

A dedicated Celery worker must consume the transcription queue(s), kept separate from the default queue so long-running transcription tasks (5–15 minutes each) don't starve fast IO-bound tasks like feed syncs.

Run the worker at `--concurrency=1`: a single whisper instance serializes ASR anyway, so parallel slots just contend for the GPU and stretch every task toward the broker visibility timeout. Use one worker per GPU instead of one worker with many slots.

```yaml
celery-transcription:
  # High-VRAM GPU: drain both queues, heavy first (see Queue Routing & Priority)
  command: celery -A config worker -Q transcription_heavy,transcription --concurrency=1 -l INFO
```

See [Queue Routing & Priority](#queue-routing--priority) for how to split the two queues across multiple GPUs.

---

## Configuration

All settings live in `.env` and are read via `config/settings.py`.

| Variable | Default | Description |
|---|---|---|
| `WHISPER_URL` | `http://whisper:9000` | Full URL of the Whisper ASR service. In IDE/DEV, point at your Unraid host (e.g. `http://192.168.1.x:9000`). |
| `WHISPER_ENABLED` | `True` | Set to `False` to pause all transcription queuing without removing any code. Useful during backfills or maintenance. |
| `WHISPER_MODEL` | `medium.en` | Default Whisper model size, sent as the `model` parameter on each `/asr` call. The service switches models per request, so this is the fallback used when no per-network/per-podcast/per-run override is set — it need not match the container's `ASR_MODEL` (that is only the service's own default). Overridable per-network and per-podcast in the creator settings UI. |
| `WHISPER_LANGUAGE` | `en` | Default BCP-47 language code passed to the ASR service. Overridable per-network and per-podcast. |
| `WHISPER_TIMEOUT` | `5400` | HTTP timeout in seconds for calls to `/asr`. 5400 = 90 minutes, which covers long episodes on slower hardware. |
| `WHISPER_KEEP_SOURCE_AUDIO` | `False` | When `True`, retains the downloaded subscriber MP3 at `MEDIA_ROOT/source_audio/{network}/{podcast}/{filename}`. Useful for DEV debugging. Always `False` in production. Retained files are written world-readable/writable (`chmod 777`) — they hold no secrets, and this keeps them accessible once the worker stops running as root. |

### Per-Network Defaults

Network owners can configure transcription defaults in **Creator Settings → Network Profile → Transcription Defaults**:

- **Whisper Model** — model size (tiny → large-v3). Applies to all podcasts on the network unless overridden.
- **Language** — BCP-47 code (e.g. `en`, `es`, `fr`).
- **Min / Default / Max Speakers** — diarization hints. Min=1, Default=2, Max=4 is a good starting point for interview-style podcasts.
- **Initial Prompt** — free-text vocabulary hint passed to Whisper before transcription. Improves accuracy for host names, show titles, and recurring proper nouns. Example: `Hosts: Jim and A.Ron. Shows: Bald Move Pulp, Bald Move Prestige.`

### Per-Podcast Overrides

Each podcast can override any of the above in **Creator Settings → Podcasts → [Show Name] → Transcription Overrides**. Leave a field blank to inherit the network default. Useful when one show is recorded in a different language or features more speakers than the network default expects.

### Settings Fallback Chain

When a transcription task runs, effective settings are resolved in this order:

```
per-run override (admin action / re-transcription panel / backfill UI)
    → podcast-level override (null = skip)
        → network-level default
            → settings.WHISPER_* (.env fallback)
```

---

## Queue Routing & Priority

Transcription work is split across two Celery queues so heavy GPU models are only ever run by a box that can fit them:

| Queue | Models | Priority band (Redis: lower = sooner) |
|---|---|---|
| `transcription` | everything except large\* | high=7, default=8, low=9 |
| `transcription_heavy` | `large`, `large-v2`, `large-v3` | high=0, default=1, low=2 |

`route_transcription()` in `services/transcription.py` picks the queue and priority from the episode's **effective** model (resolved via the [fallback chain](#settings-fallback-chain): per-run → podcast → network → `WHISPER_MODEL`). Every dispatch path uses it — auto-queue on save, the re-transcription panel, the admin **Queue transcription** / **Requeue** actions, the backfill UI, and the `backfill_transcripts` command — so a large model can never leak onto the normal queue.

The two priority bands are carved out of `priority_steps: [0, 1, 2, 7, 8, 9]` in `CELERY_BROKER_TRANSPORT_OPTIONS` (`config/settings.py`). A worker's `BRPOP` scans buckets low→high, so **any** heavy job (0–2) is taken before **any** normal job (7–9), while each band still has three working levels. Slots `3–6` are reserved for future bands.

### Worker placement

Run one Celery worker per GPU and assign queues with `-Q`:

- **High-VRAM GPU** (e.g. RTX 3060, 12 GB) — drains both, heavy first, falling back to normal work when the heavy queue is empty:
  `-Q transcription_heavy,transcription`
- **Lower-VRAM GPU** (e.g. GTX 1080, 8 GB) — normal only, so it never loads a large model:
  `-Q transcription`

Because heavy jobs live in the better priority band, the heavy-capable worker always clears them first regardless of in-band level; the normal-only worker only ever sees the 7–9 band and honors high/default/low within it. `queue_order_strategy: 'priority'` (already set) keeps the listed queue order for same-bucket ties.

> **Which worker, which GPU:** queue assignment decides *which worker* runs a job, and each worker's `WHISPER_URL` points at its own local whisper container — so routing `large-v3` to the high-VRAM worker only helps if that worker's whisper instance has the VRAM. `large-v3` in `int8` is tight on an 8 GB card.

### Per-request model switching

The `learnedmachine/whisperx-asr-service` `/asr` endpoint accepts a `model` query parameter, and Vecto sends the effective model with every request. The container's `ASR_MODEL` is therefore only the service's *default* (used when no model is supplied) — overriding the model per network/podcast/run actually changes which model runs.

---

## Transcript Formats

Each completed transcription produces five files. They live in **Cloudflare R2** (the `vecto-cdn` bucket, served via `cdn.joshtheblack.com`) at `transcripts/{episode_id // 1000}/{episode_id}.{ext}` — see [Transcript Storage on Cloudflare R2](#transcript-storage-on-cloudflare-r2-cdn). When R2 is disabled (`R2_MEDIA_ENABLED=False`, e.g. dev) they fall back to `MEDIA_ROOT/transcriptions/{bucket}/{episode_id}.{ext}` (where `bucket = episode_id // 1000`).

| Format | Extension | Use |
|---|---|---|
| WebVTT | `.vtt` | Primary delivery format. Supported by PocketCasts, Apple Podcasts, Overcast, AntennaPod, and most modern clients. Includes per-word timecodes and speaker labels. |
| SubRip | `.srt` | Widely compatible subtitle format. |
| HTML | `.html` | Human-readable, inline display on the episode page. Includes `data-start` attributes per word for future audio player sync. |
| Podcast Index JSON | `.json` | Machine-readable format per the [Podcast Index spec](https://github.com/Podcastindex-org/podcast-namespace/blob/main/transcripts/transcripts.md). |
| Words JSON | `.words` | Word-level timing + speaker data. Source of truth for all other formats — all formats can be regenerated deterministically from this file. Contains a metadata header (`episode_id`, `title`, `guid_public`, `guid_private`, `audio_url`, `language`, `model`, `transcribed_at`, `speaker_mappings`) — the title/GUIDs/URL are recovery anchors for re-matching after a DB rebuild. |

> **The `.words` file is the source of truth.** If transcript files are lost but the `.words` file survives, all other formats can be regenerated from it.

All transcript formats are reached through the on-platform chokepoint
`/transcripts/<episode_id>.<ext>` (`serve_transcript`). The bytes themselves come
from the edge — see the serving split in [Transcript Storage on Cloudflare R2](#serving-the-three-links).

---

## Transcript Storage on Cloudflare R2 (cdn)

Transcript files are stored in the same `vecto-cdn` bucket as uploaded avatars and mix covers (see [User Image Assets on R2](images.md) for the shared storage backend, cache rule, and CORS setup). This moves the feed-referenced transcript traffic — hit by every podcast app — onto an edge CDN instead of reading each file into a gunicorn worker per request, and is part of making the app container stateless.

Like the images, transcripts use **stable keys + a version-int cache-bust**, so there is **no content hashing, no orphan table, and no GC**:

```
transcripts/{episode_id // 1000}/{episode_id}.{ext}    # ext ∈ vtt json srt html words
```

The key is derived from `episode_id + ext`, so no URL column is stored — the serve view recomputes the target from the key + `Transcript.version`. The `// 1000` bucket folder mirrors the local layout and keeps each prefix to ~1000 episodes (5 objects each) so the bucket stays navigable for recovery. A re-transcribe **overwrites** the same keys in place and bumps `version`; this is safe (unlike audio) because transcripts are tiny single GETs, not range-streamed sessions, so there's no in-flight-stream concern.

> **Recovery metadata is baked into `.words` at upload time.** The `.words` header carries `title`, `guid_public`, `guid_private`, and `audio_url` (alongside `episode_id`, `language`, `model`, `transcribed_at`) so a transcript can be re-matched to its episode after a DB rebuild — episode IDs change on re-import, but those survive. Written by `run_transcription` for new transcripts and merged in by the backfill for existing ones, so no second PUT is ever needed.

### Write path

`run_transcription` and `apply_speaker_labels` write the five formats via `write_transcript_file(episode_id, ext, content)` (`services/transcription.py`):

- **R2** when `R2_MEDIA_ENABLED`, else the legacy `MEDIA_ROOT` path (dev / pre-cutover). Each object is PUT with its per-format `Content-Type` (R2 has no bucket default) and `Cache-Control: public, max-age=31536000, immutable`.
- Both paths then **bump `Transcript.version`** (the `?v=N` cache-bust). The `*_file` fields are kept as per-format existence markers (holding the R2 key or the legacy relpath).

Every (re)transcribe and speaker-label edit rewrites all five keys in place — there's **no content comparison**, by design (see the [no-content-comparison note](images.md#single-write-save) for images). Overwriting is the intended path; `?v=N` busts the cache. The backfill is a run-once migration (state-based `--only-missing`), not a content-sync.

> **`Transcript.version` is the storage signal.** `version == 0` means a legacy, local-only transcript (written before the cutover / with R2 off); `version >= 1` means it's R2-backed. `read_transcript_bytes(episode_id, ext, version)` routes reads accordingly — R2 when `version >= 1` and R2 enabled, else local disk.

### Serving the three links

Each link was always a *different* URL, so download-rename and edge-offload don't fight:

| Link | URL | Behavior |
|---|---|---|
| **RSS feed** | `/transcripts/<id>.<ext>?v=N` | `serve_transcript` **302-redirects** to the immutable cdn object (`?v=N`). Bytes come from the edge; only the 302 touches Django. Uniform, trackable, reversible (mirrors `/play/<id>` → R2). |
| **Download buttons** | `/transcripts/<id>.<ext>?download=1` | Django **streams the bytes from R2** with `Content-Disposition: attachment; filename="<audio-stem>.<ext>"`. Stays same-origin so the `<a download>` rename works (a cross-origin 302 would drop it). Low volume. |
| **Inline page render** | server-side | The episode view reads `html` + `words` **from R2** (`read_transcript_bytes`) to embed the transcript and extract speakers. |

> **The page's client-side `.words` fetch must be versioned.** The episode page's JS re-fetches the `.words` file to build the word-synced "enhanced" transcript, overwriting the server-rendered HTML. With immutable cdn objects that URL **must** carry `?v=N` (`data-words-url=".../words?v={{ transcript.version }}"`), or a re-transcribe shows stale text until a hard refresh. The version bump is exactly what makes `immutable` safe here — it replaces the interim `no-cache` revalidation the local-disk view used.

### Caching model (changed by design)

The old local-disk view sent `Cache-Control: public, no-cache` + a content ETag so a re-transcribe revalidated immediately (an earlier `immutable` attempt caused stale-until-hard-refresh bugs). The R2 design **supersedes** that: objects carry `immutable`, and `?v=N` busts on re-transcribe. Legacy local-only transcripts (`version 0`) are still served the old way (ETag + `no-cache`) until backfilled.

### Backfill — `backfill_transcripts_to_r2`

Uploads existing local transcript files to R2 and sets `Transcript.version`. Same ergonomics as the [image backfill](images.md#backfill--backfill_media_to_r2): idempotent, resumable, dry-runnable, with a presence-aware `--only-missing` (HEADs R2, not just the version) and a `--verify` prune gate.

```bash
python manage.py backfill_transcripts_to_r2 --all --dry-run   # rehearse
python manage.py backfill_transcripts_to_r2 --all             # upload + set version
python manage.py backfill_transcripts_to_r2 --network baldmove --limit 10
python manage.py backfill_transcripts_to_r2 --all --verify    # HEAD every object (prune gate)
python manage.py backfill_transcripts_to_r2 --all --prune --dry-run   # preview prune
python manage.py backfill_transcripts_to_r2 --all --prune            # delete local copies
```

**MIGRATE → VERIFY → PRUNE:** run the backfill, `--verify` confirms every expected object is in R2, then `--prune` deletes the local copies. Deleting a `Transcript` row deletes its cdn objects too (the `post_delete` signal, for `version >= 1`).

> **`--prune` is per-episode all-or-nothing and re-HEADs first.** It deletes an episode's five local format files only after re-confirming **all five** are in R2 — a partially-present episode is left intact. The per-episode `{id}.whisper_raw.txt` debug dump (dev only) is **left in place** for manual cleanup. Honors `--dry-run`.

---

## RSS Feed Integration

Completed transcripts are exposed in RSS feeds via [Podcasting 2.0](https://podcastindex.org/namespace/1.0) `podcast:transcript` tags, one per format per episode:

```xml
<podcast:transcript url="https://yourdomain.com/transcripts/42.vtt?v=3"  type="text/vtt" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.json?v=3" type="application/json" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.srt?v=3"  type="application/x-subrip" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.html?v=3" type="text/html" />
```

Tags are only emitted when `Transcript.status == completed`. The on-platform URL carries `?v=<Transcript.version>` and 302-redirects to the immutable cdn object (see [Serving the three links](#serving-the-three-links)). The Podcasting 2.0 namespace is always declared on the root `<rss>` element.

**Podcast app support:**
- **PocketCasts** — full support, renders VTT with timecode-synced highlighting
- **Apple Podcasts** — generates its own transcripts but respects externally provided ones
- **Overcast** — in progress / partial
- **AntennaPod / Podcast Addict** — Podcasting 2.0 support, VTT renders
- **Spotify** — supports transcripts on hosted content; external RSS support limited

---

## Episode Detail Page

Each episode detail page gains a **Transcript** tab (visible only when a Transcript record exists for the episode). The tab shows:

- A status badge: **Ready** (green) / **Processing** (amber) / **Queued** (grey) / **Failed** (red)
- Inline HTML transcript with speaker blocks and per-word timecode hooks for player sync
- **Download** buttons for VTT and SRT
- **Re-transcription panel** (network owners only) — see [Re-transcription](#re-transcription) below
- **Speaker Labels form** — see [Speaker Identification](#speaker-identification) below

---

## Automatic Triggering

Transcription is queued automatically whenever an episode is saved with a subscriber audio URL and no pending/processing/completed transcript exists. A `Transcript` record is created immediately with `status=pending` so the admin and episode page display a **Queued** badge while the task waits for a Celery worker.

> **Only subscriber audio is transcribed.** Public audio has dynamic ad insertion — its content differs per listener, so transcribing it would produce an incorrect transcript. Episodes with only public audio are silently skipped.

Scheduled episodes are queued immediately — the transcript can be ready before the episode is published.

---

## Manual Triggering

### Django Admin Action

In the Django admin → **Episodes**, select one or more episodes and choose **Queue transcription** from the actions dropdown. The action:

- Skips episodes without subscriber audio
- Skips episodes already pending, processing, or completed (delete the `Transcript` record in the admin to force a re-run)
- In the IDE environment (`IS_IDE=True`): runs synchronously in-process
- In DEV/PROD: dispatches via Celery with a 30-second stagger between episodes

### Transcript admin — Requeue actions

In the Django admin → **Transcripts**, selecting records exposes requeue actions that reset the chosen transcripts to `pending` and re-dispatch them:

- **Requeue transcription — HIGH / default / LOW priority** — re-runs with each episode's *effective* model, at the chosen [priority band](#queue-routing--priority).
- **Requeue large-v3 — HIGH / MEDIUM / LOW priority** — forces the `large-v3` model for the run. Because `large-v3` is a heavy model, `route_transcription()` sends it to the `transcription_heavy` queue, and the model is passed through as a task kwarg so the worker actually runs it.

### Management Command

For scripted use or IDE testing without the admin:

```bash
python manage.py transcribe_episode <episode_id>
```

Runs synchronously. Safe to use without a Celery worker.

---

## Bulk Backfill

To transcribe a large number of existing episodes:

### Via Creator Settings UI

**Creator Settings → Transcripts tab:**
- Pick a podcast (or leave blank for all podcasts)
- Set a stagger delay (seconds between Celery dispatches, default 30)
- Optionally override model, language, initial prompt, and speaker counts for this backfill run
- Click **Queue Backfill**

Only episodes with subscriber audio and no pending/processing/completed transcript are queued. Failed transcripts are re-queued.

### Via Management Command

```bash
python manage.py backfill_transcripts [options]

Options:
  --podcast=<slug>          Limit to a single podcast
  --model=<name>            Whisper model override (e.g. large-v3)
  --language=<code>         Language override (e.g. es)
  --initial-prompt=<text>   Vocabulary hint override
  --min-speakers=<n>        Diarization min speakers override
  --num-speakers=<n>        Diarization expected speakers override
  --max-speakers=<n>        Diarization max speakers override
  --stagger=<seconds>       Countdown between dispatches (default 30)
```

---

## Re-transcription

Network owners can re-transcribe any individual episode from the episode detail page → **Transcript** tab → **Re-transcribe** button.

Clicking the button reveals an options panel pre-populated with the podcast/network defaults. Any field can be left blank to use the current defaults at execution time. On submit:

1. The transcript status is reset to `pending`
2. A new Celery task is queued with the selected options
3. Existing transcript files are overwritten when the task completes
4. RSS fragment caches are busted automatically

### Audio source override

The panel also exposes an **Audio source** selector. It defaults to *— default (cached / subscriber) —*, which behaves exactly as automatic transcription does (reuse the cached file if present, otherwise download the subscriber URL). Selecting a specific source — **Private (subscriber)**, **R2 mirror**, or **Public** (shown only when one exists) — forces a *fresh* download from that URL and deletes any cached source file first. Only the episode's own audio URLs are accepted (validated server-side and SSRF-checked); the freshly downloaded override bytes are never written into the shared `source_audio/` cache.

> **R2 is written only from the subscriber source.** A re-transcription from the **Private (subscriber)** source re-runs the [R2 mirror](audio-mirroring.md) with a content-hash comparison: identical bytes dedupe with no upload, changed bytes upload and the superseded object is recorded for garbage collection. The **R2** and **Public** sources are transcribed but **never** written back to the mirror — public audio carries dynamic per-listener ads and must never become the served backup.

---

## Speaker Identification

### How Diarization Works

When `diarize=True` is set (always on), the ASR service attempts to separate speakers and assigns labels like `SPEAKER_00`, `SPEAKER_01` to each segment and word. This requires a Hugging Face token (`HF_TOKEN`) on the Whisper container.

The number of speakers expected can be tuned via the min/default/max speaker settings at the network and podcast level.

### Mapping Labels to Names

After a transcript is complete, any authenticated user can submit speaker name assignments from the episode detail page → **Transcript** tab → **Speaker Labels** form. Entering `SPEAKER_00 → Jim` and `SPEAKER_01 → A.Ron` and submitting:

1. Creates an **Edit Suggestion** in the normal trust-score / approval workflow (same system used for episode metadata edits)
2. **Trusted users** (above the network's auto-approve threshold): mapping is applied immediately, all transcript formats are regenerated, and the RSS fragment cache is busted
3. **Standard users**: the suggestion goes to the creator inbox for review and approval

When approved, `apply_speaker_labels()` updates `segment.speaker` and `word.speaker` fields in the `.words` JSON **by key lookup only** (not string replace), then regenerates all format files from the updated data. The ETag on served files changes automatically.

The approval appears in the audit log with a before/after diff of the speaker mapping.

> **Rollback:** Like all edit suggestions, speaker label approvals can be rolled back. The `original_data` field captures the previous speaker mapping, enabling one-click reversal.

---

## Transcript Search

The home page search bar includes a **Search within transcripts** toggle (chat-quote icon). When active, search results include episodes whose `transcript_text` field contains the query term, in addition to the normal title and description matches.

Transcript search uses a SQL `LIKE` query on the `transcript_text` field. This is sufficient for moderate corpus sizes. See the [planned_features.txt](../planned_features.txt) Phase 8 section for a documented upgrade path to PostgreSQL full-text search (GinIndex + SearchVector + snippet extraction) when the corpus grows large enough to require it.

---

## Error Handling & Source Validation

### Captured ASR errors

When the Whisper ASR call fails, the service raises a FastAPI `HTTPException` whose JSON body is `{"detail": "<the engine's own error>"}` — out-of-memory, diarization failure, unreadable audio, `413` (file too large), `400` (bad parameters), and so on. Vecto extracts that `detail` and stores it on `Transcript.error_message` (prefixed with the HTTP status), so the admin shows the **real** reason a run failed instead of a bare `500 Server Error for url: …`. Non-HTTP failures (download errors, parse errors) fall back to the exception text.

### HTML-instead-of-audio guard

Some hosts — Google Drive most of all — hand back an HTML page (a download interstitial or a quota wall) instead of the MP3. Sending that to the ASR engine wastes a GPU run. So **before every ASR call** the audio file is validated: any file under ~1 MB whose first bytes look like HTML (`<!doctype html>`, `<html>`, `<head>`, …) is discarded and re-downloaded, up to **5 download attempts** with a short back-off between them. If all five still return HTML, the transcription **fails without ever calling ASR** (`error_message` records the HTML source) and the normal Celery retry/back-off applies.

The cached file (when `WHISPER_KEEP_SOURCE_AUDIO=True`) is checked first and deleted if it is HTML, without consuming a download attempt. The guard runs on every source/host, not just Google Drive.

---

## Transcript Status Reference

| Status | Meaning |
|---|---|
| `pending` | Task has been queued; waiting for a Celery worker to pick it up |
| `processing` | Worker is actively downloading audio and/or calling the ASR service |
| `completed` | All format files written; transcript is live in the RSS feed and episode page |
| `failed` | An error occurred; see `error_message` on the Transcript record in the admin — it carries the ASR engine's own `detail` when the failure came from `/asr`. Will be re-queued by the next backfill or episode re-save |

The `retry_count` field tracks how many times the Celery task has automatically retried (max 3, with 60/120/180-second back-off).

---

## Docker Stack

The Whisper ASR service is part of the Vecto Docker stack. `docker-compose.yml` includes:

- **`whisper`** — the ASR container (`learnedmachine/whisperx-asr-service:latest`). Reads `ASR_MODEL` from `${WHISPER_MODEL}` in `.env` so it stays in sync with Vecto's model setting. Healthcheck on `/health` with a 120-second start period (the model takes time to load on first start).
- **`celery-transcription`** — dedicated Celery worker for transcription (`--concurrency=1`). In the single-box dev stack it drains both queues, heavy first (`-Q transcription_heavy,transcription`); in a multi-GPU prod setup, split the queues across workers per [Queue Routing & Priority](#queue-routing--priority). Starts only after `whisper` passes its healthcheck. Shares the same `media_data` volume as the main `celery` worker.
- **`celery`** — now scoped to `-Q default,celery` so transcription tasks never compete with fast IO-bound tasks like feed syncs.

The `whisper_cache` named volume persists the downloaded Whisper model and pyannote alignment model across container restarts. The alignment model (~360MB) downloads on the first transcription per language, not at startup.

## Deployment Checklist

When deploying to PROD for the first time:

- [ ] Apply all pending migrations (`0071` through `0075`)
- [ ] Add `HF_TOKEN` to your `.env` (required for speaker diarization)
- [ ] Set `WHISPER_ENABLED=True` in `.env`
- [ ] `WHISPER_URL` defaults to `http://whisper:9000` — no override needed once the stack is running
- [ ] `docker compose up -d` — the `whisper` and `celery-transcription` services start automatically
- [ ] Mount `media/source_audio` as a bind mount to persistent array storage (not a Docker named volume) on Unraid. With `R2_MEDIA_ENABLED`, new transcripts go to R2, so the `media/transcriptions` bind mount is only needed for **legacy** (`version 0`) transcripts until they're backfilled + pruned.
- [ ] Run a test transcription via the Django admin → Episodes → select one episode → **Queue transcription** to confirm end-to-end connectivity
- [ ] **R2 transcript cutover** (after the [image CDN setup](images.md)): with `R2_MEDIA_ENABLED=True`, run `backfill_transcripts_to_r2 --all` then `--all --verify`; only after verify passes, prune the local `media/transcriptions` files.

---

## File Storage & Recovery

Transcript files are stored in R2 at `transcripts/{episode_id // 1000}/{episode_id}.{ext}` when `R2_MEDIA_ENABLED` (see [Transcript Storage on Cloudflare R2](#transcript-storage-on-cloudflare-r2-cdn)). With R2 disabled they fall back to:
```
MEDIA_ROOT/transcriptions/{bucket}/{episode_id}.{ext}
```
where `bucket = episode_id // 1000` (keeps each directory to ~1000 files). Both layouts use the same `// 1000` bucketing.

The `.words` JSON file embeds `title`, `guid_public`, `guid_private`, and `audio_url` in its metadata header. This makes recovery possible even after a database rebuild — episodes can be re-matched by GUID/title/audio URL when the episode ID changes. A future `recover_transcripts` management command will automate this process (spec documented in [planned_features.txt](../planned_features.txt)).
