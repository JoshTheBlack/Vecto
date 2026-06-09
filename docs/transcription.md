# Podcast Transcription

Vecto automatically transcribes podcast episodes using [WhisperX](https://github.com/learnedmachine/whisperx-asr-service), an ASR (automatic speech recognition) service built on OpenAI Whisper. Transcripts are generated asynchronously, stored per-episode, exposed in RSS feeds via the [Podcasting 2.0](https://podcastindex.org/namespace/1.0) `podcast:transcript` tag, and displayed inline on each episode's detail page.

---

## How It Works

1. A new episode is saved with a subscriber audio URL → a background Celery task is queued automatically.
2. The Celery worker downloads the subscriber MP3, sends it to the Whisper ASR service, and receives back timestamped segments with speaker labels.
3. Transcripts are written to disk in five formats: VTT, SRT, HTML, Podcast Index JSON, and a word-level JSON file.
4. The RSS feed for that podcast is updated to include `<podcast:transcript>` tags pointing to each format.
5. The episode detail page gains a **Transcript** tab showing the transcript inline with download links.

Transcription runs in a dedicated Celery queue (`transcription`) and never blocks the request cycle. For the IDE environment (no Celery), transcription can be triggered synchronously via the Django admin or a management command.

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

A dedicated Celery worker must consume the `transcription` queue. We recommend splitting it from the default queue to prevent long-running transcription tasks (5–15 minutes each) from starving fast IO-bound tasks like feed syncs.

```yaml
celery-transcription:
  command: celery -A config worker -Q transcription --concurrency=4 -l INFO
  # concurrency=4 is appropriate for medium.en on ~14GB total VRAM
  # reduce to --concurrency=2 when using the large model
```

The concurrency value should be tuned to your GPU memory. `medium.en` uses roughly 3–4GB VRAM per worker slot.

---

## Configuration

All settings live in `.env` and are read via `config/settings.py`.

| Variable | Default | Description |
|---|---|---|
| `WHISPER_URL` | `http://whisper:9000` | Full URL of the Whisper ASR service. In IDE/DEV, point at your Unraid host (e.g. `http://192.168.1.x:9000`). |
| `WHISPER_ENABLED` | `True` | Set to `False` to pause all transcription queuing without removing any code. Useful during backfills or maintenance. |
| `WHISPER_MODEL` | `medium.en` | Default Whisper model size. Must match the `ASR_MODEL` set on the Whisper container. Overridable per-network and per-podcast in the creator settings UI. |
| `WHISPER_LANGUAGE` | `en` | Default BCP-47 language code passed to the ASR service. Overridable per-network and per-podcast. |
| `WHISPER_TIMEOUT` | `5400` | HTTP timeout in seconds for calls to `/asr`. 5400 = 90 minutes, which covers long episodes on slower hardware. |
| `WHISPER_KEEP_SOURCE_AUDIO` | `False` | When `True`, retains the downloaded subscriber MP3 at `MEDIA_ROOT/source_audio/{network}/{podcast}/{filename}`. Useful for DEV debugging. Always `False` in production. |

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

## Transcript Formats

Each completed transcription produces five files stored at `MEDIA_ROOT/transcriptions/{bucket}/{episode_id}.{ext}` (where `bucket = episode_id // 1000`):

| Format | Extension | Use |
|---|---|---|
| WebVTT | `.vtt` | Primary delivery format. Supported by PocketCasts, Apple Podcasts, Overcast, AntennaPod, and most modern clients. Includes per-word timecodes and speaker labels. |
| SubRip | `.srt` | Widely compatible subtitle format. |
| HTML | `.html` | Human-readable, inline display on the episode page. Includes `data-start` attributes per word for future audio player sync. |
| Podcast Index JSON | `.json` | Machine-readable format per the [Podcast Index spec](https://github.com/Podcastindex-org/podcast-namespace/blob/main/transcripts/transcripts.md). |
| Words JSON | `.words` | Word-level timing + speaker data. Source of truth for all other formats — all formats can be regenerated deterministically from this file. Contains a metadata header (`episode_id`, `audio_url`, `language`, `model`, `transcribed_at`, `speaker_mappings`). |

> **The `.words` file is the source of truth.** If transcript files are lost but the `.words` file survives, all other formats can be regenerated from it.

All transcript files are served at `/transcripts/<episode_id>.<ext>` with:
- `Cache-Control: public, max-age=31536000, immutable`
- ETag based on MD5 of file content (changes automatically when files are regenerated, e.g. after speaker label edits)
- `Access-Control-Allow-Origin: *` (podcast clients fetch transcripts cross-origin)
- `Content-Disposition` using the original audio filename stem for readable downloads

---

## RSS Feed Integration

Completed transcripts are exposed in RSS feeds via [Podcasting 2.0](https://podcastindex.org/namespace/1.0) `podcast:transcript` tags, one per format per episode:

```xml
<podcast:transcript url="https://yourdomain.com/transcripts/42.vtt"  type="text/vtt" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.json" type="application/json" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.srt"  type="application/x-subrip" />
<podcast:transcript url="https://yourdomain.com/transcripts/42.html" type="text/html" />
```

Tags are only emitted when `Transcript.status == completed`. The Podcasting 2.0 namespace is always declared on the root `<rss>` element.

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

## Transcript Status Reference

| Status | Meaning |
|---|---|
| `pending` | Task has been queued; waiting for a Celery worker to pick it up |
| `processing` | Worker is actively downloading audio and/or calling the ASR service |
| `completed` | All format files written; transcript is live in the RSS feed and episode page |
| `failed` | An error occurred; see `error_message` on the Transcript record in the admin. Will be re-queued by the next backfill or episode re-save |

The `retry_count` field tracks how many times the Celery task has automatically retried (max 3, with 60/120/180-second back-off).

---

## Docker Stack

The Whisper ASR service is part of the Vecto Docker stack. `docker-compose.yml` includes:

- **`whisper`** — the ASR container (`learnedmachine/whisperx-asr-service:latest`). Reads `ASR_MODEL` from `${WHISPER_MODEL}` in `.env` so it stays in sync with Vecto's model setting. Healthcheck on `/health` with a 120-second start period (the model takes time to load on first start).
- **`celery-transcription`** — dedicated Celery worker consuming only the `transcription` queue (`--concurrency=4`). Starts only after `whisper` passes its healthcheck. Shares the same `media_data` volume as the main `celery` worker.
- **`celery`** — now scoped to `-Q default,celery` so transcription tasks never compete with fast IO-bound tasks like feed syncs.

The `whisper_cache` named volume persists the downloaded Whisper model and pyannote alignment model across container restarts. The alignment model (~360MB) downloads on the first transcription per language, not at startup.

## Deployment Checklist

When deploying to PROD for the first time:

- [ ] Apply all pending migrations (`0071` through `0075`)
- [ ] Add `HF_TOKEN` to your `.env` (required for speaker diarization)
- [ ] Set `WHISPER_ENABLED=True` in `.env`
- [ ] `WHISPER_URL` defaults to `http://whisper:9000` — no override needed once the stack is running
- [ ] `docker compose up -d` — the `whisper` and `celery-transcription` services start automatically
- [ ] Mount `media/transcriptions` and `media/source_audio` as bind mounts to persistent array storage (not Docker named volumes) on Unraid, so transcript files survive stack rebuilds. Both `celery` and `celery-transcription` need access to these paths.
- [ ] Run a test transcription via the Django admin → Episodes → select one episode → **Queue transcription** to confirm end-to-end connectivity

---

## File Storage & Recovery

Transcript files are stored at:
```
MEDIA_ROOT/transcriptions/{bucket}/{episode_id}.{ext}
```
where `bucket = episode_id // 1000`. This keeps each directory to ~1000 files regardless of total episode count.

The `.words` JSON file contains a metadata header that embeds the original subscriber audio URL. This makes recovery possible even after a database rebuild — episodes can be matched by audio URL if the episode ID changes. A future `recover_transcripts` management command will automate this process (spec documented in [planned_features.txt](../planned_features.txt)).
