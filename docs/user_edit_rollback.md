# User-Submitted Edits — Rollback & Reward Rework

_Scope: immutable speaker-id base + replay rollback, exact-wash trust/counter
reversal for every user-submitted edit field, a unified reward scorer, and the
cross-publish lockdown. (Originally "Transcript Speaker Labels — Rollback &
Immutable Base".)_

> **Status:** Implemented. All phases (1–9) are complete; this document remains the
> source of truth and the implementation-progress log of record.

## Implementation progress

_Each implementation window appends its entry below (window, phases, what changed,
any deviations from the plan) and ticks the phase in §10. Read this before starting._

- **Window 1 — Phases 1 & 2 (§3.5, §4).** Foundational write-path changes only;
  the replay engine (Phase 3) is untouched, so `apply_speaker_labels` keeps its
  current mappings-argument signature and write logic for now.
  - **Phase 1 (§3.5)** — `_parse_whisper_response` now stamps a write-once
    `speaker_id` on every segment **and** word via a new `_stamp_speaker_ids`
    helper (idempotent: only sets where absent; SRT-fallback / undiarized
    segments get none). At initial transcription `speaker == speaker_id == the
    raw SPEAKER_XX label`. `_to_words_json` emits **both** `speaker_id` and
    `speaker` (segment + word) and the `.words` schema version is bumped
    `1.0.0 → 1.1.0`. `_to_vtt` / `_to_srt` / `_to_podcast_index_json` / `_to_html`
    are **unchanged** — they still emit resolved `speaker` only, so `speaker_id`
    never reaches a feed format. (The podcast-index `json` keeps its own separate
    `version: "1.0.0"`.)
  - **Phase 2 (§4)** — added `media_object_etag(key)` to
    [r2_storage.py](../pod_manager/services/r2_storage.py) (unquoted ETag via a
    Class B HEAD, `None` on 404). New `write_transcript_formats(episode_id,
    rendered)` in [transcription.py](../pod_manager/services/transcription.py)
    hash-checks each format before PUT: on R2 it compares `md5(new_bytes)` to the
    object's ETag and PUTs only changed formats, falling back to GET+hash when the
    ETag isn't a plain 32-char md5 (multipart). Local (R2-disabled) path always
    writes (free) and reports all formats changed. `run_transcription` now calls
    it and bumps `Transcript.version` **only when ≥1 format changed**, after all
    writes succeed (partial-write safety). A brand-new transcript writes all five
    (all "changed") so version still goes 0→1 as before.
  - **Deviations:** none from the plan. `apply_speaker_labels` deliberately left
    on the old write path (unconditional bump, no hash-check) — it converges onto
    `write_transcript_formats` in Phase 3 per the brief.
  - **Tests:** added `_to_words_json` speaker_id + `1.1.0` schema assertions,
    `_parse_whisper_response` speaker_id stamping, `MediaObjectEtagTests`, and
    `WriteTranscriptFormatsR2Tests` (skip-unchanged / put-missing / multipart
    fallback). Full suite (390 tests, incl. the new ones) green via
    `./.venv/Scripts/python.exe manage.py test`.

- **Window 2 — Phases 3, 4 & 8 (§3.2–§3.4, §7).** Replay engine, speaker-aware
  rollback, and re-transcription supersede. Phase 5 (trust counters / §8a),
  Phase 6 (backfill) and Phase 7 (front-end / review surfaces) remain.
  - **Phase 3 (§3.2–§3.3, replay engine)** — `apply_speaker_labels` lost its
    `speaker_mappings` argument; it is now `apply_speaker_labels(episode_id)` and
    **recomputes from base**: new `fold_speaker_mappings(episode_id)` folds the
    episode's `APPROVED` speaker edits (last-writer-wins per `speaker_id`, ordered
    `resolved_at, id`) and each segment/word resolves `speaker = mapping.get(speaker_id, speaker_id)`.
    The pristine `speaker_id` is preserved verbatim (never rewritten); the rewritten
    `.words` re-emits it. Renders all 5 formats and uses Phase 2's idempotent
    `write_transcript_formats` (hash-check, version bump only on real change). The
    whole read→render→write→bump runs inside `transaction.atomic()` +
    `select_for_update()` on the `Transcript` row (per-episode lock, §3.3).
    **Robustness:** `seg_id = seg.get('speaker_id') or seg.get('speaker')` (word-level
    too) so pre-backfill `.words` with no `speaker_id` still label correctly.
    `original_data.speaker_mappings` (submit-time audit before-state) and the
    `.words` header's `speaker_mappings` cache are both still written.
  - **Call sites converged to the no-arg form:** `edits.apply_approved_edit`
    speaker branch; `actions._handle_approve_edit` (see deviation below);
    `submit_speaker_labels`'s trusted path is unchanged in shape — it still creates
    the row `APPROVED` *then* calls `apply_approved_edit`, so the fold already
    includes the new edit.
  - **`SUPERSEDED` status** added to `EpisodeEditSuggestion.Status`
    (migration `0087_alter_episodeeditsuggestion_status`). It's a distinct status,
    so the `status=APPROVED` fold/rollback filters exclude it automatically.
  - **Phase 4 (§3.4, rollback handlers)** — single + bulk rollback in
    [actions.py](../pod_manager/views/creator/actions.py) are now speaker-aware
    (`'speaker_mappings' in edit.suggested_data`). **Single:** the
    "newer approved edits exist" blocker is skipped for speaker edits (replay is
    order-correct); flip to `ROLLED_BACK`, then `apply_speaker_labels(ep.id)`.
    Metadata edits keep the blocker + the field-restore path verbatim. **Bulk:**
    speaker edits are flipped in the loop and collected into a deduped set, then
    replayed **once per affected episode** after the loop (metadata edits restore
    in-loop as before). Trust accounting left **as-is** per the brief (flat `−5`
    single, zeroing on bulk) — Phase 5 replaces it.
  - **Phase 8 (§7, re-transcription)** — new `supersede_speaker_edits(episode_id)`
    marks all prior `APPROVED`/`PENDING` speaker edits `SUPERSEDED` (via a
    `suggested_data__has_key='speaker_mappings'` filter) under the same per-episode
    `Transcript` `select_for_update` lock. `run_transcription` calls it right after
    a transcript is marked `COMPLETED` — a no-op for brand-new transcripts (no prior
    edits), and the freshly-written formats already carry raw labels, so superseding
    only stops the stale chain from being re-applied by a future replay.
  - **Deviations:**
    1. **Approve-handler ordering.** The old speaker block applied labels *before*
       the row was flipped to `APPROVED`; under recompute-from-base that would
       exclude the very edit being approved. Reworked so the speaker block only
       scores (`points += 1`) + sets an `apply_speaker` flag, and the actual
       `apply_speaker_labels(ep.id)` runs **after** `edit.status = APPROVED` is
       saved. Also dropped that block's own fragment-rebuild (it referenced an
       **unimported `settings`** — a latent `NameError` on the non-custom-domain
       path); the function's existing end-of-handler `task_rebuild_episode_fragments`
       covers it.
    2. **File markers on replay.** `apply_speaker_labels` now also writes the
       `*_file` markers returned by `write_transcript_formats` (only when ≥1 format
       changed), where the old version saved `version` alone. Markers are
       deterministic from id+ext, so this is harmless and keeps the row consistent
       with `run_transcription`.
  - **Tests:** rewrote `ApplySpeakerLabelsTests` to drive state via `APPROVED`
    edit rows + the no-arg call (last-writer-wins, rolled-back-excluded,
    `speaker_id`-fallback, immutable-base assertions); updated the
    `apply_approved_edit` mock to `assert_called_once_with(ep.id)`; added
    `SupersedeSpeakerEditsTests` (supersedes approved+pending, leaves
    metadata/rolled-back alone, excluded from fold) and speaker-rollback wiring
    tests in `CreatorRollbackTests` (no newer-approved block; bulk replays once
    per episode). Full suite **398 tests green** via
    `./.venv/Scripts/python.exe manage.py test`.
  - **For Window 3:** (a) Phase 5 will add `EpisodeEditSuggestion.points` +
    `NetworkMembership.edits_speakers`/`edits_sequence` and rework the trust math
    that is currently left flat in approve/rollback. (b) The submit endpoint still
    keys mappings on whatever the client sends and does **not yet** validate keys
    against known `speaker_id`s (§5.1/§9) — that's Phase 7. (c) `_to_words_json`
    only emits `speaker_id` when truthy, so a degraded backfill that seeds it from
    a name still round-trips. (d) Re-transcription's supersede runs unconditionally
    on every completion; it's a cheap no-op when there's nothing to supersede.

- **Window 3 — Phase 5 & §8a (trust counters + edit-pipeline cleanup).** Per-speaker
  trust award, `edits_speakers`/`edits_sequence` counters, `EpisodeEditSuggestion.points`,
  and the cross-publish lockdown. Phase 6 (backfill + retroactive `edits_speakers`
  recompute) and Phase 7 (front-end / review surfaces) remain.
  - **Migration `0088`** — one migration adds `EpisodeEditSuggestion.points`,
    `NetworkMembership.edits_speakers`, and `NetworkMembership.edits_sequence`
    (all `IntegerField(default=0)`).
  - **Reusable helper (§3.4)** — new `speaker_edit_points(edit_mappings, prior_mapping)
    -> (points, newly_named)` in [transcription.py](../pod_manager/services/transcription.py)
    beside `fold_speaker_mappings`. `points` = the number of distinct
    `(prior name → new name)` changes: a rename cascading to many ids scores once, a
    split (one current name → several new names) scores per distinct target, and
    first-time namings fall out for free (each raw `SPEAKER_XX` is a distinct prior).
    The caller owns the fold ordering and passes the prior cumulative mapping, so the
    Phase 6 backfill recompute reuses it unchanged.
  - **Approve handler** ([actions.py](../pod_manager/views/creator/actions.py)) —
    the speaker block now scores via the helper against `fold_speaker_mappings(ep.id)`
    (the APPROVED chain *before* this still-PENDING edit) into a **separate**
    `speaker_points`, kept out of the metadata perfect-sweep (`points == 3`) bonus.
    The zero-approval trap now treats a speaker approval as a real action
    (`points == 0 and not apply_speaker`) so a no-op rename isn't converted to a
    penalising rejection. `total_points = points + speaker_points` is banked on
    `edit.points`, added to `trust_score`, and `speaker_points` increments
    `edits_speakers`. The flat `+1` for speaker approval is gone.
  - **Trusted submit path** ([main.py](../pod_manager/views/creator/main.py)
    `submit_speaker_labels`) — replaced the flat `+5` with the same helper, using the
    `.words` header cache (`existing_mappings`) as the prior fold; banks `points`,
    awards `trust_score` + `edits_speakers`.
  - **Rollback** — single speaker rollback subtracts the **exact** `edit.points` from
    both `trust_score` and `edits_speakers` (wash); metadata rollback keeps the `−5`
    path and now also decrements `edits_sequence` by the count of approved
    season/episode/type fields present in `suggested_data`. Bulk rollback zeroes
    `edits_speakers` + `edits_sequence` alongside the other counters. Reject is the
    unchanged `−2`; supersede touches no trust.
  - **Sequence counter + field restore (§8a)** — the season/episode/type approve
    blocks each `membership.edits_sequence += 1` when their field changes (grouped
    "iTunes / Sequence Metadata"). Rollback now reverts **both** halves: a new
    `_restore_sequence_fields(ep, original_data)` helper restores
    `season_number / episode_number / episode_type` onto the episode (key-presence
    guarded) in the single **and** bulk handlers, and `edits_sequence` is decremented
    on the membership. To make the restore possible, `pre_approval_snapshot` (the
    approve handler's `original_data`) now also captures those three fields — they
    were previously dropped, so the pre-Phase-5 rollback left edited sequence values
    baked into the episode.
  - **Cross-publish lockdown (§8a, owner/admin only)** — removed the `editCrossPublish`
    section + its touched-listener + payload key from the community suggestion form
    in [episode_detail.html](../pod_manager/templates/pod_manager/episode_detail.html)
    (owner `ownerCrossPublishForm` untouched); `submit_episode_edit` now **drops**
    `cross_publish_podcast_ids` server-side unless `_require_owner` passes
    (authoritative against crafted POSTs); `_require_owner`
    ([publish.py](../pod_manager/views/creator/publish.py)) now also allows
    `is_staff` (superuser already allowed). No contribution counter for cross-publish.
    The in-flight `apply_approved_edit` / `snapshot_episode` / rollback cross-publish
    handling is kept so already-submitted rows still resolve.
  - **Profile** ([profile_content.html](../pod_manager/templates/pod_manager/profile_tabs/profile_content.html))
    — added "Voices Named" (`edits_speakers`) and "Episodes Sequenced"
    (`edits_sequence`) counter rows and a "Diarization Path" speaker skill-tree
    (tiers 1/10/50/100/500) reusing the existing `skill-node` / `badge-icon-sm`
    classes (radii come from the shared `--vecto-radius-*` tokens; nothing hardcoded).
  - **Deviations:** none from the brief. `edits_speakers`/`edits_sequence` are awarded
    only at approval (not retroactively) — the §6 backfill recompute will set
    `edits_speakers` from the historical chain.
  - **Tests:** added `SpeakerEditPointsTests` (helper math: naming / correction /
    mixed / no-op / cascade / split / merge), approve+rollback speaker-trust cases, a
    sequence-counter approve case, a sequence rollback that asserts **both** the
    restored episode field values and the decremented counter, and an end-to-end
    approve→rollback round-trip of the sequence values — all in
    `CreatorInboxActionTests` — plus a non-owner cross-publish-drop test in
    `CrossPublishSuggestionTests` (existing two community-submission tests repointed
    to an owner). Full suite **410 tests green** via
    `./.venv/Scripts/python.exe manage.py test`.

- **Window 4 — Phase 6 (§6) + rollback-completeness audit.** The one-time
  `speaker_id` backfill command, the retroactive `edits_speakers` recompute, and a
  verification pass that every edit-suggestion field reverses on rollback (value
  **and** counter). Phase 7 (front-end / review surfaces) and Phase 9 (docs fold)
  remain.
  - **Backfill command (§6)** — new
    [`backfill_speaker_ids`](../pod_manager/management/commands/backfill_speaker_ids.py)
    (preview unless `--apply`; scope `--all` / `--network` / `--podcast` /
    `--episode` / `--limit`). Per completed transcript: reads the current `.words`
    (local for the majority, R2 for `version>=1`), **skips schema `1.1.0`**
    (idempotent re-runs), then:
    - **raw present** (`{id}.whisper_raw.txt`) → re-parse via
      `_parse_whisper_response` to recover pristine `SPEAKER_XX` as write-once
      `speaker_id`, fold the episode's APPROVED edits (`fold_speaker_mappings`) to
      set the resolved `speaker`, **preserve the existing `.words` header verbatim**
      (transcribed_at / model / language / recovery anchors — only `speaker_id` is
      added, plus the refreshed `speaker_mappings` cache), render all 5 formats.
    - **raw missing** → degraded fallback: seed `speaker_id` from the current
      resolved `speaker` (preserved as-is; no fold — its name keys wouldn't match).
    - **Write routing (local-first):** R2-resident (`R2_MEDIA_ENABLED` and
      `version>=1`) → Phase 2 idempotent `write_transcript_formats` (hash-check, PUT
      only changed, bump version only on real change). Everything else → write the
      `MEDIA_ROOT` files in place (free) with **no R2 I/O and no version bump**
      (a `version>=1` would misroute reads to a not-yet-existing R2 object);
      `backfill_transcripts_to_r2` pushes those later.
    - **Name-change logging:** the fold excludes `ROLLED_BACK` edits, so a file
      still baking in a since-reverted edit reconciles here — every episode whose
      distinct resolved-name set moves is logged `before -> after`.
  - **Retroactive `edits_speakers` recompute (§6)** — part of the same command
    (`--apply`; `--skip-recompute` to opt out). For each `NetworkMembership` (scoped
    by `--network` / `--podcast` when given) it replays that user's APPROVED
    (non-SUPERSEDED, non-ROLLED_BACK — distinct statuses) speaker edits **per
    episode in `resolved_at` order**, folding the user's own deltas as it goes and
    summing `speaker_edit_points` against the prior fold, then **SETs**
    `edits_speakers` to that total (idempotent; independent of the per-episode
    `1.1.0` skip). Trust score is **not** re-credited. Logs per-membership
    `before -> after`. Registered in the admin-console
    [registry](../pod_manager/admin_console/registry.py) under "Transcription".
  - **Rollback-completeness audit + fixes.** Verified every `suggested_data` key
    reverses on single **and** bulk rollback, in both value and `NetworkMembership`
    counter. Found and fixed:
    1. **`chapters_private` not restored** — approve mirrors new chapters onto
       **both** `chapters_public` + `chapters_private`
       ([edits.py](../pod_manager/services/edits.py) `apply_approved_edit`;
       [actions.py](../pod_manager/views/creator/actions.py) approve handler), but
       rollback restored only `chapters_public`, leaving the edited chapters baked
       into the private feed (same class of bug §8a fixed for the sequence fields).
       Both single + bulk rollback now restore **both** columns.
    2. **`edits_chapters` decrement used the wrong count** — it subtracted
       `len(suggested_data['chapters'])`, which on a v1.2 chapters **dict** counts
       its keys (`version`/`chapters`/…), not the chapters. Now uses
       `len(chapter_items(...))` to mirror the approval credit.
    3. **Trusted auto-apply never credited `edits_sequence`** —
       `update_contribution_stats` incremented title/tags/chapters/descriptions but
       not the §8a sequence counter, so a trusted season/episode/type edit banked
       `0` while rollback decremented it (clamped at 0, silently asymmetric). It now
       credits one per present sequence field, matching the inbox approve handler and
       the rollback decrement.
  - **Exact per-counter wash via `EpisodeEditSuggestion.counter_deltas`** (migration
    `0089`). A new `JSONField(default=dict)` banks the exact membership-counter
    deltas an approval credited, e.g. `{"edits_tags": 3, "edits_chapters": 2}`. All
    three approval sites populate it — the inbox approve handler
    ([actions.py](../pod_manager/views/creator/actions.py)), the trusted auto-apply
    (`update_contribution_stats` now **returns** the delta map, banked by
    `submit_episode_edit`), and `submit_speaker_labels` (`{"edits_speakers": N}`).
    Single rollback replaced its per-field decrement block with **one generic loop**
    — `for attr, amt in edit.counter_deltas: membership.attr -= amt` (clamped at 0)
    — so `edits_tags` (and every counter) is now an exact wash, killing the old flat
    `−1` tags drift and the `len(dict)` chapters miscount. **Trust** still reverses
    via the flat `−5` (metadata) / banked `points` (speaker); **`first_responder_count`**
    still reverses from `edit.is_first_responder` (both legacy-safe). The earlier
    per-field fixes (chapters counter, trusted `edits_sequence`) are subsumed by this.
  - **Legacy + forward-compat (decided with the user):** pre-feature rows have an
    empty `counter_deltas` (the `AddField` default), so rollback **skips** their
    counter decrement (trust still reverses) — no zero-backfill data migration. And
    because the loop iterates only the keys actually banked, a **counter added in the
    future** is reversed generically, with older edits that lack the key left
    untouched (no `KeyError`, no over-decrement). Bulk rollback still zeroes every
    counter (griefer nuke), so it's independent of the map.
  - **Deviations:**
    1. The §6 retroactive recompute folds **each user's own** approved deltas per
       episode (per the brief's wording), which can differ slightly from the live
       award's prior fold (which folds *all* users' approved edits as the prior). On
       single-contributor episodes — the common case — they coincide; the recompute
       is a deliberate idempotent **set**, not a reproduction of historical awards.
    2. Backfill is **lossless only where the raw dump lives** (the Unraid
       transcription box). Run `--apply` there so the catalogue is recovered from
       `whisper_raw.txt`; running it elsewhere (e.g. a dev box that has the `.words`
       in R2 but no raw dumps) falls back to seeding from resolved names and
       **degrades** the base (split == combined). The dev preview shows this:
       7 transcripts "seeded (fallback)" because the raw dumps are on Unraid.
  - **Tests:** `BackfillSpeakerIdsTests` (1.1.0 skip; whisper_raw id-recovery +
    fold; header preservation; ROLLED_BACK reconciliation + name-change log;
    fallback seeding; preview writes nothing; recompute set + idempotent + trust
    untouched), two `UpdateContributionStatsTests` sequence-credit cases, and a
    `CreatorInboxActionTests` chapters rollback case (both columns + inner-count
    decrement), plus the `counter_deltas` cases (tags exact wash, legacy-row skip,
    speaker counter_deltas banked, sequence/chapters rollback via the banked map).
    Full suite **423 tests green** via `./.venv/Scripts/python.exe manage.py test`.

- **Window 5 — Unified trust model (exact award + exact reversal).** Replaced the
  two-scale trust system (inbox per-section + sweep-3 vs. trusted flat `+5`) with one
  scorer used by every approval path, and made single **and** bulk rollback reverse
  the exact banked amounts.
  - **`score_contribution(changes, is_first)`** ([edits.py](../pod_manager/services/edits.py))
    is the single source of truth → `(points, counter_deltas)`. Per applied field:
    title/description **+1pt/+1ctr**; tags **+1pt / +N ctr** (N = tags added; a pure
    removal scores the point, adds 0 counter); chapters **+N pt / +N ctr** (N =
    chapter count); season/episode#/type **+1pt/+1ctr each** (`edits_sequence`);
    speaker **+N/+N**; first-responder **+1pt/+1ctr**. **Cross-publish is never
    scored** (owner/admin-only). Multi-field **bonus** (trust only, no counter):
    **+2** if ≥3 of fields 1–7 applied, **+4** if all of 1–7 — banked into `points`
    so rollback removes it exactly.
  - **`points` = trust, `counter_deltas` = counters** (incl. `first_responder_count`),
    both banked on the edit at approval. **Auto-approve now scores identically to
    manual** — `update_contribution_stats` builds the same `changes` (value-compared
    to the original) and calls the shared scorer; the flat `+5` is gone. The inbox
    approve handler builds `changes` as it applies sections and calls the same scorer.
  - **Rollback is a pure exact wash.** Single + bulk both call `_reverse_award`:
    `trust -= edit.points`, then `counter -= amt` for each `counter_deltas` entry
    (clamped at 0). No more flat `−5`, no more blanket zeroing — **bulk rollback now
    just runs the exact per-edit reversal in a loop** (value restore via the shared
    `_restore_metadata_values`, speaker episodes replayed once). Reject stays `−2`.
  - **Legacy rows** (pre-`counter_deltas`, `points` 0) reverse nothing by design;
    a counter added in the future is reversed generically (the loop only touches keys
    that were banked). Speaker rollback continues to replay from the immutable base.
  - **Tests:** rewrote `UpdateContributionStatsTests` to the new scorer (no flat +5,
    added-only tag counter, sweep bonus), updated the trusted-path/cross-publish/
    rollback expectations, and `CreatorRollbackTests` bulk now asserts exact reversal
    (not zeroing). Full suite **424 green** via `./.venv/Scripts/python.exe manage.py test`.

- **Window 6 — Full rollback closure (legacy backfill, metadata unlock, chapters).**
  Three follow-ups so *every* user-submittable edit is fully reversible.
  - **Legacy points backfill** — new
    [`backfill_edit_points`](../pod_manager/management/commands/backfill_edit_points.py)
    (preview unless `--apply`; `--all`/`--network`/`--podcast`) reconstructs and
    banks `points` + `counter_deltas` onto historical APPROVED edits that predate
    the unified trust model (they had `points 0` / empty map, so rollback reversed
    no trust/counters). Metadata edits → `metadata_changes()` + `score_contribution()`;
    speaker edits → per-episode `resolved_at` fold scored with `speaker_edit_points`.
    Idempotent SET; does NOT touch `NetworkMembership` aggregates (historical totals
    stand — re-crediting would double-count), only per-edit deltas so future
    rollbacks are exact. Registered under "Maintenance".
  - **Metadata unlock on rollback** — `_handle_approve_edit` sets
    `Episode.is_metadata_locked` but rollback never cleared it. New
    `_maybe_unlock_metadata(ep)` clears the lock when an episode has no APPROVED
    edits left, called from single + bulk rollback (bulk captures the affected
    episodes before statuses flip, since the APPROVED queryset re-evaluates empty).
  - **Chapters effective-snapshot fix** — approve overwrites BOTH `chapters_public`
    and `chapters_private`, but the snapshot captured only `chapters_public` (which
    is typically blank — the public feed falls back to `chapters_private`
    [feeds.py](../pod_manager/views/feeds.py), and the editor edits the private set
    [listener/main.py](../pod_manager/views/listener/main.py)). So approve→rollback
    on a typical episode wiped both columns to blank, losing the real (private)
    chapters. `snapshot_episode` + the approve handler now capture the **effective**
    chapters (`chapters_private or chapters_public`); rollback restores both columns
    to it, so served output matches the pre-edit state. (Column-level public/private
    distinction isn't separately preserved — public is best-effort and approve
    already collapses it; flag if exact column fidelity is wanted.)
  - **Tests:** `BackfillEditPointsTests` (metadata award, speaker chain fold,
    preview no-op, idempotent re-run), plus `CreatorInboxActionTests` cases for
    metadata unlock (and stays-locked when another approved edit remains) and the
    private-chapters effective-snapshot restore. Full suite **431 green** via
    `./.venv/Scripts/python.exe manage.py test`.

- **Window 7 — Phases 7 & 9 (§5, §8b, §3.1/§3.5 front-end, §9).** Front-end
  speaker_id contract + combined↔split toggle, submit key validation, the structured
  review-surface diff, and the docs fold. No model migration (read/display-only).
  - **Listener context** ([views/listener/main.py](../pod_manager/views/listener/main.py)
    `episode_detail`) — now exposes, per `speaker_id`, the **current resolved name**
    derived from `fold_speaker_mappings(ep.id)` over the `.words` `speaker_id` set
    (not from distinct `seg.speaker`), as `transcript_speaker_data` (`[{id, name}]`,
    timeline order) serialised to `transcript_speaker_data_json`. `transcript_speakers`
    is now the distinct `speaker_id` set (used only as the "any speakers?" guard) and
    `transcript_speaker_names` is the fold mapping (authoritative). Pre-backfill
    `.words` fall back to `seg.speaker` (`seg.get('speaker_id') or seg.get('speaker')`),
    so split degrades to combined there.
  - **Speaker Labels form** ([episode_detail.html](../pod_manager/templates/pod_manager/episode_detail.html))
    — boxes are now **JS-rendered** from `transcript_speaker_data_json` so the toggle
    can rebuild them without a reload. Each box **displays the resolved name** and
    **carries the immutable `speaker_id`(s)** in `data-speakers` (comma-joined); submit
    fans the typed name across every id the box carries, always keyed on `speaker_id`.
    A **"Show individual diarized speakers"** switch (persisted in `localStorage` key
    `vecto_speaker_split`, default combined, mirroring the episode-type chips) drives
    both the form (combined = one box per distinct resolved name with fan-out; split =
    one box per `speaker_id`, the un-merge path) and the transcript. `buildEnhancedTranscript`
    grouping/colour key now keys on `seg.speaker` (combined) or `seg.speaker_id` (split,
    with a `.speaker-id-badge` showing the trailing diarization index, radius from
    `--vecto-radius-sm`); the cached doc re-renders in place on a `tx:splitchange`
    event the form dispatches.
  - **Key validation (§5.1)** ([views/creator/main.py](../pod_manager/views/creator/main.py)
    `submit_speaker_labels`) — builds the known `speaker_id` set from the `.words`
    (segment **and** word level, with the `seg.speaker` fallback) and **rejects** a
    submission containing any unknown key (HTTP 400) **before** the existing
    `speaker_edit_points` / `points` / `counter_deltas` banking, so scoring reflects
    only valid keys. Enforced only when a non-empty known set is readable (an
    unreadable `.words` can't validate and shouldn't lock out the feature).
  - **Review surfaces (§8b)** ([views/creator/data.py](../pod_manager/views/creator/data.py))
    — `_annotate_edit_changes` now sets `edit.speaker_changed` + a structured
    `edit.speaker_diff = [(speaker_id, before, after)]` (before from
    `original_data.speaker_mappings`, defaulting an unmentioned id to its raw label);
    `speaker_changed` (not a bare `bool(speaker_mappings)`) now drives `has_changes`,
    so a no-op rename reads as no change. `gather_inbox` **overrides** the diff with
    the **current** `fold_speaker_mappings(ep.id)` as the before-state (a pending
    edit's live name may differ from its submit-time snapshot). The inbox
    ([tab_inbox.html](../pod_manager/templates/pod_manager/creator_tabs/tab_inbox.html))
    and audit log ([tab_audit_log.html](../pod_manager/templates/pod_manager/creator_tabs/tab_audit_log.html))
    both render `speaker_id` (before →) after from `edit.speaker_diff`, suppressing a
    redundant before when it equals the raw id; the audit log's old separate
    "Previous:" line is folded into the inline diff. `original_data.speaker_mappings`
    is still kept as the audit before-state.
  - **Docs (§9)** ([transcription.md](transcription.md)) — rewrote the Speaker
    Identification section: corrected the obsolete "one-click reversal via
    `original_data`" claim, and documented the immutable `speaker_id` vs mutable
    `speaker`, replay-from-base on approve/rollback, supersede on re-transcription, the
    `original_data` audit-only role, and the combined/split view.
  - **Deviations:** none from the brief. The combined↔split toggle lives in the
    (authenticated) Speaker Labels form; the transcript IIFE reads the same
    `localStorage` pref at init, so a returning user's preference applies on load.
  - **Tests:** `EpisodeDetailSpeakerContextTests` (resolved name from the fold not
    `seg.speaker`; pre-backfill fallback), `SpeakerDiffAnnotationTests` (correction /
    first-time-naming / no-op / `gather_inbox` current-fold override), and
    `SubmitSpeakerLabelsValidationTests` (unknown rejected, known accepted, mixed
    rejected atomically). Full suite **440 green** via
    `./.venv/Scripts/python.exe manage.py test`.

## 1. Problem

Speaker name matching (the **Speaker Labels** form on the episode page) currently
mutates transcripts **destructively**:

- `apply_speaker_labels()` ([services/transcription.py](../pod_manager/services/transcription.py))
  reads the *current* `.words` JSON, replaces `segment.speaker` / `word.speaker`
  in place by key lookup, regenerates all five formats, and bumps
  `Transcript.version`. The R2 key is version-independent
  (`transcripts/{id//1000}/{id}.{ext}`) and is **overwritten in place**.
- Submissions are keyed on whatever name is *currently displayed*, so once
  `SPEAKER_00 → Aron` is applied, the label `SPEAKER_00` no longer exists in the
  stored data.

Consequences:

1. **Rollback is documented but not implemented.** `_handle_rollback_single_edit`
   and `_handle_bulk_rollback` ([views/creator/actions.py](../pod_manager/views/creator/actions.py))
   only restore `title/description/tags/chapters/cross_publish` — they never
   touch `speaker_mappings`. The `original_data={'speaker_mappings': …}` captured
   at submit time is dead weight. A speaker-label rollback today resets the trust
   score but leaves the griefed names baked into the files.
2. **Griefing is unrecoverable.** A trusted (or compromised) account that maps
   every speaker to one name collapses the diarization irreversibly — the
   distinct `SPEAKER_XX` labels are gone, so even a hypothetical rollback can't
   reconstruct them.

## 2. Goals / Non-goals

**Goals**
- Make speaker-label edits fully reversible (single + bulk rollback).
- Survive griefing — recover original distinct speakers even after a malicious
  "collapse everything to one name."
- Preserve the existing **merge** UX (two diarized speakers mapped to one name
  render as one grouped block).
- Add an **un-merge / split** capability the current model can't offer.
- Keep podcast feeds and external clients **completely unaffected**.
- Minimise R2 Class A (write) operations.

**Non-goals**
- No change to metadata-edit rollback (title/desc/tags/chapters) — that keeps its
  existing `original_data` snapshot mechanism.
- No change to the trust-score / auto-approve gating model.
- No on-the-fly / per-request rendering. All formats stay pre-materialised on R2.

## 3. Core design

### 3.1 Immutable `speaker_id`, mutable `speaker`

The `.words` JSON gains a **write-once** `speaker_id` field alongside the existing
mutable `speaker`:

| field        | meaning                              | mutated by edits? |
|--------------|--------------------------------------|-------------------|
| `speaker_id` | pristine diarization label (`SPEAKER_03`) | **never** |
| `speaker`    | current resolved display name (`Aron`)    | every apply/rollback |

`speaker_id` is written once at transcription time (segment **and** word level)
and is **never** rewritten. It is the immutable base. There is **no separate
`.words.base` / `.words.orig` file** — the anchor lives inline as a column we
promise never to mutate.

> `.words` is **not** served in podcast feeds. The `<podcast:transcript>` tags
> emit only `vtt / json / srt / html` ([views/feeds.py](../pod_manager/views/feeds.py)).
> `.words` is fetched only by our own episode page for word-level highlighting.
> Therefore `speaker_id` is invisible to every external client, and the feed
> formats are **not** changed (they keep `speaker` = resolved name only).

Bump the `.words` schema version `1.0.0 → 1.1.0` to signal the new field.

### 3.2 The edit chain is the source of truth

Current applied state is a pure function of:

```
state = fold(approved speaker edits, ordered by resolved_at) over speaker_id base
```

Each `EpisodeEditSuggestion` with `suggested_data = {'speaker_mappings': {...}}`
is one delta in the chain. The cumulative mapping is a last-writer-wins fold over
the `APPROVED` (non-superseded) speaker edits for the episode, ordered by
`resolved_at`. Submissions are **deltas** keyed on `speaker_id`; unmentioned keys
keep their prior value.

### 3.3 Apply / replay (single function)

Replace the mappings-argument signature with a **recompute-from-base** function:

```text
apply_speaker_labels(episode_id):
    1. load .words; take speaker_id per segment/word as the base
       (ignore the current mutable `speaker`)
    2. mapping = fold(APPROVED speaker edits for episode, by resolved_at)   # last-writer-wins per speaker_id
    3. for each segment/word: speaker = mapping.get(speaker_id, speaker_id)
    4. render vtt / json / srt / html / words   (words now carries speaker_id)
    5. idempotent-write each format to R2 (see §4); store `speaker_mappings`
       header as a cache; bump Transcript.version iff any format changed
```

- **Approve** an edit → set row `APPROVED`, then `apply_speaker_labels(episode_id)`.
- **Rollback** an edit → set row `ROLLED_BACK`, then `apply_speaker_labels(episode_id)`.

Same function rebuilds "current state from base + remaining chain" in every case.

**Call sites to converge.** The new no-arg `apply_speaker_labels(episode_id)`
replaces every current invocation that passed a mappings dict:
`edits.apply_approved_edit` (the `speaker_mappings` branch), the trusted-submit
path in `submit_speaker_labels` ([views/creator/main.py](../pod_manager/views/creator/main.py)),
and the approve handler in `actions.py`. Speaker edits are always **speaker-only**
(created exclusively via `submit_speaker_labels`, never mixed with metadata), so
replay never has to reconcile with the partial-approval/points machinery.

**Concurrency — per-episode lock (required).** Replay is a read-modify-write on
`.words` + a non-atomic `Transcript.version` bump, so two concurrent approvals (or
an approval racing a re-transcription) can interleave and clobber each other.
**Solution:** run the whole replay inside a DB transaction and take
`select_for_update()` on the episode's `Transcript` row at the top. The second
writer blocks, then re-reads fresh state and recomputes; other episodes still run
in parallel (per-row lock). **Re-transcription takes the same lock.** This is the
correctness guarantee.

This concerns the **live submit / approve / rollback path only**. The one-time
backfill (§6) is a standalone batch job with no page-load interaction and no race,
so it needs no lock.

**Execution context (decided): synchronous / immediate.** The live apply runs
synchronously in the request, exactly as today — the page gets the new `version`
back right away and refetches `.words`. The per-episode lock is held only briefly
and is invisible unless two people edit the *same* episode at the same instant.
(No `admin`-queue indirection for the live path.)

### 3.4 Rollback handlers

- **Single rollback** (`_handle_rollback_single_edit`): for a speaker-only edit,
  flip to `ROLLED_BACK` and call `apply_speaker_labels`. **The "newer approved
  edits exist" blocker is not needed for speaker edits** — replay-from-base over
  the remaining chain is deterministic and order-correct regardless of which edit
  is removed. (The blocker stays for snapshot-based metadata edits.)
- **Bulk rollback** (`_handle_bulk_rollback`): flip all the user's speaker edits
  to `ROLLED_BACK`, then `apply_speaker_labels` once per affected episode (dedupe
  episode ids). Trust zeroing unchanged.

`original_data.speaker_mappings` is no longer load-bearing for **restore** (replay
from base handles that), but it is **kept** — it's the audit log's before-state for
the speaker diff (§8b). Don't drop it.

> **⚠ As built (Windows 5–6) — superseded by the unified reward model.** The
> speaker-only scheme below remains accurate for *speaker* scoring
> (`speaker_edit_points`), but trust and counters are now driven by **one** scorer
> (`score_contribution`, [edits.py](../pod_manager/services/edits.py)) across every
> edit type, banking exact `points` (trust) + `counter_deltas` (per-counter) on
> each edit so single **and** bulk rollback are a pure exact wash. §12 and the
> Window 5/6 progress entries are the authoritative description.

**Trust accounting (decided).** Points are awarded **per distinct name change**, and
a rollback is an exact wash. The award for one approved edit is:

```
prior  = cumulative mapping of already-APPROVED edits, before this one
points = # of distinct (prior_value → new_name) pairs in the edit where
         new_name != prior_value
       = len({(prior.get(sid, sid), new_name)
              for sid, new_name in edit if new_name != prior.get(sid, sid)})
```

Group the edited `speaker_id`s by their **current** (prior) name and, within each
group, count the distinct **new** names assigned (drop any id that maps to its own
current name). The lossless `speaker_id` base makes this the natural unit: an id's
prior value is either a real name or — if still unnamed — its own raw `SPEAKER_XX`
label (unique per id), so naming and correcting fall under one rule.

- **First-time identification** → `+1` per speaker (each raw label is a distinct
  prior value). Naming `SPEAKER_00 = Aron` and `SPEAKER_03 = Aron` in one edit is
  **+2**.
- **Rename that cascades to several ids** (one current name → one new name across
  many ids) → **+1**, regardless of how many `speaker_id`s share that name. Flipping
  the *combined* view `Aron ⇆ Jim` is two changes → **+2**.
- **Split** (one current name un-collapsed into several new names — what the old
  in-place rename could not do) → **+1 per distinct target.** Toggling to the
  original ids and mapping `Aron(01)→Jim, Aron(02)→Roy, Jim(03)→Aron, Jim(00)→Dan`
  is four distinct changes → **+4**.
- **Merge** (two different current names sent to the same new name, e.g. two diarized
  speakers declared to be the same person) → **+1 per source name** (each name
  changed) → typically **+2**.
- **Single rollback:** subtract the **exact `points` recorded on the edit** → wash.
- **Reject** (creator rejects a still-pending, never-applied edit): existing flat
  `−2`, unchanged.
- **Bulk rollback** (griefer): existing behaviour — trust zeroed for the network.
- **Supersede** (re-transcription, §7): **no** trust change — superseding isn't the
  user's fault, so earned credit stays.

Record `points` on the `EpisodeEditSuggestion` at approval so rollback reverses the
exact amount without recomputation.

**Expose this as a reusable helper** — e.g. `speaker_edit_points(edit_mappings,
prior_mapping) -> (points, newly_named)` — used by both the live approve handler
**and** the §6 backfill recompute. It needs the `prior_mapping` (the cumulative
fold *before* the edit) to resolve each id's current name and count distinct
changes (`newly_named` is the subset that were first-time identifications), so
callers pass it in.

**Counter on `NetworkMembership`.** Add an `edits_speakers` IntegerField alongside
the existing `edits_title / edits_chapters / edits_tags / edits_descriptions`
([models.py](../pod_manager/models.py)), incremented/decremented by the same
`points` (mirrors how `update_contribution_stats` in
[services/edits.py](../pod_manager/services/edits.py) and the rollback handlers
adjust the other counters). Surface it on the profile page
([profile_tabs/profile_content.html](../pod_manager/templates/pod_manager/profile_tabs/profile_content.html)):
a counter row ("Voices Named") and a skill-tree badge branch with tiers
(1 / 10 / 50 / 100 / 500) consistent with the existing Chapters/Tags/Descriptions
trees.

### 3.5 Transcription pipeline (new transcriptions)

**This is the foundational change** — the live transcription path must populate
`speaker_id` from the start so every new transcript is born with the immutable
base and never needs backfill. In `run_transcription` →
`_parse_whisper_response` → `_to_*`
([services/transcription.py](../pod_manager/services/transcription.py)):

- `_parse_whisper_response` already yields segments/words carrying the diarization
  label in `speaker`. Carry that raw label into a **new `speaker_id`** field on
  each segment and word (set once, here).
- At initial transcription there are no mappings, so
  `speaker == speaker_id == SPEAKER_XX`. The first speaker-label edit only ever
  rewrites `speaker`; `speaker_id` is never touched again.
- `_to_words_json` emits **both** `speaker_id` and `speaker` (segment + word
  level). `_to_vtt` / `_to_srt` / `_to_podcast_index_json` / `_to_html` are
  **unchanged** — they keep emitting the resolved `speaker` only, so `speaker_id`
  never reaches the feed formats.
- Bump the `.words` schema version to `1.1.0`.

No change to ASR invocation, diarization parameters, source validation, or the
file-write flow beyond the new field. Because new transcripts carry `speaker_id`
natively, the §6 backfill is a **one-time** migration for the existing catalogue
only.

## 4. Idempotent R2 writes (Class A minimisation)

Regeneration re-renders all five formats, but many writes are no-ops. We **never
assume** which formats changed — we hash-check **all five** unconditionally and
write only those that actually differ:

- On **backfill**, `.words` always changes (it gains `speaker_id`). `vtt/json/srt/html`
  are *expected* to match (they don't carry `speaker_id` and the resolved names
  are unchanged) — but a mutation could legitimately shift them, so they are
  verified per-format, not presumed unchanged.
- On a **rollback to a prior state** or a **no-op edit**, some/all formats are
  identical — again, determined by hashing, not assumption.

So before each PUT, compare hashes and skip only objects that genuinely match:

```text
for ext, new_bytes in rendered:
    existing_etag = head_object(key).ETag        # Class B (cheap, no body)
    if existing_etag == md5_hex(new_bytes):
        skip                                     # no Class A write
    else:
        put_media_object(key, new_bytes)         # Class A
```

- R2 ETag for a single (non-multipart) PUT is the MD5 hex of the body. Our
  transcript objects are small single PUTs, so `ETag == md5(content)` holds.
  Extend the existing `media_object_exists` HEAD helper
  ([r2_storage.py](../pod_manager/services/r2_storage.py)) with a
  `media_object_etag(key)` returning the unquoted ETag (or `None` on 404 → must PUT).
- Fallback if an ETag is ever not a plain MD5 (e.g. multipart): GET the object
  (edge-cached, Class B) and hash the body.
- **Economics:** a HEAD is **Class B**; a PUT is **Class A**. R2's free tier gives
  ~10× more Class B than Class A, so trading a skipped write for a HEAD is the
  right call: across the catalog a typical backfill writes just `.words` and
  HEAD-skips the rest — but every format is hash-verified, so any that did shift
  is rewritten. No format is assumed unchanged.
- **Version bump:** bump `Transcript.version` only when ≥1 format actually
  changed. A `.words`-only change (backfill) still bumps the shared version so the
  page re-fetches `.words`; the unchanged feed formats get a harmless edge
  revalidation at the new `?v=N`. (A future enhancement could store per-format
  hashes on `Transcript` to skip the HEADs entirely and/or decouple versions.)
- **Local (R2-disabled) path:** dev without R2 writes to `MEDIA_ROOT` via
  `write_transcript_file`. There's no ETag there — either hash the local file
  bytes or just always write (local writes are free). The hash-check is an
  R2-only optimisation.
- **Partial-write safety:** render all formats and confirm all writes succeed
  **before** bumping `version`, so a mid-write R2 failure can't leave a
  version-advanced but partially-updated set.

## 5. Front-end (episode page)

Today the form derives its boxes from the distinct `seg.speaker` values
([views/listener/main.py](../pod_manager/views/listener/main.py)) and submits
keyed on the displayed name. Both change.

### 5.1 Form data contract

Each box **displays the current resolved name** but **carries the immutable
`speaker_id`** as its hidden submit key:

- box value (shown / editable) = current `speaker` (`"Aron"`)
- carries the immutable id(s) in `data-speakers` (comma-joined — a combined-view
  box fans its name across the whole group, a split-view box carries one id)
- submit payload keyed on `speaker_id`, never on display name —
  `{ "SPEAKER_00": "Aron", "SPEAKER_03": "Aron" }` for a combined "Aron" box.

Server (`submit_speaker_labels`) validates that submitted keys are **known
`speaker_id`s** for that episode (reject unknown keys) — also tightens grief
resistance.

**Server-side context must change too.** The page context built in
[views/listener/main.py](../pod_manager/views/listener/main.py) currently exposes
`transcript_speakers` (distinct `seg.speaker`) and `transcript_speaker_names`
(the header map). The new form needs, per `speaker_id`: its current resolved name
and its combined-group membership. Build that context from the `.words`
`speaker_id` set + current `speaker` per id (and the header map as a convenience).

### 5.2 Combined vs split toggle

One **"Show individual diarized speakers"** switch drives both the form and the
transcript display (default = combined; persist per-user in `localStorage` like
the episode-type chips):

| view | form boxes | transcript grouping ([episode_detail.html](../pod_manager/templates/pod_manager/episode_detail.html) `buildEnhancedTranscript`) | colour key |
|------|-----------|-------------------------------|-----------|
| **Combined** (default) | one per **distinct current name** (2) | group consecutive segments by `seg.speaker` (resolved name) → merged `ARON` block | `seg.speaker` → one colour per name |
| **Split** | one per **`speaker_id`** (4) | group by `seg.speaker_id` → `ARON·0` / `ARON·3` as separate runs | `seg.speaker_id` → distinct shade + badge |

```
            ┌─ default (combined) ──────┐   ┌─ "Show diarized speakers" ─┐
display     │ ▍ARON   (00+03, 1 colour) │   │ ▍ARON·0   (SPEAKER_00)     │
            │ ▍JIM    (01+02, 1 colour) │   │ ▍ARON·3   (SPEAKER_03)     │
            └───────────────────────────┘   │ ▍JIM·1 / ▍JIM·2            │
form        [Aron] [Jim]   ← 2, grouped      [Aron][Aron][Jim][Jim] ← 4, per-id
submit      fan-out to ids in group          one SPEAKER_XX each
```

- **Combined edit** fans the typed name across every `speaker_id` in the group:
  renaming combined `Aron` → `A.Ron` submits `{SPEAKER_00:"A.Ron", SPEAKER_03:"A.Ron"}`.
- **Split edit** targets one `speaker_id`: this is how a user **un-merges** —
  setting `SPEAKER_03` to a different name than `SPEAKER_00`. The split view is
  also the answer to "which paragraphs were `SPEAKER_03`?" — the merged `ARON`
  block visually breaks apart by `speaker_id` so the user can see and target it.

The merge UX is preserved verbatim: the served `.words` still carries resolved
names in `seg.speaker`, so combined grouping/colouring is byte-for-byte what it is
today; `speaker_id` only *adds* the split capability.

## 6. Backfill (existing transcripts)

Lossless reconstruction from the raw ASR dump — **no re-transcription**.

`{episode_id}.whisper_raw.txt` is saved next to the transcript files (on the
container's mounted `MEDIA_ROOT`) whenever `WHISPER_KEEP_SOURCE_AUDIO=True`
([transcription.py](../pod_manager/services/transcription.py)). The **Unraid
Docker deployment** — the primary environment where transcription runs — has this
enabled, so the raw dumps exist on that storage for virtually all transcripts. It
is the raw `asr.text`, re-parseable via `_parse_whisper_response` into pristine
segments with word-level `SPEAKER_XX`.

> **Local-first, mixed storage.** Most transcripts are **not yet in R2** (some
> are). Backfill writes each transcript back to **wherever it currently lives**:
> - **Local-only** (the majority): rewrite the local `MEDIA_ROOT` files in place —
>   free disk writes, **no R2 I/O**. The existing `backfill_transcripts_to_r2`
>   command later pushes the already-correct files up (one PUT each). Local-first
>   avoids "push then rewrite" and saves a large number of Class A operations.
> - **Already in R2**: update R2 in place with the §4 hash-check (typically only
>   `.words` changes, so usually a single PUT).
>
> Either way `whisper_raw.txt` is read from local disk.

**Idempotent / resumable (decided).** Check the existing `.words` schema version
first and **skip anything already at `1.1.0`** (i.e. already carrying
`speaker_id`), so re-runs don't redo finished transcripts. The check reads the
`.words` `version` field (free for local; one Class B read for R2). Optionally
record a per-`Transcript` flag once migrated to skip even that read on re-runs.

Backfill management command (preview unless `--apply`), per episode:

```text
for each episode with a completed transcript:
    if existing .words schema >= 1.1.0:               # already migrated
        skip
        continue
    if {id}.whisper_raw.txt exists:
        segments = _parse_whisper_response(raw)        # pristine SPEAKER_XX, word level
        speaker_id = SPEAKER_XX from the raw parse     # write-once
        mapping = fold(APPROVED speaker edits)         # re-apply existing names
        speaker = mapping.get(speaker_id, speaker_id)
        header = preserve existing .words metadata     # transcribed_at, model, language, anchors
        if any name differs from the current file:     # excluded ROLLED_BACK edits
            log {episode_id, before -> after}          # for post-run review
        render 5 formats (words now carries speaker_id)
        write back to wherever the transcript lives:
            local -> plain local writes (free)
            R2    -> §4 hash-check, PUT only changed
    else:                                              # fallback: raw missing
        speaker_id = current seg.speaker               # degraded: split == combined,
                                                       # no un-merge until re-transcribed
# local files are later pushed by backfill_transcripts_to_r2 (separate step)
```

- Where raw exists → fully lossless, distinct `SPEAKER_XX` recovered even for
  already-mapped episodes.
- Where raw is missing → `speaker_id` seeded from the current resolved name (note:
  this seeds `speaker_id` with a *name*, not a `SPEAKER_XX` label, and collapses
  any previously-merged speakers into one id); the episode works and is reversible
  going forward but can't be split until a future re-transcription regenerates a
  true base.

**Gotchas the backfill command must handle:**

- **Preserve original metadata.** Regenerating `.words` must keep the existing
  header's `transcribed_at`, `model`, language, and recovery anchors (read from
  the current `.words` header / `Transcript` row) — do **not** stamp "now" or the
  current model. Only `speaker_id` is added.
- **Runs local-first, before the R2 push** (see the note above) — no R2
  prerequisite; it operates on the not-yet-mirrored local files.
- **It reconciles historically-broken speaker rollbacks (accepted, logged).**
  Because today's speaker "rollback" never un-applied names, a few test episodes'
  files include the effect of edits now marked `ROLLED_BACK`. The chain fold
  **excludes** `ROLLED_BACK` edits, so backfill retroactively completes those
  rollbacks and changes the displayed names there. This is accepted; the command
  **logs every episode whose names change** (`before -> after`) for post-run
  review. Expected to be a handful (test rollbacks only).
- **Idempotent / re-runnable.** Skips transcripts already at schema `1.1.0`, and
  re-parses `whisper_raw.txt` (not the live `.words`) so repeated runs are stable.

### Retroactively credit `edits_speakers`

The `edits_speakers` counter (added in Phase 5) starts at 0 for everyone, but
historical speaker identifications predate it. As part of the conversion,
**recompute** each `NetworkMembership.edits_speakers` from the historical chain:

- For each network membership, replay that user's `APPROVED` (non-`SUPERSEDED`,
  non-`ROLLED_BACK`) speaker edits **per episode, in `resolved_at` order**, folding
  as you go, and sum each edit's `speaker_edit_points(...)` (§3.4 helper) against
  the prior fold.
- **Set** the counter to that sum (don't increment) so the pass is **idempotent**
  and safe to re-run independently of the per-episode `1.1.0` skip.
- **Scope: the counter only.** This sets the `edits_speakers` aggregate; it does
  **not** re-credit `trust_score`. (Per-edit `points` / `counter_deltas` for
  historical rows are reconstructed separately by `backfill_edit_points` — Window 6
  — so rollbacks are exact; the aggregate trust already banked stands, and
  re-crediting it would double-count.)
- Gated behind `--apply` like the rest of the command; logs the per-membership
  before → after.

## 7. Re-transcription

A fresh diarization renumbers speakers (`SPEAKER_00` in v2 ≠ v1), so the old edit
chain no longer lines up. On re-transcription:

1. Write the new `.words` with `speaker_id` from the fresh diarization.
2. Mark all prior `APPROVED` / `PENDING` speaker edits for the episode
   **`SUPERSEDED`** (new status) — retained for audit & trust history, excluded
   from replay.
3. Names reset to raw labels (empty chain); users re-label against the new base.

## 8. Data-model changes

- `EpisodeEditSuggestion.Status`: ensure `ROLLED_BACK` (already used by rollback
  code) and add `SUPERSEDED`. Migration required.
- `NetworkMembership`: add `edits_speakers` and `edits_sequence` IntegerFields
  (default 0). One migration. (No cross-publish counter — §8a locks it to
  owner/admin, out of the contribution system.)
- `EpisodeEditSuggestion`: add `points` (IntegerField) recorded at approval so
  rollback reverses the exact **trust** delta (speaker edits). Also add
  `counter_deltas` (JSONField, migration `0089`) recording the exact
  **per-`NetworkMembership`-counter** deltas the approval credited, so single
  rollback reverses every `edits_*` counter generically and exactly (Window 4).
- `.words` schema: add `speaker_id` (segment + word), bump version to `1.1.0`
  (also the backfill idempotency marker).
- Optional later: per-format content hashes on `Transcript` to skip HEADs; and/or
  a per-`Transcript` "speaker_id backfilled" flag to skip the re-run version read.
- `r2_storage`: add `media_object_etag(key)`.

## 8a. Edit-pipeline cleanup (adjacent scope)

Audit of `submit_episode_edit` + the approve handler
([views/creator/actions.py](../pod_manager/views/creator/actions.py)) found four
**community-submittable** edit types (the form is **not** `is_owner`-gated:
[episode_detail.html](../pod_manager/templates/pod_manager/episode_detail.html))
that **award trust but have no `NetworkMembership` counter and no profile surface**.
Two outcomes: track the sequence edits; **lock cross-publish to owner/admin.**

| Field | Trust on approve | Decision |
|-------|------------------|----------|
| `season_number`  | `+1` | track via `edits_sequence` |
| `episode_number` | `+1` | track via `edits_sequence` |
| `episode_type`   | `+1` | track via `edits_sequence` |
| `cross_publish_podcast_ids` | `+1` | **remove from community path — owner/admin only** |

### Track the sequence edits

- **`edits_sequence`** — one counter for the "iTunes / Sequence Metadata" section
  (`season_number` + `episode_number` + `episode_type`), incremented per approved
  field in that group (matches how the edit form groups them). Decrement on
  rollback. Profile row: e.g. "Episodes Sequenced". (Granularity adjustable —
  could be three counters; grouped recommended.) Skill-tree badges optional (the
  `edits_title` "Titles Scribed" stat is counter-only today, a fine precedent.)
- **Rollback restores the field values, not just the counter.** Pre-existing
  metadata rollback never restored `season_number / episode_number / episode_type`
  (the approve handler's `pre_approval_snapshot` didn't even capture them), so a
  rollback decremented the counter while leaving the edited values baked into the
  episode. Phase 5 fixes this: the snapshot now captures the three fields and the
  single + bulk rollback handlers restore them (key-presence guarded for
  pre-feature edits) **and** decrement `edits_sequence`. Both the episode data and
  the `NetworkMembership` stat are reverted on rollback.

### Lock down cross-publish (owner/admin only)

Deciding which shows an episode appears in is a privileged action — a non-owner
should not be able to suggest placing an episode in another show's feed. Owners
already have a dedicated, owner-gated control (`manage_episode` →
`update_cross_publish`, [publish.py:217-258](../pod_manager/views/creator/publish.py#L217-L258)),
so cross-publish leaves the community-suggestion pipeline entirely:

- **Template** — remove the `editCrossPublish` section from the community
  suggestion form (the separate owner `ownerCrossPublishForm` stays).
- **Client JS** — stop adding `cross_publish_podcast_ids` to the suggestion
  payload.
- **`submit_episode_edit`** — **server-side gate (authoritative):** drop
  `cross_publish_podcast_ids` from the payload unless the user is owner/admin, so a
  crafted POST can't bypass the hidden UI.
- **Owner action covers admin** — extend `_require_owner` (or the
  `update_cross_publish` gate) to also allow `is_staff` / `is_superuser`, so
  "owner **and** admin" both manage cross-publish.
- **No contribution counter** — cross-publish is no longer a community
  contribution, so it earns no trust and gets no profile stat.
- **Back-compat** — keep the existing cross-publish handling in
  `apply_approved_edit` / `snapshot_episode` / the rollback handlers so any
  **in-flight** suggestion rows still resolve; only *new* submissions are blocked.

## 8b. Review surfaces (approval desk + audit log)

Audit of the inbox ([tab_inbox.html](../pod_manager/templates/pod_manager/creator_tabs/tab_inbox.html))
and audit log ([tab_audit_log.html](../pod_manager/templates/pod_manager/creator_tabs/tab_audit_log.html)),
fed by `_annotate_edit_changes` / `gather_inbox`
([views/creator/data.py](../pod_manager/views/creator/data.py)):

| Field | Approval desk | Audit log |
|-------|---------------|-----------|
| title / description / tags / chapters | ✓ approve + diff | ✓ before→after |
| season / episode# / type | ✓ approve (`metadata-scalars`) | ✓ before→after |
| cross-publish | ✓ approve | ✓ before→after (legacy after §8a lockdown) |
| **speaker labels** | ⚠ approve, but **suggested-only** (no before-state) | ✓ suggested + "Previous:" |

Everything is surfaced; the only gap is the **inbox speaker block shows no
current/before name**, so a *correction* (rename of an already-named speaker)
isn't legible. Adds:

- **`_annotate_edit_changes`** — add an `edit.speaker_changed` flag and a structured
  per-speaker diff `[(speaker_id, before_name, after_name)]`, so both surfaces
  render a consistent before→after instead of the templates reading the raw dicts.
- **`gather_inbox`** — expose the **current** resolved name per `speaker_id` (like
  the existing `current_title` / `current_tags`), since for a pending edit the live
  name may differ from the submit-time snapshot. Optionally a `speaker_conflict`
  flag mirroring the other `*_conflict` flags.
- **Inbox template** — render `SPEAKER_03: Aron → Jim` (before→after) in the
  speaker block, matching how the scalar fields show their diff.
- **Audit template** — already shows "Previous:"; align it to the same
  before→after formatting.

> This is why `original_data.speaker_mappings` is **kept**, not dropped (see §3.4):
> it is the audit log's before-state. The sequence fields need no display change —
> grouping them into `edits_sequence` is a counter concern only; their per-field
> diffs stay as-is.

## 9. Feed & gating safety

- Feeds: **no change.** `.words` is not advertised; `vtt/json/srt/html` bytes are
  unchanged by this work (they never carry `speaker_id`).
- Gating: **no change.** Auto-apply still gated on
  `trust_score >= network.auto_approve_trust_threshold`; untrusted submissions
  still queue to the creator inbox. Tightened only by validating submitted keys
  against known `speaker_id`s.

## 10. Implementation phases

1. ~~**Schema & write-once base (§3.5)** — populate `speaker_id` in the live
   transcription pipeline (`_parse_whisper_response` → `_to_words_json`); every
   new transcription is born with the immutable base. Bump words schema version.
   (`media_object_etag` helper.)~~ ✅ **Done (Window 1).**
2. ~~**Idempotent R2 write** (§4) — hash-before-PUT in the format-write path;
   version bump only on change.~~ ✅ **Done (Window 1).**
3. ~~**Replay engine** — rewrite `apply_speaker_labels` to recompute from
   `speaker_id` base + approved chain; wire approve & rollback to call it.
   Add `SUPERSEDED` status + migration.~~ ✅ **Done (Window 2).**
4. ~~**Rollback handlers** — speaker-aware single + bulk rollback; relax the
   newer-approved blocker for speaker-only edits.~~ ✅ **Done (Window 2).**
5. ~~**Trust counters & edit-pipeline cleanup (§8a)** — add `edits_speakers` +
   `edits_sequence` and `EpisodeEditSuggestion.points` (one migration); wire the
   per-speaker distinct-name-change award (§3.4) into
   approve/rollback, and the sequence counter into the existing per-section blocks;
   add counter rows ("Voices Named", "Episodes Sequenced") + speaker skill-tree on
   the profile. The per-speaker award is now **+1 per distinct name change** (rename
   cascade = once, split = per target); see §3.4. **Factor the per-edit
   speaker-points calc into a reusable helper**
   (§3.4) — the Phase 6 backfill recompute reuses it. **Lock cross-publish to
   owner/admin** — remove it from the community suggestion form/payload, server-side
   gate it in `submit_episode_edit`, and allow `is_staff` in the owner action.~~
   ✅ **Done (Window 3).**
6. ~~**Backfill command** (§6) — mixed local/R2 reconstruction with fallback;
   `1.1.0` skip; name-change logging; preview/`--apply`. Plus a **retroactive
   `edits_speakers` recompute** from the historical approved chain (idempotent
   set, using the Phase 5 helper).~~ ✅ **Done (Window 4).**
7. ~~**Front-end & review surfaces** — form carries `speaker_id` / displays name;
   combined↔split toggle for form boxes + transcript grouping + colour key. Add the
   structured speaker diff (§8b): `speaker_changed` + per-speaker before→after in
   `_annotate_edit_changes` / `gather_inbox`, and render before→after in the inbox
   speaker block (audit log already shows "Previous:").~~ ✅ **Done (Window 7).**
8. ~~**Re-transcription** — supersede prior speaker edits.~~ ✅ **Done (Window 2).**
9. ~~**Docs** — fold the implemented behaviour back into
   [transcription.md](transcription.md) (correct the current "one-click
   reversal" claim) and remove the dead `original_data` speaker capture note.~~
   ✅ **Done (Window 7).**

## 11. Testing

- Apply → rollback restores prior names; griefing (collapse all → one name) is
  fully recoverable from base.
- Multi-edit chain: rollback of a middle edit replays correctly (order-correct,
  last-writer-wins per `speaker_id`).
- Merge preserved: two `speaker_id`s mapped to one name render as one combined
  block, one colour.
- Split: per-`speaker_id` rename un-merges; split view groups/colours by
  `speaker_id`.
- Idempotent write: all five formats are hash-checked; only those that differ are
  PUT (typically just `.words` on backfill, but any shifted format is rewritten);
  version bumps only on real change.
- Backfill lossless from `whisper_raw.txt`; fallback path when raw is absent.
- Re-transcription supersedes the prior chain; names reset to raw labels.
- Feed regression: `vtt/json/srt/html` bytes and `<podcast:transcript>` tags
  unchanged (sanity, since formats aren't touched).
- Submit validation rejects unknown `speaker_id` keys.
- Review surfaces (§8b): inbox shows speaker before→after (first-time naming vs
  correction both legible); audit log shows the speaker diff; `speaker_changed`
  drives `has_changes`.
- Cross-publish lockdown (§8a): non-owner POST with `cross_publish_podcast_ids` is
  dropped server-side; owner/admin direct action still works; no `edits_*` counter
  moves for cross-publish.

## 12. Decisions (all settled)

- **Concurrency (§3.3)** — per-episode `select_for_update` lock on `Transcript`;
  re-transcription takes the same lock. Applies to the live path only.
- **Execution context (§3.3)** — live apply/rollback runs **synchronously /
  immediately** in the request (page refetches the new version right away). No
  `admin`-queue indirection.
- **Trust accounting** — *superseded by the unified model (Window 5/6); this line
  is the original speaker-only decision, kept for history.* As built today:
  **one scorer** (`score_contribution`, [edits.py](../pod_manager/services/edits.py))
  drives every approval path (manual inbox **and** auto-approve, identically).
  Per applied field: title/description **+1pt/+1ctr**; tags **+1pt / +N(added) ctr**;
  chapters **+N pt/+N ctr**; season/episode#/type **+1pt/+1ctr each** (`edits_sequence`);
  speaker **+N/+N** (`+1` per distinct `prior name → new name` change — rename
  cascade scores once, split scores per target; §3.4);
  first-responder **+1pt/+1ctr**; cross-publish **never scored**. Multi-field bonus
  (trust only, banked in `points`): **+2** for ≥3 of fields 1–7, **+4** for all of
  1–7. Every edit banks `points` (trust) + `counter_deltas` (the exact counters,
  incl. first_responder) at approval; **rollback — single AND bulk — is a pure exact
  wash** (`trust −= points`, `counter −= counter_deltas[*]`). Reject `−2`; supersede
  leaves trust intact. Legacy pre-banking rows are reconciled by the one-time
  `backfill_edit_points` command. The original speaker decision was `+1` per
  first-time naming / `+1`-flat per correction; it was later refined to `+1` per
  distinct `prior name → new name` change (§3.4) so a *split* (un-collapsing a
  wrongly-merged speaker) is rewarded per target while a rename cascade still scores
  once. Banked on a new `NetworkMembership.edits_speakers` counter, surfaced on the
  profile.
- **Edit-pipeline cleanup (§8a)** — track the currently-untracked
  `season/episode/type` community edits, grouped as `edits_sequence` (decided);
  **lock `cross_publish` to owner/admin** (remove from the community suggestion
  path, server-side gated, no contribution counter).
- **Review surfaces (§8b)** — all fields already appear on the approval desk and
  audit log; add a structured before→after speaker diff (the inbox currently shows
  the suggested mapping only). `original_data.speaker_mappings` is retained as the
  audit before-state.
- **Backfill (§6)** — local-first, writing each transcript back to wherever it
  lives (local or R2); skips schema `1.1.0`; logs name-change reconciliations.
- **Backfill name reconciliation (§6)** — accepted; a handful of test-rollback
  episodes may change names; all changes logged for review.
- **Colour (§5.2)** — driven by the toggle: by name in combined, by `speaker_id`
  in split (first-speaker-by-timeline keeps order stable).

## 13. Future enhancements

- Per-format content hashes stored on `Transcript` to skip HEADs entirely and/or
  decouple per-format versions (avoid edge revalidation of unchanged feed formats
  on a `.words`-only change).
- Optional belt-and-suspenders immutable `.words.base` snapshot (rejected for now;
  inline write-once `speaker_id` is the agreed anchor).
