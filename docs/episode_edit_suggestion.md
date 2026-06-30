# Episode Edit Suggestions — Admin Guide

> **Audience:** site owners and admins running Vecto. This describes **what the
> feature does** — how listeners suggest edits, how contributions are scored, and
> how you approve or reverse them. It is not a code walkthrough.

Speaker-label edits are a related but separate flow with their own mechanics
(immutable diarization base, replay, combined/split view) — see
[transcription.md → Speaker Identification](transcription.md#speaker-identification).
This guide focuses on **metadata** edits and the scoring/approval/rollback model
that speaker edits also plug into.

---

## 1. What an episode edit is

Any signed-in listener can suggest improvements to an episode's metadata from the
episode page → **Edit** tab ("The Archivist Desk"). The editable fields are:

| Field | Notes |
|-------|-------|
| **Title** | Episode title. |
| **Description** | The clean (sanitised) HTML description. |
| **Tags** | Free-form keywords. |
| **Chapters** | Timestamped chapter list (with optional links, images, locations, waypoints). |
| **Season # / Episode # / Episode Type** | iTunes/sequence metadata, grouped together. |
| **Speaker Labels** | Submitted from the **Transcript** tab, not the Edit tab — separate flow. |

**Cross-publish (feed placement) is owner/admin only** and is *not* part of the
community suggestion system. Owners manage it directly from the episode's owner
controls. A non-owner cannot suggest placing an episode in another show's feed.

When a listener submits, Vecto compares their input against the current episode and
keeps **only the fields they actually changed** — untouched fields are dropped so
they don't clutter your review queue.

---

## 2. Two paths: instant vs. review

Every network has an **auto-approve trust threshold** (a setting on the network).
What happens on submit depends on the submitter's **trust score** in that network:

- **Trusted submitter** (trust score ≥ the network threshold): the edit is
  **applied instantly**, scored, and the feed/transcript caches refresh. The
  submitter sees "Edit approved instantly. +N Trust."
- **Standard submitter** (below the threshold): the edit becomes **Pending** and
  lands in your **Community Edits** inbox for review. Nothing changes on the
  episode until you approve it.

Either way the edit is scored the **same way** (see §3) — instant approval is not a
shortcut around the scoring rules.

> **First responder:** the first approved edit on a given episode earns a small
> bonus (see §3). This rewards the person who first improves an untouched episode.

---

## 3. How scoring works

Approving (or auto-approving) an edit awards the submitter **Trust points** and
increments their **contribution counters** (which drive profile stats and badges).
Trust and counters are recorded on the edit at approval, so a later rollback
reverses the **exact** amounts.

### Per-section points

| Section | Trust points | Contribution counter |
|---------|-------------|----------------------|
| Title | +1 | Titles |
| Description | +1 | Descriptions |
| Tags | **+1 flat** (any number of tags) | Tags (+1 per tag *added*) |
| Chapters | **+1 per chapter** | Chapters (+1 per chapter) |
| Season # / Episode # / Episode Type | +1 **each** | Episodes Sequenced (+1 per field) |
| Speaker Labels | +1 per newly-named speaker, +1 flat for a correction | Voices Named |

Note the deliberate asymmetry on **Tags**: trust is a flat +1 (so you can't farm
points by adding many low-value tags), but the contribution counter credits each
tag actually added.

### Bonuses

- **Multi-field bonus** (trust only, not a counter):
  - **+2** if **3 or more** of the seven core fields are approved in one edit.
  - **+4** if **all seven** are approved.
  - These do **not** stack — all-seven is +4 total, not +2 plus +4.
- **First responder:** **+1** trust (plus a "first responder" counter) when this is
  the first approved edit on the episode.

**Cross-publish is never scored** (it's an owner action, not a contribution).

### Worked example

A single edit that changes title, description, 2 tags, 3 chapters, season, episode,
and type:

```
Title +1 · Description +1 · Tags +1 · Chapters +3 · Season +1 · Episode +1 · Type +1
  = 9 (Sections)
All 7 core fields approved  = +4 (Bonus)
Not the first edit on this episode = +0 (First responder)
-------------------------------------------------
Total = 13 Trust
```

The contribution counters from the same edit would be: Titles +1, Descriptions +1,
Tags +2, Chapters +3, Episodes Sequenced +3.

---

## 4. Reviewing in the Community Edits inbox

Pending edits appear in **Creator Settings → Community Edits**. Each edit is broken
into **sections**, one per changed field, shown as Original vs. Suggested (and a
third "Current" column with a **CHANGED** badge if the live episode drifted since
the listener submitted — a conflict warning that the snapshot is stale).

### Approving selectively

Every section has its own **Approve toggle**, on by default. You can approve some
sections and leave others off — for example, take the tags and description but not
the title. As you toggle sections:

- Each section shows a **+N points** badge; an unchecked section's badge dims and
  drops out of the total.
- A running **Sections · Bonus · First responder = +N Trust** breakdown and the
  **Approve & Apply (+N Trust)** button update live to show exactly what will be
  awarded for the current selection (bonus included).

Press **Approve & Apply** to apply only the checked sections. The episode updates,
the submitter is awarded the shown trust + counters, and the episode's metadata is
**locked** (so a flurry of further edits doesn't immediately pile on).

> **Only what you approve is recorded.** Sections you leave unchecked are not
> applied, not scored, and do not appear in the audit log for that edit.

### Edge cases

- **Approving with nothing selected** converts the edit to a **rejection** and
  penalises the submitter **−2 Trust** (treat it as "this had no usable changes").
- **Reject** (the explicit button) is **−2 Trust** and applies nothing.

---

## 5. The audit log

**Creator Settings → Audit Log** is the record of every resolved edit (approved,
rejected, rolled back, superseded). For an approved edit it shows:

- The **before → after** for each approved section, each with its **+N** badge.
- A **Sections · Bonus · First responder = +N Trust** tally that reconciles to the
  exact trust the edit earned.
- The submitter's **current trust → trust after revert**, so you can see the effect
  of a rollback before you click it.

You can filter the log by episode/show, status, and user.

---

## 6. Rolling back

Two reversal actions live on each approved edit in the audit log:

- **Revert Edit (−N Trust):** rolls back this one edit. It restores the episode to
  the state it was in **right before that edit was approved**, and reverses the
  **exact** trust points and counters the edit earned (an exact wash — the
  button shows the precise amount). If it was the last approved edit on the
  episode, the metadata lock is released.
- **Bulk Rollback User:** the "griefer" button. Reverts **every** approved edit
  that user has made across the network and reverses each one's exact award. Use
  this when an account has been vandalising at scale.

Notes:

- **Speaker-label rollbacks** are always safe and order-correct: speaker state is
  rebuilt by replaying the remaining approved labels over the original diarization,
  so reverting one label edit never corrupts the others.
- **Metadata rollbacks** restore the captured pre-edit snapshot. If newer approved
  edits exist on the same episode, Vecto blocks a single metadata rollback and asks
  you to roll the newer ones back first (so you don't silently clobber later work).
- **Very old edits** that predate the current scoring model earn/reverse nothing —
  they show no points and revert no trust. (A one-time maintenance command can
  retroactively bank points onto those historical edits if you want their future
  rollbacks to be exact — see §7.)

---

## 7. Related maintenance commands

Run from the admin console (Maintenance / Transcription categories) or the CLI.
All are preview-by-default and idempotent; add `--apply` to commit.

- **`backfill_edit_points`** — banks trust points + counters onto historical
  approved edits that predate the unified scoring model, so their future rollbacks
  are exact. Does not change anyone's current totals.
- **`backfill_speaker_ids`** — one-time conversion that stamps the immutable
  diarization base onto existing transcripts and retroactively credits "Voices
  Named." (Speaker-feature specific; see transcription.md.)

---

## 8. Quick reference

| Action | Trust effect |
|--------|-------------|
| Approve a section | + the section's points (see §3) |
| Approve 3+ core fields together | additional **+2** bonus |
| Approve all 7 core fields together | additional **+4** bonus (replaces the +2) |
| First approved edit on an episode | additional **+1** |
| Approve with nothing checked | **−2** (becomes a rejection) |
| Reject | **−2** |
| Revert a single edit | reverses that edit's exact award |
| Bulk rollback a user | reverses every approved edit's exact award |
| Cross-publish | never scored |
