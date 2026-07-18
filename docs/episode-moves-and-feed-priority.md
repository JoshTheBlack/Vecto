# Episode Moves, Podcast Pinning & Feed Ingest Priority

How episodes change parent podcasts — manually (single or bulk move) and
automatically (GUID-based migration away from low-priority feeds) — and the
pin that keeps automation's hands off manual decisions.

- **Where:** single move on the episode's detail page (owner panel → *Move to
  Feed*); bulk move in Creator Settings → Bulk Move; the *Low-priority feed*
  checkbox in Creator Settings → Manage Podcasts.
- **Source map:** shared move logic in
  [`pod_manager/services/episode_move.py`](../pod_manager/services/episode_move.py);
  auto-migration hook in
  [`pod_manager/ingesters/default.py`](../pod_manager/ingesters/default.py)
  (`commit_episode`).

## Manual moves and the pin

Moving an episode (single or bulk) stamps it as **pinned** (`podcast_pinned_at`
/ `podcast_pinned_by`). The pin has exactly one meaning: **auto-migration will
never move a pinned episode**. It never blocks manual moves, metadata updates,
or cross-publishing — humans always win. There is no unpin button; moving the
episode where you want it is the unpin.

Moves also clean up now-redundant cross-publish links into the target podcast,
re-key mirrored R2 audio, and rebuild feed fragments. Moves do **not** lock
metadata — a moved episode keeps receiving updates from its source feed.

## Low-priority feeds

Marking a podcast **low-priority** (Manage Podcasts) means:

1. **Auto-migration:** when a normal-priority feed ingests an episode whose
   GUID already exists under a low-priority feed in the same network, the
   episode automatically moves to the normal feed — as if bulk-moved — unless
   it's pinned. Episodes only ever move *away from* low-priority feeds; two
   low-priority feeds never fight over an episode, and normal feeds never
   lose episodes.
2. **Polling order:** low-priority feeds poll ~10 minutes after normal feeds
   each cycle, so the real home feed usually wins the race outright.
3. **Metadata guard:** a low-priority feed never overwrites the metadata or
   audio of an episode it doesn't own — it only refreshes GUID routing
   identifiers.

**Ambiguous matches:** if one ingest pass resolves an episode's public and
private GUIDs to two *different* rows, migration is a **true skip**: the
import log shows `[SKIP MIGRATE] … resolve via Merge Desk`, and neither row's
GUIDs, audio, or metadata are touched while the pair is unresolved (an earlier
build fell through to updating the row anyway, which minted a duplicate GUID —
see `planned_migration_match_suggestions.txt` §1b for the history). Instead,
the pair is persisted as a **Suggested Pair** — reviewable in the Merge Desk's
*Suggested Pairs* mode, alongside a badge count on the Merge Desk tab. Open a
card's *Review & Merge* to resolve it with the field-level merge editor (pick
the surviving row, reconcile each field, choose the parent podcast), or
*Dismiss* it to suppress that exact GUID pair sticky (it won't resurface on
later polls). The next ingest then migrates normally once the survivor carries
both GUIDs.

**Historical duplicates:** the pre-fix fallthrough could already have minted
duplicate-GUID rows before this behavior shipped. `python manage.py
backfill_match_suggestions --network=<slug> --apply` (or `--all`) scans a
network's episodes column-by-column for rows sharing a `guid_public` or
`guid_private`, and seeds the same Suggested Pair rows for each collision
found — live detection can't see this class of pre-existing corruption on its
own. Preview (no `--apply`) reports what it would seed without writing
anything. See the command's own `--help` / module docstring for details.
