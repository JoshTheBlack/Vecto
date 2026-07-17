# Release Calendar

A public, per-network planning calendar at `/calendar` that reconciles planned
releases with real episodes. One page serves everyone: listeners get a
read-only themed calendar; network owners get add / edit / delete /
drag-reschedule controls on the same page.

- **Where:** the **Calendar** link in the top nav → `/calendar/` (only on a
  network's configured domain).
- **Who:** public read for everyone; every mutation re-checks network
  ownership server-side — hiding the buttons is not the gate.
- **Source map:** [`pod_manager/views/calendar.py`](../pod_manager/views/calendar.py)
  (page, JSON event source, mutations, `link_episode`/`unlink_episode`, the
  owner episode-search typeahead),
  [`pod_manager/services/release_calendar.py`](../pod_manager/services/release_calendar.py)
  (matching/linking rules + the shared `link_episode_to_entry` /
  `create_calendar_entry_from_episode` helpers),
  [`calendar.html`](../pod_manager/templates/pod_manager/calendar.html),
  the episode-page controls in
  [`pod_manager/views/creator/publish.py`](../pod_manager/views/creator/publish.py)
  (`manage_episode` link/unlink/create actions) +
  [`episode_detail.html`](../pod_manager/templates/pod_manager/episode_detail.html),
  the [`backfill_calendar`](../pod_manager/management/commands/backfill_calendar.py)
  command, ICS feed in [`pod_manager/views/feeds.py`](../pod_manager/views/feeds.py).

## What an entry is

A calendar entry is a planned release: a title and a date/time, optionally
tied to a **podcast** (with season / episode / type to help matching) or fully
**freeform** (with an external link — e.g. a Live Watch). Entries are created
three ways:

1. **Manually** — the owner's *Add Entry* modal on `/calendar`.
2. **Automatically** — scheduling or publishing an episode always puts it on
   the calendar (an entry is created if none was planned).
3. **By reconciliation** — a pre-planned entry and a later real episode merge
   into one row instead of duplicating (see matching below).

**Notes are public copy.** An entry's notes appear on the public calendar's
detail popup and in subscribers' calendar apps as the event description. The
episode's description overwrites them once an episode links.

## Time zone

The calendar displays in **Eastern Time** for every viewer. The add/edit form
has a timezone selector (Eastern default) so times can be entered in any US
zone / UTC / London; storage is UTC and DST is handled correctly.

## Matching and linking rules

When an episode is scheduled, published, or ingested, the system looks for an
unlinked entry to adopt, scoped to the same network + podcast and within
**±60 days** of the episode's release time:

- **Numbered shows:** season + episode number is the key (titles may differ).
- **Unnumbered shows:** title (case-insensitive) + type; an entry with a
  *blank* type matches an episode of any type.
- **Explicit pick:** the publish/schedule form offers a *Link to Calendar
  Entry* selector (filtered to the chosen podcast; freeform entries always
  offered) that overrides matching entirely.
- **No match →** a new entry is auto-created (publish/schedule only).
- **Ingest is link-only:** RSS-ingested episodes link a matching planned
  entry but never auto-create — ~88 feeds would flood the calendar.
  Reconciliation runs for any *not-yet-linked* episode the feed owns on every
  ingest, **not just brand-new ones** — an episode that first ingested under a
  low-priority catch-all feed (wrong podcast, so nothing matched) and later
  auto-migrated to its real podcast is `is_new=False` at the moment its podcast
  finally lines up with a planned entry, so a new-only gate silently skipped it.
  A reverse-OneToOne guard keeps already-linked episodes from re-querying.

## Pre-publish visibility (teasers & hidden entries)

Every entry has a **pre-publish visibility** that controls how it appears on
public surfaces (the page's JSON source and the ICS feed) *while its linked
episode is unpublished*. The moment the episode publishes it is **revealed** —
the actual title/notes/`SxE` always show from then on, whatever the setting was.

- **`actual`** (default) — show the real title, numbering, and notes right away,
  as before.
- **`teaser`** — show `placeholder_title` / `placeholder_notes` instead and
  suppress the `SxE` prefix and type, so a secret stays secret (e.g. list "The
  Santa Clause review" as *Secret Christmas Episode* until it goes live).
- **`hidden`** — omit the entry from the public calendar and ICS entirely until
  it publishes, then it pops in with the real info.

Owners always see the real data on their own `/calendar` (with a "hidden until
publish" / "teaser shown publicly" flag on the tile) so they can manage it — the
masking is a listener-facing view, not a security boundary. Set it from the
**Add/Edit entry** modal, the **publish/schedule form** (as you schedule the
secret), or the episode page's **Release Calendar** block. Implemented as
`CalendarEntry.prepublish_visibility` + the `public_title/notes/show_sxe` reveal
helpers, keyed on the live `episode.is_published` (the same signal that already
withholds the episode URL until publish), so the reveal needs no publish-path
wiring — a Celery-scheduled auto-publish reveals on the next feed poll.

## Manual linking (when auto-match misses)

Auto-matching is deliberately strict (exact title for unnumbered shows, feed
season/episode for numbered ones), so it misses on title drift, cross-feed
migration, or an entry planned after the episode already ingested. Two owner
controls close the gap — both re-check ownership server-side:

- **From `/calendar`** (entry detail popup): *Link episode* opens a typeahead
  over the network's still-unlinked episodes (published, scheduled, or draft);
  picking one adopts it. A linked entry shows *Unlink episode* instead, which
  returns the entry to the planned pool without deleting it.
- **From an episode page** (owner controls → *Release Calendar*): adopt a
  planned entry from a dropdown, or *Create calendar entry from this episode*
  (a fresh entry mirroring the episode), or *Unlink* an existing link.

Manually linking follows the same rule as the auto path: the episode becomes
the source of truth and the entry's time snaps to the episode's schedule.

## Backfilling from existing episodes

`manage.py backfill_calendar` seeds the calendar from the back catalog, placing
each entry at the episode's `pub_date` (adopting a pre-planned entry when one
exists; idempotent on re-run). Published-only by default; preview unless
`--apply` (see [management-command-conventions.md](management-command-conventions.md)).

    python manage.py backfill_calendar --network=baldmove --apply
    python manage.py backfill_calendar --podcast=silo --days=90 --apply
    python manage.py backfill_calendar --all --since=2025-01-01 --until=2025-06-30 --apply
    python manage.py backfill_calendar --all --limit=50 --apply    # start small

Once linked:

- The entry adopts the episode's title, season/episode/type, and description,
  and its time follows the episode's schedule from then on.
- **Linked entries can't be dragged** — reschedule the episode instead
  (the entry's Edit button routes to the publisher while unpublished).
- The public link appears **only after the episode publishes** — a scheduled
  episode shows on the calendar and in the ICS feed with no URL until it goes
  live. Freeform entries link to their external URL.
- Deleting an unpublished episode returns a **pre-planned** entry (one the
  episode adopted by matching or a manual link) to the unlinked pool with its
  planned time — the plan survives the episode. An **episode-born** entry (one
  auto-created on schedule/publish with no pre-plan, backfilled, or made via the
  episode page's *Create entry* button — `CalendarEntry.created_from_episode`)
  is deleted along with the episode, since it only ever represented that
  episode. The episode's **R2 audio is not deleted synchronously** — it's
  reclaimed by the orphan GC (`services/r2_maintenance`) once the row is gone;
  the linked transcript's R2 files *are* deleted immediately on cascade.

Manual moves interact with the pin system: see
[episode-moves-and-feed-priority.md](episode-moves-and-feed-priority.md).

## Subscribing (ICS)

Every network exposes a public feed at `feed/<network-slug>/calendar.ics`.
The subscribe row offers Google Calendar, Apple (webcal), Outlook.com, and a
copy-URL field. Event titles carry `SxEy ·` prefixes for numbered entries;
descriptions carry the podcast/type plus the public notes.

**Google Calendar lag:** Google refreshes subscribed URLs on its own schedule
(commonly 8–24 h). Entries added after its last fetch appear on its next
poll; removing and re-adding the subscription forces a fresh fetch.
