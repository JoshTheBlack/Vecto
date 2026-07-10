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
  (page, JSON event source, mutations),
  [`pod_manager/services/release_calendar.py`](../pod_manager/services/release_calendar.py)
  (matching/linking rules),
  [`calendar.html`](../pod_manager/templates/pod_manager/calendar.html),
  ICS feed in [`pod_manager/views/feeds.py`](../pod_manager/views/feeds.py).

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

Once linked:

- The entry adopts the episode's title, season/episode/type, and description,
  and its time follows the episode's schedule from then on.
- **Linked entries can't be dragged** — reschedule the episode instead
  (the entry's Edit button routes to the publisher while unpublished).
- The public link appears **only after the episode publishes** — a scheduled
  episode shows on the calendar and in the ICS feed with no URL until it goes
  live. Freeform entries link to their external URL.
- Deleting an unpublished episode returns its entry to the unlinked pool with
  its planned time — the plan survives the episode.

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
