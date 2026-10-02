# Content rating (`itunes:explicit`)

## Resolution

An episode's published rating is resolved, first match wins:

1. `Episode.explicit` (True/False; None = inherit)
2. `Podcast.explicit` (creator override on the show form, "Content Rating")
3. `Podcast.feed_explicit` (what the source feed's channel declares, captured on ingest)
4. Explicit (the long-standing default)

`Episode.effective_explicit` and `Podcast.effective_explicit` implement this. Every `<item>`
in a generated feed carries the resolved value, so an episode stays correct when it is
cross-published into a channel with a different rating. The channel tag uses the show's
resolved value.

## Ingestion

feedparser only understands `yes`/`clean` and turns the modern `true`/`false` into None, so
`get_feed` runs `annotate_explicit()` over the raw XML and stores `vecto_explicit` on the
channel and each entry (matched by guid, falling back to document order). `extract_explicit`
prefers that value. A feed that states nothing never erases an existing value.

`Episode.explicit_locked` is set whenever an owner sets a rating by hand (episode page,
publish form, merge); ingestion skips locked episodes. Choosing "Inherit" clears the lock.

## Backfill

Feeds that answer 304 are not re-read, so existing shows only pick up ratings on their next
change. Run once after deploying:

    python manage.py backfill_explicit --all            # preview
    python manage.py backfill_explicit --all --apply    # saves, then rebuilds feed fragments
