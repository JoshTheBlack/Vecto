# Per-Network Theme Fonts

Network owners can upload a self-hosted `.woff2` font that becomes the
network's site-wide body font, replacing the old Google Fonts CDN link
(removed — all front-end assets are self-hosted).

- **Where:** Creator Settings → Network Profile → **Custom Font** section.
- **Who:** network owners; one font file per network.
- **Source map:** upload handler in
  [`pod_manager/views/creator/actions.py`](../pod_manager/views/creator/actions.py)
  (`handle_update_network_font`), fields on `Network`
  ([`models.py`](../pod_manager/models.py)), `@font-face` in
  [`base.html`](../pod_manager/templates/pod_manager/base.html).

## Upload rules

- `.woff2` only, max 2 MB, content-verified (magic bytes).
- **Font Name** is what the generated `@font-face` declares; it's prepended
  to the theme's existing `font_family` stack, so the old stack remains the
  fallback if the file ever fails to load.
- Re-uploading replaces the font in place; **Remove custom font** restores
  the plain theme stack.
- Re-uploads propagate immediately — the served URL carries a version that
  bumps on every upload, so the CDN's year-long cache never pins an old file.

## Getting a file from Google Fonts (gotchas)

Google's CSS URL (e.g. `…css2?family=Montserrat:wght@400;700`) is not one
font — it's many `@font-face` rules: several **script subsets** (latin,
latin-ext, cyrillic, …) × each requested weight, each pointing at a different
`.woff2`. Grab the file from the block commented `/* latin */` unless you
need other scripts; a wrong subset renders with missing or odd glyphs.

## Weights (bold)

The `@font-face` is declared with a **weight range (100–900)**:

- **Variable fonts** (what Google serves for most families today, even from
  a "400" URL): body text renders at true 400 and bold at true 700 from the
  single file — full fidelity, nothing else to upload. Note these files
  often carry misleading internal names ("Montserrat Thin") — that metadata
  is ignored; the range declaration is what makes weights render correctly.
- **Static single-cut fonts:** the one cut serves every weight as-is, so
  bold text renders at the cut's own weight (no browser faux-bold). If a
  network ships a static cut and misses bold, that's the trade-off to
  revisit.

## Serving note (prod)

Fonts are CORS-fetched from the media CDN; the R2 bucket's CORS policy
(origins `*`, methods GET+HEAD) covers it. If a custom font silently falls
back to the theme stack in prod, check DevTools for a CORS error first.
