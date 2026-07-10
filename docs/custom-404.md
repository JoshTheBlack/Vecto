# Custom 404 Page

Unmatched URLs render a network-themed 404 instead of Django's bare default:
navbar, favicon, a search box posting to the network home, and a random
image + caption from an owner-curated pool.

- **Where:** pool management in Creator Settings → **404 Page** tab.
- **Source map:** [`pod_manager/views/errors.py`](../pod_manager/views/errors.py),
  [`404.html`](../pod_manager/templates/pod_manager/404.html),
  `NotFoundEntry` in [`models.py`](../pod_manager/models.py).

## Behavior

- Each pool entry is a fixed **image + caption pair** — the pair always
  renders together; a random pair is picked per hit. To fix a caption,
  delete the entry and re-add it.
- Empty pool: the page falls back to the network logo (or the Vecto wordmark)
  with a stock line. Unrecognized domains get a generic Vecto-branded 404
  with no search box.
- Images are processed to WebP at 800 px on upload, like other network
  imagery. Deleting an entry also deletes its stored image.
- The handler only runs with `DEBUG=False` (standard Django behavior).
