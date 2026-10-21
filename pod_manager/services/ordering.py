"""Drag-to-reorder persistence for any model with a `position` field."""
from django.db import transaction


def parse_id_list(raw):
    """'3,1,2' -> [3, 1, 2]. Returns None if any entry is not an integer."""
    parts = [p.strip() for p in (raw or '').split(',') if p.strip()]
    if not all(p.isdigit() for p in parts):
        return None
    return [int(p) for p in parts]


def apply_order(queryset, ordered_ids):
    """Rewrite `position` so rows follow `ordered_ids`, in one transaction.

    `ordered_ids` must name exactly the rows in `queryset`, each once: a partial
    list would leave the rest colliding with the new positions, and a foreign id
    means the client is reordering rows it doesn't own. Returns False (and
    changes nothing) when the list doesn't match.
    """
    if len(ordered_ids) != len(set(ordered_ids)):
        return False
    with transaction.atomic():
        rows = {row.pk: row for row in queryset.select_for_update()}
        if set(rows) != set(ordered_ids):
            return False
        for position, pk in enumerate(ordered_ids):
            rows[pk].position = position
        queryset.model.objects.bulk_update(rows.values(), ['position'])
    return True
