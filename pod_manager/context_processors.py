from django.urls import reverse

from .models import Commission, EpisodeEditSuggestion, EpisodeMatchSuggestion, Network

def current_network(request):
    """
    Grabs the primary network and makes it available to ALL HTML templates
    automatically so the theme loads on every page.
    """
    if hasattr(request, 'network'):
        return {'current_network': request.network}
    return {}


def _plural(n, word):
    return f"{n} {word}{'' if n == 1 else 's'}"


def pending_approvals(request):
    """Floating-badge counts of items awaiting owner review — PENDING community
    edits, PENDING suggested pairs (Merge Desk) and commission requests awaiting
    approval — aggregated across every network the user owns (or all networks
    for superusers). NOT scoped to request.network, since that's domain-matched
    and is often None on the shared admin console path where owners actually
    manage their queue.

    pending_approval_count is the badge's TOTAL (kept under its original name
    so every consumer stays live). The badge links to the first queue with work
    (edits, then pairs, then commissions) and its tooltip lists them all; both
    are built here so _boosted_region_close.html stays declarative."""
    user = getattr(request, 'user', None)
    if not user or not user.is_authenticated:
        return {}

    if user.is_superuser:
        networks = Network.objects.all()
    else:
        networks = user.owned_networks.all()
        if not networks.exists():
            return {}

    edit_count = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network__in=networks,
        status=EpisodeEditSuggestion.Status.PENDING,
    ).count()
    pair_count = EpisodeMatchSuggestion.objects.filter(
        network__in=networks,
        status=EpisodeMatchSuggestion.Status.PENDING,
    ).count()
    pending_commissions = Commission.objects.filter(
        network__in=networks, status=Commission.Status.PENDING)
    commission_count = pending_commissions.count()

    total = edit_count + pair_count + commission_count
    creator = reverse('creator_settings')
    if edit_count:
        url = f"{creator}?tab=inbox"
    elif pair_count:
        url = f"{creator}?tab=merge&merge_view=pairs"
    elif commission_count:
        slug = pending_commissions.order_by('requested_at').values_list('network__slug', flat=True).first()
        url = f"{creator}?network={slug}&tab=commissions"
    else:
        url = creator
    parts = [text for count, text in (
        (edit_count, _plural(edit_count, 'edit')),
        (pair_count, _plural(pair_count, 'suggested pair')),
        (commission_count, _plural(commission_count, 'commission request')),
    ) if count]
    return {
        'pending_approval_count': total,
        'pending_edit_count': edit_count,
        'pending_pair_count': pair_count,
        'pending_commission_count': commission_count,
        'pending_approval_url': url,
        'pending_approval_summary': (
            f"{_plural(total, 'item')} awaiting review: {', '.join(parts)}" if total else ''),
    }


def htmx(request):
    """Exposes the base template chosen by HtmxBaseTemplateMiddleware so
    converted templates can `{% extends base_template|default:... %}`: the skinny
    base_htmx.html on an htmx-boosted request, the full base.html otherwise.
    Falls back to base.html if the middleware didn't run (e.g. a bare
    RequestFactory request in a unit test).

    `is_htmx` is the same signal, exposed directly for the one thing the base
    template can't express: an hx-swap-oob fragment is only meaningful in an AJAX
    RESPONSE. htmx processes it there; on a full page load nothing does, so the
    fragment renders inline as a stray, duplicate-id'd element. Any template that
    is EVER rendered on a full load — which includes every eager creator tab —
    must gate its OOB on this. Mirrors the middleware's condition exactly.
    """
    return {
        'base_template': getattr(request, 'base_template', 'pod_manager/base.html'),
        'is_htmx': bool(request.headers.get('HX-Request')) if hasattr(request, 'headers') else False,
    }
