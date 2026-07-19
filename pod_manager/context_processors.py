from .models import EpisodeEditSuggestion, EpisodeMatchSuggestion, Network

def current_network(request):
    """
    Grabs the primary network and makes it available to ALL HTML templates
    automatically so the theme loads on every page.
    """
    if hasattr(request, 'network'):
        return {'current_network': request.network}
    return {}


def pending_approvals(request):
    """Floating-badge counts of items awaiting owner review — PENDING community
    edits AND PENDING suggested pairs (Merge Desk) — aggregated across every
    network the user owns (or all networks for superusers). NOT scoped to
    request.network, since that's domain-matched and is often None on the
    shared admin console path where owners actually manage their queue.

    pending_approval_count is the badge's TOTAL (kept under its original name
    so every consumer stays live); the per-queue counts drive the badge's link
    target and tooltip breakdown in _boosted_region_close.html."""
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
    return {
        'pending_approval_count': edit_count + pair_count,
        'pending_edit_count': edit_count,
        'pending_pair_count': pair_count,
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