from .models import EpisodeEditSuggestion, Network

def current_network(request):
    """
    Grabs the primary network and makes it available to ALL HTML templates
    automatically so the theme loads on every page.
    """
    if hasattr(request, 'network'):
        return {'current_network': request.network}
    return {}


def pending_approvals(request):
    """Badge count of edits awaiting approval, aggregated across every network
    the user owns (or all networks for superusers) — NOT scoped to
    request.network, since that's domain-matched and is often None on the
    shared admin console path where owners actually manage their queue."""
    user = getattr(request, 'user', None)
    if not user or not user.is_authenticated:
        return {}

    if user.is_superuser:
        networks = Network.objects.all()
    else:
        networks = user.owned_networks.all()
        if not networks.exists():
            return {}

    count = EpisodeEditSuggestion.objects.filter(
        episode__podcast__network__in=networks,
        status=EpisodeEditSuggestion.Status.PENDING,
    ).count()
    return {'pending_approval_count': count}


def htmx(request):
    """Exposes the base template chosen by HtmxBaseTemplateMiddleware so
    converted templates can `{% extends base_template|default:... %}`: the skinny
    base_htmx.html on an htmx-boosted request, the full base.html otherwise.
    Falls back to base.html if the middleware didn't run (e.g. a bare
    RequestFactory request in a unit test)."""
    return {'base_template': getattr(request, 'base_template', 'pod_manager/base.html')}