"""Registry + router for lazily-loaded creator-settings tabs.

A tab's body is fetched on first open instead of being rendered into the initial
/creator/ page, so the page ships a light shell and each tab pays its own render
cost only when opened. One registry maps a tab name to the context-builder that
produces its data and the template that renders it — SRP: the builders live in
data.py, this module only wires name -> (builder, template) and dispatches.

Two tabs stay eager in the shell and are intentionally absent here:
  - network  : the default tab (small; would load immediately anyway) and its
               form-validation JS binds at page load.
  - gdrive   : already self-lazy — its JS fetches the file list on tab show.
Merge is lazy too but keeps its own endpoint (merge_desk_partial) because its
in-tab navigation re-fetches the same body.
"""
from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import redirect, render
from django.urls import reverse

from ...utils import diagnostic_page
from .main import _resolve_creator_network
from . import data as _data


def _shows(request, net):
    return _data.gather_manage_podcasts(request, net)


def _mixes(request, net):
    return {**_data.gather_mixes(net), 'network_podcasts': _data.network_podcast_list(net)}


def _move(request, net):
    return {**_data.gather_move_context(request, net), 'network_podcasts': _data.network_podcast_list(net)}


def _crosspub(request, net):
    return {**_data.gather_cross_publish_context(request, net), 'network_podcasts': _data.network_podcast_list(net)}


def _sync(request, net):
    return _data.gather_reports_data()


def _inbox(request, net):
    return _data.gather_inbox(net)


def _transcripts(request, net):
    pods = _data.network_podcast_list(net)
    return {
        'network_podcasts': pods,
        # The picker uses the shared chip widget (podcast id values); the backfill
        # API takes slugs, so hand the JS an id -> slug map. Default: all selected.
        'transcript_all_ids': [p.id for p in pods],
        'transcript_slug_map': {p.id: p.slug for p in pods},
    }


def _notfound(request, net):
    return _data.gather_notfound_context(net)


# name -> (builder(request, current_network) -> context dict, template).
TAB_CONTENT = {
    'shows':       (_shows,                   'pod_manager/creator_tabs/tab_podcasts.html'),
    'mixes':       (_mixes,                   'pod_manager/creator_tabs/tab_mixes.html'),
    'move':        (_move,                    'pod_manager/creator_tabs/tab_move.html'),
    'crosspub':    (_crosspub,                'pod_manager/creator_tabs/tab_cross_publish.html'),
    'sync':        (_sync,                    'pod_manager/creator_tabs/tab_sync.html'),
    'inbox':       (_inbox,                   'pod_manager/creator_tabs/tab_inbox.html'),
    'audit':       (_data.gather_audit_log,   'pod_manager/creator_tabs/tab_audit_log.html'),
    'transcripts': (_transcripts,             'pod_manager/creator_tabs/tab_transcripts.html'),
    'notfound':    (_notfound,                'pod_manager/creator_tabs/tab_notfound.html'),
}


@login_required(login_url='/login/')
@diagnostic_page("Creator Tab (partial)")
def creator_tab_partial(request, tab):
    """Render one lazily-loaded tab body. HTMX swaps the fragment into the tab's
    shell pane; a direct hit (reload, bookmark) is redirected to the full page
    on that tab so the URL stays reloadable."""
    current_network, forbidden = _resolve_creator_network(request)
    if forbidden:
        return forbidden

    entry = TAB_CONTENT.get(tab)
    if entry is None:
        raise Http404("Unknown creator tab")

    if not request.headers.get('HX-Request'):
        params = request.GET.copy()
        params.pop('network', None)
        params['tab'] = tab
        return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&{params.urlencode()}")

    builder, template = entry
    context = {'current_network': current_network, **builder(request, current_network)}
    return render(request, template, context)
