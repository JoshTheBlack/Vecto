/*
 * Shared chapter-row editor — the ONE implementation behind every surface that
 * edits Podcasting-2.0 chapters (planned_migration_match_suggestions.txt Q10).
 *
 * Consumers (all four):
 *   - episode_detail.html   community-edit form
 *   - publish_episode.html  publish/schedule form
 *   - creator_tabs/tab_inbox.html   inbox review diff-grid (readonly + editable)
 *   - creator_tabs/_match_editor.html   the field-level merge editor
 *
 * Before this file each surface carried its own copy of addChapterRow +
 * formatTime/parseTime + a serialize loop, which drifted apart. This module
 * exposes the row markup, the two chapter columns' time helpers, and a
 * serializer; each surface keeps only its own container/orchestration.
 *
 * Pure helpers + DOM builders — it binds NO listeners to document/window, so it
 * needs no VectoPage cleanup and is safe to (re-)execute on every boosted swap.
 * Load it with a bumped ?v= whenever this file changes (static filenames are not
 * hashed — see the v= note in creator_settings.html).
 */
(function () {
    'use strict';

    function escapeHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function formatTime(totalSeconds) {
        if (!totalSeconds) return '00:00:00';
        const h = Math.floor(totalSeconds / 3600).toString().padStart(2, '0');
        const m = Math.floor((totalSeconds % 3600) / 60).toString().padStart(2, '0');
        const s = Math.floor(totalSeconds % 60).toString().padStart(2, '0');
        return `${h}:${m}:${s}`;
    }

    function parseTime(timeStr) {
        const parts = String(timeStr || '').split(':').reverse();
        let seconds = 0;
        if (parts[0]) seconds += parseInt(parts[0], 10) || 0;
        if (parts[1]) seconds += (parseInt(parts[1], 10) || 0) * 60;
        if (parts[2]) seconds += (parseInt(parts[2], 10) || 0) * 3600;
        return seconds;
    }

    // opts:
    //   readonly       {bool}   disable every input, drop the controls
    //   inputClass     {str}    extra classes on each input (surface styling)
    //   labelClass     {str}    extra classes on the field labels
    //   controlStyle   {str}    'inline'  -> geo/remove buttons with inline onclick
    //                           'hooks'   -> .chap-toggle-loc / .chap-remove (caller wires listeners)
    //                           'none'    -> no controls (readonly diff cells)
    //   controlsSeparate {bool} put the controls in their own full-width row
    //                           (the inbox layout) instead of the header cluster
    //   rowSelector    {str}    the .closest() hook inline onclick walks up to
    function rowInnerHTML(chapter, opts) {
        chapter = chapter || {};
        opts = opts || {};
        const readonly = !!opts.readonly;
        const ic = opts.inputClass != null ? opts.inputClass : 'bg-black text-light border-secondary';
        const lc = opts.labelClass != null ? opts.labelClass : 'text-muted';
        const controlStyle = opts.controlStyle || (readonly ? 'none' : 'inline');
        const rowSelector = opts.rowSelector || '.chapter-row';
        const dis = readonly ? ' disabled' : '';

        const startTimeStr = formatTime(chapter.startTime);
        const endTimeStr = chapter.endTime != null && chapter.endTime !== '' ? formatTime(chapter.endTime) : '';
        const title = escapeHtml(chapter.title);
        const url = escapeHtml(chapter.url);
        const img = escapeHtml(chapter.img);
        const toc = chapter.toc !== false;
        const loc = chapter.location || null;
        const locName = escapeHtml(loc ? (loc.name || '') : '');
        const locGeo = escapeHtml(loc ? (loc.geo || '') : '');
        const locOsm = escapeHtml(loc ? (loc.osm || '') : '');
        const locVisible = !!(loc && (loc.name || loc.geo));

        let controls = '';
        if (controlStyle === 'inline') {
            controls =
                `<button type="button" class="btn btn-sm btn-outline-info" title="Toggle Location" onclick="this.closest('${rowSelector}').querySelector('.location-fields').classList.toggle('d-none')"><i class="bi bi-geo-alt"></i></button>` +
                `<button type="button" class="btn btn-sm btn-outline-danger" title="Remove Chapter" onclick="this.closest('${rowSelector}').remove()"><i class="bi bi-trash"></i></button>`;
        } else if (controlStyle === 'hooks') {
            controls =
                `<button type="button" class="btn btn-sm btn-outline-info chap-toggle-loc" title="Toggle Location"><i class="bi bi-geo-alt"></i></button>` +
                `<button type="button" class="btn btn-sm btn-outline-danger chap-remove" title="Remove Chapter"><i class="bi bi-trash"></i></button>`;
        }

        // Header cluster: Start / End / Title, with the controls either tucked
        // into the title row (episode_detail/publish) or split out below (inbox).
        const headerControls = (controls && !opts.controlsSeparate)
            ? `<div class="col-md-2 text-end mt-4 d-flex justify-content-end gap-2">${controls}</div>`
            : '';
        const separateControls = (controls && opts.controlsSeparate)
            ? `<div class="col-12 mt-2 chapter-row-controls d-flex justify-content-end gap-2">${controls}</div>`
            : '';
        const titleCol = headerControls ? 'col-md-6' : 'col-md-8';

        return `
            <div class="col-md-2">
                <label class="form-label small ${lc} mb-1">Start Time</label>
                <input type="text" class="form-control form-control-sm ${ic} chap-time" placeholder="00:00:00" value="${startTimeStr}"${dis}>
            </div>
            <div class="col-md-2">
                <label class="form-label small ${lc} mb-1">End Time</label>
                <input type="text" class="form-control form-control-sm ${ic} chap-endtime" placeholder="Optional" value="${endTimeStr}"${dis}>
            </div>
            <div class="${titleCol}">
                <label class="form-label small ${lc} mb-1">Chapter Title</label>
                <input type="text" class="form-control form-control-sm ${ic} chap-title" placeholder="Required" value="${title}"${dis}>
            </div>
            ${headerControls}
            <div class="col-md-5 mt-2">
                <input type="url" class="form-control form-control-sm ${ic} chap-url" placeholder="Optional Link URL (https://…)" value="${url}"${dis}>
            </div>
            <div class="col-md-5 mt-2">
                <input type="url" class="form-control form-control-sm ${ic} chap-img" placeholder="Optional Image URL (https://…)" value="${img}"${dis}>
            </div>
            <div class="col-md-2 mt-2 d-flex align-items-center">
                <div class="form-check form-switch ms-2">
                    <input class="form-check-input chap-toc" type="checkbox" role="switch" ${toc ? 'checked' : ''}${dis}>
                    <label class="form-check-label small ${lc}">TOC</label>
                </div>
            </div>
            ${separateControls}
            <div class="col-12 mt-2 location-fields ${locVisible ? '' : 'd-none'}">
                <div class="p-2 border border-info rounded ${opts.locBoxClass != null ? opts.locBoxClass : 'bg-black'} d-flex gap-2 flex-wrap">
                    <div class="flex-grow-1" style="min-width: 12rem;">
                        <input type="text" class="form-control form-control-sm ${ic} chap-loc-name" placeholder="Location Name (e.g. Elmyr, Atlanta)" value="${locName}"${dis}>
                    </div>
                    <div class="flex-grow-1" style="min-width: 12rem;">
                        <input type="text" class="form-control form-control-sm ${ic} chap-loc-geo" placeholder="GeoURI (e.g. geo:33.7653,-84.3494)" value="${locGeo}"${dis}>
                    </div>
                    <div class="flex-grow-1" style="min-width: 12rem;">
                        <input type="text" class="form-control form-control-sm ${ic} chap-loc-osm" placeholder="OSM URL (Optional)" value="${locOsm}"${dis}>
                    </div>
                </div>
            </div>`;
    }

    // Build a standalone editable row element (the episode_detail / publish /
    // merge-editor container layout). opts as rowInnerHTML plus:
    //   rowClass {str} classes on the wrapping element (default the shared
    //                  .chapter-row shell). Inline controls walk up to
    //                  opts.rowSelector, defaulted from the first class token.
    function createRow(chapter, opts) {
        opts = opts || {};
        const rowClass = opts.rowClass || 'row g-2 align-items-center mb-3 chapter-row p-3 rounded bg-dark border border-secondary';
        const rowSelector = opts.rowSelector || ('.' + (rowClass.split(/\s+/).find(c => c.indexOf('chapter-row') !== -1) || 'chapter-row'));
        const row = document.createElement('div');
        row.className = rowClass;
        row.innerHTML = rowInnerHTML(chapter, Object.assign({}, opts, { rowSelector: rowSelector }));
        return row;
    }

    function appendRow(container, chapter, opts) {
        const row = createRow(chapter, opts);
        container.appendChild(row);
        return row;
    }

    // Read one .chapter-row (or any element holding .chap-* inputs) into a
    // Podcasting-2.0 chapter object, or null when it has no title/time. Matches
    // the previous per-surface serialize loops exactly.
    function serializeRow(row) {
        const timeEl = row.querySelector('.chap-time');
        const titleEl = row.querySelector('.chap-title');
        if (!timeEl || !titleEl) return null;
        const timeStr = timeEl.value;
        const title = titleEl.value.trim();
        if (!title || !timeStr) return null;

        const chap = { startTime: parseTime(timeStr), title: title };
        const endStr = (row.querySelector('.chap-endtime') || {}).value;
        if (endStr && endStr.trim()) {
            const parsedEnd = parseTime(endStr.trim());
            if (parsedEnd > chap.startTime) chap.endTime = parsedEnd;
        }
        const url = ((row.querySelector('.chap-url') || {}).value || '').trim();
        const img = ((row.querySelector('.chap-img') || {}).value || '').trim();
        if (url.startsWith('http')) chap.url = url;
        if (img.startsWith('http')) chap.img = img;
        const tocEl = row.querySelector('.chap-toc');
        if (tocEl && !tocEl.checked) chap.toc = false;
        const locName = ((row.querySelector('.chap-loc-name') || {}).value || '').trim();
        const locGeo = ((row.querySelector('.chap-loc-geo') || {}).value || '').trim();
        if (locName && locGeo) {
            chap.location = { name: locName, geo: locGeo };
            const locOsm = ((row.querySelector('.chap-loc-osm') || {}).value || '').trim();
            if (locOsm) chap.location.osm = locOsm;
        }
        return chap;
    }

    // Collect + sort the chapters under a container. opts.rowSelector picks the
    // rows (default '.chapter-row'); opts.skip(row) drops rows the caller wants
    // excluded (readonly / placeholder diff cells).
    function collectChapters(container, opts) {
        opts = opts || {};
        const selector = opts.rowSelector || '.chapter-row';
        const skip = opts.skip;
        const out = [];
        container.querySelectorAll(selector).forEach(row => {
            if (skip && skip(row)) return;
            const chap = serializeRow(row);
            if (chap) out.push(chap);
        });
        out.sort((a, b) => a.startTime - b.startTime);
        return out;
    }

    // The full Podcasting-2.0 payload: {version, chapters, [waypoints]}. Pass the
    // waypoints checkbox state (or its element) in opts.waypoints.
    function buildPayload(container, opts) {
        opts = opts || {};
        const chapters = collectChapters(container, opts);
        const payload = { version: '1.2.0', chapters: chapters };
        let wp = opts.waypoints;
        if (wp && typeof wp === 'object' && 'checked' in wp) wp = wp.checked;
        if (wp) payload.waypoints = true;
        return payload;
    }

    window.ChapterEditor = {
        escapeHtml: escapeHtml,
        formatTime: formatTime,
        parseTime: parseTime,
        rowInnerHTML: rowInnerHTML,
        createRow: createRow,
        appendRow: appendRow,
        serializeRow: serializeRow,
        collectChapters: collectChapters,
        buildPayload: buildPayload,
    };
})();
