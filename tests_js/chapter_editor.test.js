'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadScript, fakeRow, fakeContainer } = require('./load');

// Objects built inside the vm sandbox have a different Object.prototype, which strict
// deepEqual rejects; compare their plain-data form instead.
const plain = (x) => JSON.parse(JSON.stringify(x));

const CE = loadScript('chapter_editor.js').ChapterEditor;

test('exposes the documented API', () => {
    for (const name of ['escapeHtml', 'formatTime', 'parseTime', 'rowInnerHTML', 'createRow',
        'appendRow', 'serializeRow', 'collectChapters', 'buildPayload']) {
        assert.equal(typeof CE[name], 'function', name);
    }
});

test('formatTime renders HH:MM:SS and treats empty as zero', () => {
    assert.equal(CE.formatTime(0), '00:00:00');
    assert.equal(CE.formatTime(null), '00:00:00');
    assert.equal(CE.formatTime(65), '00:01:05');
    assert.equal(CE.formatTime(3725), '01:02:05');
    assert.equal(CE.formatTime(36000), '10:00:00');
});

test('parseTime accepts SS, MM:SS and HH:MM:SS and survives junk', () => {
    assert.equal(CE.parseTime('45'), 45);
    assert.equal(CE.parseTime('01:05'), 65);
    assert.equal(CE.parseTime('01:02:05'), 3725);
    assert.equal(CE.parseTime(''), 0);
    assert.equal(CE.parseTime(undefined), 0);
    assert.equal(CE.parseTime('xx:yy'), 0);
});

test('parseTime and formatTime round-trip', () => {
    for (const secs of [1, 59, 60, 3599, 3600, 86399]) {
        assert.equal(CE.parseTime(CE.formatTime(secs)), secs);
    }
});

test('escapeHtml neutralises markup and quotes, and tolerates null', () => {
    assert.equal(CE.escapeHtml(`<img src=x onerror="a('b')">&`),
        '&lt;img src=x onerror=&quot;a(&#39;b&#39;)&quot;&gt;&amp;');
    assert.equal(CE.escapeHtml(null), '');
    assert.equal(CE.escapeHtml(undefined), '');
    assert.equal(CE.escapeHtml(42), '42');
});

test('rowInnerHTML escapes user-supplied chapter fields', () => {
    const html = CE.rowInnerHTML({ startTime: 5, title: '"><script>x()</script>', url: 'http://a/?q="1"' });
    assert.ok(!html.includes('<script>'));
    assert.ok(html.includes('&lt;script&gt;'));
    assert.ok(html.includes('&quot;1&quot;'));
});

test('rowInnerHTML readonly disables inputs and drops the controls', () => {
    const html = CE.rowInnerHTML({ startTime: 0, title: 'x' }, { readonly: true });
    assert.ok(html.includes('disabled'));
    assert.ok(!html.includes('Remove Chapter'));
});

test('rowInnerHTML hooks style exposes listener classes instead of inline onclick', () => {
    const html = CE.rowInnerHTML({ startTime: 0, title: 'x' }, { controlStyle: 'hooks' });
    assert.ok(html.includes('chap-toggle-loc') && html.includes('chap-remove'));
    assert.ok(!html.includes('onclick='));
});

const input = (value, extra) => Object.assign({ value }, extra);

test('serializeRow builds a minimal chapter', () => {
    const row = fakeRow({ '.chap-time': input('00:01:00'), '.chap-title': input('  Intro  ') });
    assert.deepEqual(plain(CE.serializeRow(row)), { startTime: 60, title: 'Intro' });
});

test('serializeRow drops rows without a title or time, or without the inputs at all', () => {
    assert.equal(CE.serializeRow(fakeRow({ '.chap-time': input('00:01:00'), '.chap-title': input('  ') })), null);
    assert.equal(CE.serializeRow(fakeRow({ '.chap-time': input(''), '.chap-title': input('x') })), null);
    assert.equal(CE.serializeRow(fakeRow({})), null);
});

test('serializeRow keeps endTime only when it is after the start', () => {
    const base = { '.chap-time': input('00:01:00'), '.chap-title': input('x') };
    assert.equal(CE.serializeRow(fakeRow({ ...base, '.chap-endtime': input('00:02:00') })).endTime, 120);
    assert.equal('endTime' in CE.serializeRow(fakeRow({ ...base, '.chap-endtime': input('00:00:30') })), false);
    assert.equal('endTime' in CE.serializeRow(fakeRow({ ...base, '.chap-endtime': input('  ') })), false);
});

test('serializeRow only keeps http(s) urls and images', () => {
    const base = { '.chap-time': input('0'), '.chap-title': input('x') };
    const ok = CE.serializeRow(fakeRow({ ...base, '.chap-url': input('https://a.test'), '.chap-img': input('http://i.test/a.png') }));
    assert.equal(ok.url, 'https://a.test');
    assert.equal(ok.img, 'http://i.test/a.png');
    const bad = CE.serializeRow(fakeRow({ ...base, '.chap-url': input('javascript:alert(1)'), '.chap-img': input('data:x') }));
    assert.equal('url' in bad, false);
    assert.equal('img' in bad, false);
});

test('serializeRow records toc:false only when unchecked', () => {
    const base = { '.chap-time': input('0'), '.chap-title': input('x') };
    assert.equal(CE.serializeRow(fakeRow({ ...base, '.chap-toc': { checked: false } })).toc, false);
    assert.equal('toc' in CE.serializeRow(fakeRow({ ...base, '.chap-toc': { checked: true } })), false);
});

test('serializeRow needs both a location name and geo; osm is optional', () => {
    const base = { '.chap-time': input('0'), '.chap-title': input('x') };
    assert.equal('location' in CE.serializeRow(fakeRow({ ...base, '.chap-loc-name': input('Paris') })), false);
    const loc = CE.serializeRow(fakeRow({
        ...base, '.chap-loc-name': input('Paris'), '.chap-loc-geo': input('geo:48.8,2.3'), '.chap-loc-osm': input('R71525'),
    })).location;
    assert.deepEqual(plain(loc), { name: 'Paris', geo: 'geo:48.8,2.3', osm: 'R71525' });
});

test('collectChapters sorts by start time, honours skip, ignores empty rows', () => {
    const mk = (time, title) => fakeRow({ '.chap-time': input(time), '.chap-title': input(title) });
    const rows = [mk('00:05:00', 'late'), mk('00:00:10', 'early'), mk('', 'no time'), mk('00:02:00', 'skipped')];
    const out = CE.collectChapters(fakeContainer(rows), { skip: (r) => r === rows[3] });
    assert.deepEqual(plain(out.map((c) => c.title)), ['early', 'late']);
});

test('buildPayload wraps chapters in the 1.2.0 envelope; waypoints from a bool or a checkbox', () => {
    const rows = [fakeRow({ '.chap-time': input('0'), '.chap-title': input('x') })];
    const c = fakeContainer(rows);
    assert.deepEqual(plain(CE.buildPayload(c)), { version: '1.2.0', chapters: [{ startTime: 0, title: 'x' }] });
    assert.equal(CE.buildPayload(c, { waypoints: true }).waypoints, true);
    assert.equal(CE.buildPayload(c, { waypoints: { checked: true } }).waypoints, true);
    assert.equal('waypoints' in CE.buildPayload(c, { waypoints: { checked: false } }), false);
});
