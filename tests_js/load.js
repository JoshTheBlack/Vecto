'use strict';
// Loads one of the site's browser scripts (IIFEs that hang their API off `window`) into a
// vm sandbox so it can be tested under plain Node, no bundler or DOM library needed.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const JS_DIR = path.join(__dirname, '..', 'pod_manager', 'static', 'pod_manager', 'js');

function loadScript(name, extraGlobals) {
    const window = {};
    const sandbox = Object.assign({ window, console }, extraGlobals || {});
    vm.createContext(sandbox);
    vm.runInContext(fs.readFileSync(path.join(JS_DIR, name), 'utf8'), sandbox, { filename: name });
    return window;
}

// A stand-in for a chapter row: querySelector(sel) returns the stub registered for that
// selector (or null), which is all serializeRow/collectChapters ask of a row.
function fakeRow(fields) {
    return {
        querySelector(sel) {
            return Object.prototype.hasOwnProperty.call(fields, sel) ? fields[sel] : null;
        },
    };
}

function fakeContainer(rows) {
    return { querySelectorAll: () => rows };
}

module.exports = { loadScript, fakeRow, fakeContainer };
