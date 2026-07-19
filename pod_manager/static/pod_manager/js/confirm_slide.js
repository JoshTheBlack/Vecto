/*
 * Two-step inline confirm — replaces window.confirm() popups on destructive
 * actions, site-wide.
 *
 * Usage: put data-confirm-slide="warning text" on the triggering element
 * (submit button, link, plain button with its own JS handler). First click
 * arms it: a confirm checkmark — plus the warning, when the attribute is
 * non-empty — slides out the element's LEFT, so the user has to move the
 * mouse and click a second time to proceed. data-confirm-slide="" gives a
 * checkmark with no warning text.
 *
 * The confirmed action is delivered by RE-CLICKING the original element with
 * a pass-through flag set, so every native and scripted behavior runs
 * exactly as an unguarded click would: form submission (incl. form="id"
 * buttons), htmx boost, link navigation, and any bubble-phase JS listeners.
 * The arming click is stopped in the CAPTURE phase (preventDefault +
 * stopPropagation) so none of those fire early. Clicking anywhere else —
 * including the armed element itself — disarms.
 *
 * Loaded ONCE per full page load from base.html, OUTSIDE the boosted region:
 * plain document listener with a bind guard (NOT VectoPage.on — that cleanup
 * fires on region swaps, and this script never re-executes to re-register).
 * Styles: .confirm-slide rules in base.html.
 */
(function () {
    'use strict';
    if (window.__confirmSlideBound) return;
    window.__confirmSlideBound = true;

    function disarm(btn) {
        var wrap = btn._confirmWrap;
        btn._confirmWrap = null;
        btn.classList.remove('confirm-armed');
        if (!wrap) return;
        wrap.classList.remove('show');
        setTimeout(function () { wrap.remove(); }, 220);
    }

    function disarmAll(except) {
        document.querySelectorAll('.confirm-armed').forEach(function (armed) {
            if (armed !== except) disarm(armed);
        });
    }

    function arm(btn) {
        var wrap = document.createElement('span');
        wrap.className = 'confirm-slide';
        var warningText = btn.getAttribute('data-confirm-slide') || '';
        if (warningText) {
            var warning = document.createElement('span');
            warning.className = 'confirm-slide-warning';
            warning.textContent = warningText;
            wrap.appendChild(warning);
        }
        var go = document.createElement('button');
        go.type = 'button';
        go.className = 'btn btn-sm btn-danger confirm-slide-go';
        go.title = 'Confirm';
        go.innerHTML = '<i class="bi bi-check-lg"></i>';
        wrap.appendChild(go);
        go._confirmTarget = btn;
        btn.parentNode.insertBefore(wrap, btn);
        btn._confirmWrap = wrap;
        btn.classList.add('confirm-armed');
        // Double rAF so the initial (hidden) state paints before the transition.
        requestAnimationFrame(function () {
            requestAnimationFrame(function () { wrap.classList.add('show'); });
        });
    }

    function onClick(e) {
        var go = e.target.closest('.confirm-slide-go');
        if (go) {
            e.preventDefault();
            e.stopPropagation();
            var btn = go._confirmTarget;
            if (btn) {
                disarm(btn);
                btn._confirmPassThrough = true;
                btn.click();   // replay as a real click; the flag lets it through below
            }
            return;
        }
        var target = e.target.closest('[data-confirm-slide]');
        if (target) {
            if (target._confirmPassThrough) {
                target._confirmPassThrough = false;
                return;        // confirmed — native + scripted behavior proceeds
            }
            e.preventDefault();
            e.stopPropagation();
            if (target.classList.contains('confirm-armed')) {
                disarm(target);            // clicking the armed element again = cancel
            } else {
                disarmAll(target);
                arm(target);
            }
            return;
        }
        disarmAll(null);                   // any other click cancels pending confirms
    }

    document.addEventListener('click', onClick, true);
})();
