# Support Us Page and Commission Queue

Each network can run a **Support Us** page (a list of cards: Patreon, merch, donations, ...) and a **commission queue**: listeners pay the network to watch something and podcast about it. Both are per network, edited in Creator Settings, and themed like the rest of the network's site.

---

## Pages

| URL | What it is |
|---|---|
| `/support/` | The Support Us cards. Navbar shows **Support Us** while it's published. |
| `/support/commissions/` | The queue: prices, the numbered queue, a lazy "Released" list, and the owner's "how it works" copy. Signed-in requesters also see **Your requests**. |
| `/support/commissions/request/` | The request form. Open to guests; signing in is encouraged. |
| `/support/commissions/<ref>/` | One requester's request page, with the PayPal button while unpaid. Guests reach it by their private link (`?t=<token>`). |
| `/support/commissions/<ref>/withdraw/` | POST: the requester cancels a request that isn't approved yet. |
| `/support/commissions/paypal-ipn/` | PayPal's Instant Payment Notification endpoint (no CSRF; verified with PayPal). |

Owners can preview both pages while they're switched off. Everyone else gets a 404.

---

## Creator Settings

- **Support Page tab:** publish switch, title, tagline, cards (drag to reorder). The **Commission card** is a special card that links to the queue and shows "from $X" (the cheapest version at today's queue length). Listeners don't see it while commissions are off.
- **Commissions tab:**
  - **Awaiting approval:** each request with who asked, what PayPal reported, and when it expires. **Approve** adds it to the end of the queue. **Mark paid** is for payments that came some other way. **Decline** removes it and sends nothing.
  - **Queue:** drag to reorder, a status select, edit, and remove with a private reason. **Add entry** creates a commission by hand (backfill), already approved.
  - **Versions:** e.g. No Ads / With Ads, each with a signed price adjustment.
  - **Settings:** pricing with a live preview, PayPal email and sandbox switch, expiry days, and the page copy.

---

## Pricing

`services/commission_pricing.py`, pure functions:

```
price = base + step × floor(queue length ÷ every), held to [min, max]
        (an override replaces all of that)
version price = max(0, price + version adjustment)
```

"Queue length" counts approved, unreleased commissions (Queued, Scheduled, Watched). A request's price is locked when it's submitted.

---

## Lifecycle

`services/commissions.py` owns every transition, and the emails that go with them.

```
request form ──> pending ──Approve──> queued ──> scheduled ──> watched ──> released
                   │                    └─────── Remove (any approved status) ──> removed
                   ├── Withdraw (requester) ──> withdrawn
                   ├── Decline (owner) ───────> removed
                   └── unpaid past N days ────> expired ──Restore──> pending (clock restarts)
removed ──Restore──> queued (end of the queue)
```

Pending, removed, withdrawn and expired commissions are never public and never count toward the price.

**Expiry:** `task_expire_commission_requests`, daily at 05:00 (`settings.CELERY_BEAT_SCHEDULE`). It expires pending requests that are unpaid and older than the network's `pending_expiry_days`, measured from `pending_since`.

---

## Payment (PayPal)

No API keys: the network sets only its PayPal email.

**Testing with the sandbox:** create a sandbox *business* and *personal* account at developer.paypal.com > Sandbox accounts. Put the business account's email in **Sandbox PayPal email**, switch the sandbox on, and pay with the personal account. Sandbox checkout rejects a real account as the payee ("Your payment to this merchant can't be completed"), and nobody can pay their own account, so never test by paying the network's PayPal from the same login. IPN only reaches a public https domain, so automatic confirmation can't be tested from a `.local` dev domain.

1. The request page's **Pay with PayPal** button is a form POSTed to PayPal's Website Payments Standard checkout (`cmd=_xclick`, as PayPal documents it) with the locked amount, the reference as `invoice`, and `notify_url` pointing at the IPN endpoint. It's the one `hx-boost="false"` form (listed in `ALLOWED_FORM_OPT_OUTS`): leaving for PayPal is the point.
2. After payment, PayPal POSTs an **IPN**. `services/paypal_ipn.py` posts the exact body back with `cmd=_notify-validate`, and acts only on `VERIFIED`. It then checks:
   - the network's PayPal account (`receiver_email`);
   - the commission (`invoice`, or `parent_txn_id` for refunds);
   - sandbox vs. live (`test_ipn` must match the network's sandbox switch);
   - duplicates (by `txn_id`).
3. **Completed** with the quoted amount: the request is marked paid, which stops expiry. A different amount (the buyer can edit it) is flagged **amount mismatch** instead. **Refunded / Reversed** is flagged for the owner, and nothing is removed automatically.

IPN never approves: the hosts still vet every pick.

---

## Guests and accounts

- **Guests** give a name and email. They get a private link (an unguessable `access_token`) on the confirmation page and in every email.
- **Signed-in** requesters skip those fields and see their requests under **Your requests**.
- **Attach on login:** when someone signs in, their guest requests with the same email (any case, any network) attach to the account. This only runs for logins that prove the email: Recurly email code or authenticator, or Patreon when Patreon reports the email verified.
- **`?next=`:** the login pages remember a same-site `?next=` across the OAuth/OTP round trip, so the form's sign-in prompt brings people back.
- **Limits:** 10 pending requests per network per account (or per guest email), and 10 guest submissions per IP per hour.

---

## Emails

One template (`email/commission.html`) on a themed layout (`email/_layout.html`), sent through Celery after commit.

| To | When |
|---|---|
| Requester | received, approved, released, withdrawn, expired |
| Every network owner | new request |

Never emailed: a removal or decline (the owner writes that one by hand), and Scheduled/Watched changes. Owner-added entries have no requester email, so they send nothing.

Pending requests also count toward the floating **awaiting review** badge, and toward the pill on the Commissions tab.
