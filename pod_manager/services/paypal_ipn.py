"""PayPal Website Payments Standard: the checkout URL and Instant
Payment Notification (IPN) handling.

PayPal POSTs an IPN to the form's notify_url after a payment, and again on a
refund or reversal. Nothing in it is trusted until PayPal itself confirms
it: the exact body is posted back with cmd=_notify-validate and must come
back "VERIFIED". Then it must name this network's PayPal account and one of
its commissions (by reference, sent as `invoice`).

A verified payment marks the commission paid, which stops it expiring. It
never approves it: the hosts still vet every pick.
"""
import logging
from decimal import Decimal, InvalidOperation

import requests
from django.utils import timezone

from ..models import Commission

logger = logging.getLogger(__name__)

CHECKOUT_URL = {False: 'https://www.paypal.com/cgi-bin/webscr',
                True: 'https://www.sandbox.paypal.com/cgi-bin/webscr'}
VERIFY_URL = {False: 'https://ipnpb.paypal.com/cgi-bin/webscr',
              True: 'https://ipnpb.sandbox.paypal.com/cgi-bin/webscr'}


def verify_with_paypal(raw_body, sandbox):
    """True when PayPal confirms it sent this exact IPN body."""
    try:
        resp = requests.post(
            VERIFY_URL[sandbox], data=b'cmd=_notify-validate&' + raw_body, timeout=20,
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': 'Vecto-IPN'})
    except requests.RequestException as e:
        logger.warning(f"[PayPal IPN] Verification request failed: {e}")
        return False
    return resp.text.strip() == 'VERIFIED'


def _money(raw):
    try:
        return Decimal(raw).quantize(Decimal('0.01'))
    except (InvalidOperation, TypeError):
        return None


def handle_ipn(network, raw_body, params):
    """Apply one IPN to `network`'s commissions. Returns a short outcome
    string (logged, and asserted on by the tests)."""
    settings = getattr(network, 'commission_settings', None)
    if settings is None or not settings.active_paypal_email:
        return 'ignored: commissions not set up'
    # A sandbox IPN must never mark a real request paid, nor the reverse.
    if (params.get('test_ipn') == '1') != settings.paypal_sandbox:
        return 'ignored: sandbox mismatch'
    if not verify_with_paypal(raw_body, settings.paypal_sandbox):
        return 'ignored: not verified by PayPal'

    receiver = (params.get('receiver_email') or params.get('business') or '').strip().lower()
    if receiver != settings.active_paypal_email.strip().lower():
        return 'ignored: different PayPal account'

    status = params.get('payment_status', '')
    txn_id = params.get('txn_id', '')
    commission = network.commissions.filter(reference=params.get('invoice', '')).first()
    if commission is None and params.get('parent_txn_id'):
        # Refunds and reversals carry the original payment as parent_txn_id.
        commission = network.commissions.filter(paypal_txn_id=params['parent_txn_id']).first()
    if commission is None:
        return 'ignored: unknown commission'

    if status in ('Refunded', 'Reversed'):
        commission.payment_flag = Commission.PaymentFlag.REFUNDED
        commission.save(update_fields=['payment_flag', 'updated_at'])
        return 'flagged refunded'
    if status != 'Completed':
        # e.g. Pending (an eCheck still clearing): PayPal sends another IPN
        # when it completes.
        return f'noted: {status or "no status"}'

    if not txn_id or Commission.objects.filter(paypal_txn_id=txn_id).exists():
        return 'ignored: duplicate'
    amount, currency = _money(params.get('mc_gross')), params.get('mc_currency', '').upper()
    commission.paypal_txn_id = txn_id
    commission.paid_amount = amount
    commission.paid_currency = currency
    if commission.quoted_price is not None and (amount != commission.quoted_price or currency != settings.currency):
        # The buyer edited the form's amount. Recorded, not treated as paid.
        commission.payment_flag = Commission.PaymentFlag.AMOUNT_MISMATCH
        outcome = 'flagged amount mismatch'
    else:
        commission.payment_flag = Commission.PaymentFlag.NONE
        commission.paid_marked_at = commission.paid_marked_at or timezone.now()
        outcome = 'paid'
    commission.save(update_fields=['paypal_txn_id', 'paid_amount', 'paid_currency', 'payment_flag',
                                   'paid_marked_at', 'updated_at'])
    return outcome
