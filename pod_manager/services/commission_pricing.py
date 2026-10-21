"""Commission prices: pure functions over CommissionSettings and the queue length.

The formula: base_price + step_amount for every `step_every` commissions already
in the queue, clamped to [min_price, max_price] where those are set. An
override_price replaces the formula outright. Each version's signed
price_adjustment then applies on top, and no price goes below zero.
"""
from decimal import Decimal

ZERO = Decimal('0.00')
_MAX_LOOKAHEAD_STEPS = 10_000

_CURRENCY_SYMBOLS = {'USD': '$', 'CAD': 'CA$', 'AUD': 'A$', 'EUR': '€', 'GBP': '£'}


def queue_length(network):
    """Approved commissions not yet released: the count the price is based on."""
    from ..models import Commission
    return network.commissions.filter(status__in=Commission.ACTIVE_STATUSES).count()


def base_price(settings, n):
    """The formula's price with `n` commissions in the queue, before versions."""
    if settings.override_price is not None:
        return settings.override_price
    price = settings.base_price + settings.step_amount * (n // max(settings.step_every, 1))
    if settings.min_price is not None:
        price = max(price, settings.min_price)
    if settings.max_price is not None:
        price = min(price, settings.max_price)
    return price


def price_for(settings, version, n):
    """What `version` costs now. `version` may be None (a network with no versions)."""
    adjustment = version.price_adjustment if version else ZERO
    return max(ZERO, base_price(settings, n) + adjustment)


def price_table(settings, versions, n):
    """[(version, price)] for each version, or [(None, price)] when there are none."""
    versions = list(versions)
    if not versions:
        return [(None, price_for(settings, None, n))]
    return [(v, price_for(settings, v, n)) for v in versions]


def price_steps(settings, n, count):
    """The next `count` changes of the base price as the queue grows from `n`:
    [(commissions_until, new_base)]. Empty when the price is flat (an override,
    no step, or already pinned at the max)."""
    if settings.override_price is not None or settings.step_amount == 0:
        return []
    every = max(settings.step_every, 1)
    current = base_price(settings, n)
    steps = []
    until = every - (n % every)
    # Bounded: a max clamp holds the price flat forever, so stop looking eventually.
    for _ in range(_MAX_LOOKAHEAD_STEPS):
        price = base_price(settings, n + until)
        if price != current:
            steps.append((until, price))
            current = price
            if len(steps) == count:
                break
        until += every
    return steps


def next_increase(settings, n):
    """(commissions_until, new_base) for the next price change, or None if flat."""
    steps = price_steps(settings, n, 1)
    return steps[0] if steps else None


def format_money(amount, currency='USD'):
    """'$600', '$612.50', '€40', or '40.00 SEK' for currencies without a symbol."""
    if amount is None:
        return ''
    amount = Decimal(amount)
    number = f"{amount:,.0f}" if amount == amount.to_integral_value() else f"{amount:,.2f}"
    symbol = _CURRENCY_SYMBOLS.get((currency or '').upper())
    return f"{symbol}{number}" if symbol else f"{number} {currency}"
