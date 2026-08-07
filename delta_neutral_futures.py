# delta neutral future mm
import time
import random
import logging
from collections import defaultdict

from optibook.synchronous_client import Exchange

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)
exchange = Exchange()
exchange.connect()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
#EVAL
from eval_bot import EvalBot
EVAL = EvalBot(report_every=30.0, label="delta_future_2")


FUTURES = [ "OB5X_202612_F", "OB5X_202706_F", "OB5X_202703_F"]
HEDGE_FUTURE = FUTURES[0]

TICK_SIZE = 0.10
QUOTE_OFFSET_TICKS = 2

# Base clip size at the touch. Actual resting size scales up toward
# MAX_QUOTE_SIZE as we have more room left under POSITION_LIMIT, which is
# what produces the large partial fills (up to ~40) rather than a flat 5.
BASE_QUOTE_SIZE = 5
MAX_QUOTE_SIZE = 50

# Requote once fair value has moved a full tick. 0 (any move triggers a
# requote) was too aggressive combined with a fast loop -- it produced
# request bursts that tripped the exchange's rate limit and forced a
# disconnect. 1 tick is still much tighter than the original 0.25s-loop
# config while leaving headroom under the rate cap.
REQUOTE_MIN_TICK_MOVE = 1

# Cash delta hedge threshold scaled down from the old value in proportion
# to the size increase (5 -> up to 40, ~8x), so we don't let inventory run
# further just because clips got bigger.
CASH_DELTA_HEDGE_THRESHOLD = 20.0
HEDGE_AGGRESSIVENESS = 0.8

# Confirmed exchange position limit is 500. We quote to a buffer under
# that, not right up to the edge, so a burst of fills between loop
# iterations can't push us into a pre-trade position-limit rejection.
POSITION_LIMIT = 500
SAFETY_POSITION_BUFFER = 100

# Confirmed exchange limits: max position 500, max active (resting) order
# volume 100 per instrument. We stay clear of the volume cap with a margin
# so that even if a delete/insert pair races with the exchange's own order
# processing, we don't blow through the hard cap.
OUTSTANDING_VOLUME_LIMIT = 100
OUTSTANDING_VOLUME_SAFETY_MARGIN = 20

# IMPORTANT: set this to the exchange/competition's *documented* limit if
# you have it (check the optibook docs / competition rules). 40 was a guess
# and tripped a server-side "Max requests per second exceeded" disconnect.
# 12 leaves real headroom under the exchange's likely cap; SAFETY_MARGIN
# below shaves further off the top so local timing jitter can't push us
# over the edge even for a moment.
MAX_UPDATES_PER_SEC = 25
RATE_SAFETY_MARGIN = 0.7  # effective cap = MAX_UPDATES_PER_SEC * this
LOOP_SLEEP_SECONDS = 0.15

MAX_RECONNECT_ATTEMPTS = 10
BASE_RECONNECT_BACKOFF_SECONDS = 1
MAX_RECONNECT_BACKOFF_SECONDS = 3
STABLE_CONNECTION_SECONDS = 30  # reset attempt counter only after this long uptime


# ---------------------------------------------------------------------------
# Rate limiter -- simple token bucket shared by every order action
# ---------------------------------------------------------------------------

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst_cap: float = None):
        self.rate = rate_per_sec
        # Cap how many tokens can accumulate, so a quiet period can't be
        # "cashed in" as one big instant burst later -- this is what
        # protects against the multi-instrument staleness spike that
        # triggered the disconnect.
        self.capacity = burst_cap if burst_cap is not None else rate_per_sec
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    def take(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

        if self.tokens < 1:
            wait = (1 - self.tokens) / self.rate
            time.sleep(wait)
            self.tokens = 0
            self.last_refill = time.monotonic()
        else:
            self.tokens -= 1


_effective_rate = MAX_UPDATES_PER_SEC * RATE_SAFETY_MARGIN
order_rate_limiter = TokenBucket(_effective_rate, burst_cap=max(1.0, _effective_rate / 2))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_mid_price(exchange: Exchange, instrument_id: str):
    book = exchange.get_last_price_book(instrument_id)
    if not book or not book.bids or not book.asks:
        return None
    return (book.bids[0].price + book.asks[0].price) / 2.0


def round_to_tick(price: float) -> float:
    return round(price / TICK_SIZE) * TICK_SIZE


def compute_cash_delta(positions, exchange: Exchange, instruments):
    cash_delta = 0.0
    breakdown = {}
    for instrument_id in instruments:
        qty = positions.get(instrument_id, 0)
        if qty == 0:
            continue
        price = get_mid_price(exchange, instrument_id)
        if price is None:
            continue
        exposure = qty * price
        breakdown[instrument_id] = exposure
        cash_delta += exposure
    return cash_delta, breakdown


def quote_size_for_room(room: int) -> int:
    """Scale clip size up toward MAX_QUOTE_SIZE as more position room is
    available, instead of always quoting a flat size. `room` is the number
    of units we could still take on that side before hitting the buffer."""
    if room <= 0:
        return 0
    return max(1, min(MAX_QUOTE_SIZE, room, BASE_QUOTE_SIZE + room // 2))


def outstanding_volume(orders) -> int:
    """Sum of resting volume across a collection of order objects."""
    return sum(o.volume for o in orders)


def rate_limited_insert(exchange: Exchange, instrument_id, **kwargs):
    order_rate_limiter.take()
    exchange.insert_order(instrument_id, **kwargs)


def rate_limited_delete(exchange: Exchange, instrument_id, order_id):
    order_rate_limiter.take()
    exchange.delete_order(instrument_id, order_id=order_id)


def cancel_all_orders(exchange: Exchange, instrument_id: str):
    for order in exchange.get_outstanding_orders(instrument_id).values():
        rate_limited_delete(exchange, instrument_id, order.order_id)


# How long we're willing to wait for a cancel to actually be reflected in
# get_outstanding_orders() before giving up on inserting this loop. delete
# and insert are separate network round trips -- nothing guarantees the
# exchange applies the delete before a fast-following insert arrives, which
# is exactly what caused outstanding volume to breach the pre-trade limit
# even with a size margin in place (the margin assumed our own cancel had
# already landed; it hadn't).
CANCEL_CONFIRM_TIMEOUT_SECONDS = 1.0
CANCEL_CONFIRM_POLL_SECONDS = 0.05


def cancel_and_confirm(exchange: Exchange, instrument_id: str, orders):
    """Delete each order, then block until none of them still appear in
    get_outstanding_orders(). Returns True if all confirmed gone, False if
    we timed out waiting (caller should NOT insert in that case)."""
    order_ids = {o.order_id for o in orders}
    if not order_ids:
        return True

    for order_id in order_ids:
        rate_limited_delete(exchange, instrument_id, order_id)

    deadline = time.monotonic() + CANCEL_CONFIRM_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        still_resting = {
            o.order_id for o in exchange.get_outstanding_orders(instrument_id).values()
        }
        if order_ids.isdisjoint(still_resting):
            return True
        time.sleep(CANCEL_CONFIRM_POLL_SECONDS)

    log.warning(
        "Cancel not confirmed for %s within %.1fs (order_ids=%s) -- skipping insert this loop.",
        instrument_id, CANCEL_CONFIRM_TIMEOUT_SECONDS, order_ids,
    )
    return False


# ---------------------------------------------------------------------------
# Market making on futures -- requotes aggressively, one side at a time
# ---------------------------------------------------------------------------

def quote_future(exchange: Exchange, instrument_id: str, current_pos: int):
    fair = get_mid_price(exchange, instrument_id)
    if fair is None:
        return

    skew = (current_pos / POSITION_LIMIT) * QUOTE_OFFSET_TICKS * TICK_SIZE
    target_bid = round_to_tick(fair - QUOTE_OFFSET_TICKS * TICK_SIZE - skew)
    target_ask = round_to_tick(fair + QUOTE_OFFSET_TICKS * TICK_SIZE - skew)

    outstanding = exchange.get_outstanding_orders(instrument_id)
    # NOTE: previously this used next(...) to grab a single resting order
    # per side, which silently assumed at most one order per side could
    # ever be resting. If a delete hadn't fully landed on the exchange
    # before the next loop iteration inserted a fresh order, the orphaned
    # order became invisible to this logic -- it was never cancelled again,
    # and outstanding volume on the instrument crept up loop after loop
    # until it breached the exchange's pre-trade outstanding volume limit
    # and forced a disconnect. Collecting *all* orders per side and
    # cancelling every one of them (not just the first) closes that hole.
    resting_bids = [o for o in outstanding.values() if o.side == "bid"]
    resting_asks = [o for o in outstanding.values() if o.side == "ask"]

    min_move = REQUOTE_MIN_TICK_MOVE * TICK_SIZE

    # More than one resting order on a side is itself a sign of an orphan
    # from a prior loop -- treat that as stale too, so it gets swept up
    # and cancelled rather than accumulating further.
    bid_stale = (
        len(resting_bids) != 1
        or abs(resting_bids[0].price - target_bid) > min_move
    )
    ask_stale = (
        len(resting_asks) != 1
        or abs(resting_asks[0].price - target_ask) > min_move
    )

    bid_room = (POSITION_LIMIT - SAFETY_POSITION_BUFFER) - current_pos
    ask_room = (POSITION_LIMIT - SAFETY_POSITION_BUFFER) + current_pos

    bid_size = quote_size_for_room(bid_room)
    ask_size = quote_size_for_room(ask_room)

    # Cancel first, and do NOT insert until the cancel is actually confirmed
    # gone from get_outstanding_orders(). This is what closes the race: a
    # size margin alone isn't enough if the old order is still resting when
    # the new insert lands, because the old order's volume is still fully
    # counted against the exchange's pre-trade cap at that moment.
    if bid_stale:
        if not cancel_and_confirm(exchange, instrument_id, resting_bids):
            bid_size = 0  # couldn't confirm cancel -- don't risk stacking
        if bid_size > 0:
            # Recompute headroom from a fresh snapshot taken *after*
            # cancellation is confirmed, not from the stale `outstanding`
            # fetched at the top of this function.
            fresh = exchange.get_outstanding_orders(instrument_id)
            other_side_volume = outstanding_volume(
                [o for o in fresh.values() if o.side == "ask"])
            headroom = OUTSTANDING_VOLUME_LIMIT - OUTSTANDING_VOLUME_SAFETY_MARGIN - other_side_volume
            bid_size = max(0, min(bid_size, headroom))
            if bid_size > 0:
                rate_limited_insert(exchange, instrument_id, price=target_bid,
                                     volume=bid_size, side="bid", order_type="limit")

    if ask_stale:
        if not cancel_and_confirm(exchange, instrument_id, resting_asks):
            ask_size = 0
        if ask_size > 0:
            fresh = exchange.get_outstanding_orders(instrument_id)
            other_side_volume = outstanding_volume(
                [o for o in fresh.values() if o.side == "bid"])
            headroom = OUTSTANDING_VOLUME_LIMIT - OUTSTANDING_VOLUME_SAFETY_MARGIN - other_side_volume
            ask_size = max(0, min(ask_size, headroom))
            if ask_size > 0:
                rate_limited_insert(exchange, instrument_id, price=target_ask,
                                     volume=ask_size, side="ask", order_type="limit")


# ---------------------------------------------------------------------------
# Cash-delta hedging
# ---------------------------------------------------------------------------

def hedge_cash_delta(exchange: Exchange, cash_delta: float, hedge_pos: int):
    if abs(cash_delta) <= CASH_DELTA_HEDGE_THRESHOLD:
        return

    hedge_price = get_mid_price(exchange, HEDGE_FUTURE)
    if hedge_price is None:
        log.warning("No price available for hedge future %s", HEDGE_FUTURE)
        return

    dollars_to_hedge = cash_delta * HEDGE_AGGRESSIVENESS
    volume = int(round(abs(dollars_to_hedge) / hedge_price))
    if volume <= 0:
        return

    # HEDGE_FUTURE is one of the instruments we're also market-making
    # (FUTURES[0]), so a hedge order here competes for the SAME per-
    # instrument outstanding-volume budget as our resting quotes. The
    # exchange's pre-trade check counts a submitted order's volume against
    # that cap even for IOC orders that will never actually rest -- a large
    # hedge on top of ~80 already-resting quote volume can breach the cap
    # on its own, which is what forced the disconnect. Clip the hedge to
    # whatever headroom is actually left, and let the remaining cash delta
    # get hedged on a later loop iteration instead of in one shot.
    resting_volume = outstanding_volume(
        exchange.get_outstanding_orders(HEDGE_FUTURE).values())
    headroom = OUTSTANDING_VOLUME_LIMIT - OUTSTANDING_VOLUME_SAFETY_MARGIN - resting_volume
    if headroom <= 0:
        log.warning(
            "No outstanding-volume headroom on %s to hedge (resting=%d) -- "
            "deferring hedge to next loop.", HEDGE_FUTURE, resting_volume,
        )
        return
    if volume > headroom:
        log.info(
            "Hedge volume %d on %s clipped to %d by outstanding-volume headroom "
            "(resting=%d); remainder will be hedged over subsequent loops.",
            volume, HEDGE_FUTURE, headroom, resting_volume,
        )
        volume = headroom

    book = exchange.get_last_price_book(HEDGE_FUTURE)

    if cash_delta > 0:
        volume = min(volume, POSITION_LIMIT - SAFETY_POSITION_BUFFER + hedge_pos)
        if volume <= 0:
            return
        price = book.bids[0].price if book.bids else hedge_price

        # Selling aggressively at the best bid can match our OWN resting bid
        # on this instrument (HEDGE_FUTURE == one of our quoted futures),
        # which the exchange rejects as a self-trade -- and that rejection
        # is what forced the disconnect. Clear our own bid first and
        # confirm it's gone before crossing the book.
        own_bids = [o for o in exchange.get_outstanding_orders(HEDGE_FUTURE).values()
                    if o.side == "bid"]
        if own_bids and not cancel_and_confirm(exchange, HEDGE_FUTURE, own_bids):
            log.warning(
                "Could not confirm cancel of own resting bid(s) on %s -- "
                "skipping hedge this loop to avoid a self-trade.", HEDGE_FUTURE,
            )
            return

        rate_limited_insert(exchange, HEDGE_FUTURE, price=price, volume=volume,
                             side="ask", order_type="ioc")
        log.info("Hedged +%.2f cash delta by selling %d %s", cash_delta, volume, HEDGE_FUTURE)
    else:
        volume = min(volume, POSITION_LIMIT - SAFETY_POSITION_BUFFER - hedge_pos)
        if volume <= 0:
            return
        price = book.asks[0].price if book.asks else hedge_price

        # Symmetric case: buying aggressively at the best ask can match our
        # own resting ask on this instrument.
        own_asks = [o for o in exchange.get_outstanding_orders(HEDGE_FUTURE).values()
                    if o.side == "ask"]
        if own_asks and not cancel_and_confirm(exchange, HEDGE_FUTURE, own_asks):
            log.warning(
                "Could not confirm cancel of own resting ask(s) on %s -- "
                "skipping hedge this loop to avoid a self-trade.", HEDGE_FUTURE,
            )
            return

        rate_limited_insert(exchange, HEDGE_FUTURE, price=price, volume=volume,
                             side="bid", order_type="ioc")
        log.info("Hedged %.2f cash delta by buying %d %s", cash_delta, volume, HEDGE_FUTURE)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def trading_loop(exchange: Exchange, connected_at: float):
    while True:
        positions = exchange.get_positions()  # single call, reused all iteration

        for instrument_id in FUTURES:
            try:
                quote_future(exchange, instrument_id, positions.get(instrument_id, 0))
            except Exception as exc:
                log.warning("Quoting failed for %s: %s", instrument_id, exc)
                # Once we've been disconnected (rate-limit kick, network
                # blip, etc.) every further call this iteration will fail
                # the same way -- stop spamming and let main()'s reconnect
                # loop take over instead of burning through all 4 instruments.
                if "Cannot call function until connected" in str(exc):
                    raise

        cash_delta, breakdown = compute_cash_delta(positions, exchange, FUTURES)
        log.info("Cash delta: %.2f | breakdown: %s", cash_delta, breakdown)

        try:
            hedge_cash_delta(exchange, cash_delta, positions.get(HEDGE_FUTURE, 0))
        except Exception as exc:
            log.warning("Hedging failed: %s", exc)

        try:
            #EVAL
            pnl = exchange.get_pnl()
            log.info("Current PnL: %.2f", pnl)
            EVAL.record(pnl)
            line = EVAL.maybe_report()
            if line:
                log.info(line)
        except Exception:
            pass
        #EVAL
        log.info("\n%s", EVAL.format_summary())
        EVAL.write_summary()
        EVAL.close()

        # Once we've been up for a while without error, treat the connection
        # as healthy -- this is checked by main(), not reset here.
        time.sleep(LOOP_SLEEP_SECONDS)


def main():
    attempts = 0

    while attempts < MAX_RECONNECT_ATTEMPTS:
        exchange = Exchange()
        connected_at = None
        try:
            exchange.connect()
            connected_at = time.monotonic()
            log.info("Connected to exchange (attempt %d).", attempts + 1)
            trading_loop(exchange, connected_at)

        except KeyboardInterrupt:
            log.info("Interrupted by user, shutting down, cancelling all outstanding orders...")
            for inst in FUTURES:
                cancel_all_orders(exchange, inst)
            exchange.disconnect()
            return

        except Exception as e:
            uptime = (time.monotonic() - connected_at) if connected_at else 0
            # Only count this as a "real" failed attempt if the connection
            # didn't stay up for a meaningful amount of time -- a drop after
            # a long stable run shouldn't blow the backoff back to minimum
            # on the very next try, but it also shouldn't be treated as the
            # same kind of failure as an immediate reject.
            if uptime >= STABLE_CONNECTION_SECONDS:
                attempts = 1
            else:
                attempts += 1

            backoff = min(
                MAX_RECONNECT_BACKOFF_SECONDS,
                BASE_RECONNECT_BACKOFF_SECONDS * (2 ** (attempts - 1)),
            )
            backoff += random.uniform(0, backoff * 0.25)  # jitter to avoid thundering herd

            log.error(
                "Lost connection or hit an error (%s: %s) after %.1fs uptime. "
                "Reconnect attempt %d/%d in %.1fs...",
                type(e).__name__, e, uptime, attempts, MAX_RECONNECT_ATTEMPTS, backoff,
            )
            try:
                exchange.disconnect()
            except Exception:
                pass
            time.sleep(backoff)

    log.error("Max reconnect attempts (%d) reached, giving up.", MAX_RECONNECT_ATTEMPTS)


if __name__ == "__main__":
    main()