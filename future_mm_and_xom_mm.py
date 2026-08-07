# Future MM and XOM MM

import time
import random
import logging

from optibook.synchronous_client import Exchange

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


#EVAL
from eval_bot import EvalBot
EVAL = EvalBot(report_every=30.0, label="combined")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FUTURES = ["OB5X_202612_F", "OB5X_202706_F", "OB5X_202703_F"]
HEDGE_FUTURE = FUTURES[0]
XOM = "XOM"

ENABLE = {
    "FUTURES_MM": True,
    "XOMMM": True,
}

TICK_SIZE = 0.10
QUOTE_OFFSET_TICKS = 3

BASE_QUOTE_SIZE = 1
MAX_QUOTE_SIZE = 30

REQUOTE_MIN_TICK_MOVE = 1

CASH_DELTA_HEDGE_THRESHOLD = 100
HEDGE_AGGRESSIVENESS = 0.25

POSITION_LIMIT = 500
SAFETY_POSITION_BUFFER = 50

OUTSTANDING_VOLUME_LIMIT = 100
OUTSTANDING_VOLUME_SAFETY_MARGIN = 40

MAX_UPDATES_PER_SEC = 25
RATE_SAFETY_MARGIN = 0.8
LOOP_SLEEP_SECONDS = 0.15

# Reconnect: never give up mid-session. `attempts` only drives backoff
# magnitude and resets after a long stable run -- it is not a kill switch.
BASE_RECONNECT_BACKOFF_SECONDS = 0.5
MAX_RECONNECT_BACKOFF_SECONDS = 3
STABLE_CONNECTION_SECONDS = 30  # reset attempt counter only after this long uptime

# XOM module, adapted from amy-strategy.py's STOCKMM but retuned for the
# latest 0807 XOM market: 1-tick spread, very deep volume near 12.0, and
# good teams earning by repeatedly posting small, inventory-aware ladders.
XOM_MAX_SPREAD = 0.30
XOM_NEAR_OFFSET_TICKS = 1
XOM_DEEP_OFFSET_TICKS = 2
XOM_NEAR_SIZE = 18
XOM_DEEP_SIZE = 12
XOM_MAX_ABS_POSITION = 220
XOM_ADD_BRAKE = 140
XOM_HARD_BRAKE = 200
XOM_ANCHOR_EW = 0.04
XOM_SKEW_TICKS_PER_100 = 1.0
XOM_REQUOTE_SECONDS = 0.80
XOM_OBI_PULL = 0.55 

xom_anchor = None
xom_last_requote = 0.0


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

def is_connected(exchange: Exchange) -> bool:
    """Prefer the client probe over matching error-string substrings."""
    try:
        return bool(exchange.is_connected())
    except Exception:
        return False


def ensure_connected(exchange: Exchange):
    """Raise if the client reports disconnected so main()'s reconnect loop
    can take over without relying on a specific exception message."""
    if not is_connected(exchange):
        raise ConnectionError("exchange reports disconnected")


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


def clear_resting_futures(exchange: Exchange):
    """Cancel every resting order on all futures. Called once right after
    connect so orphans left by a prior rate-limit kick / disconnect cannot
    sit wrong-sided until the first quote_future pass notices them."""
    for instrument_id in FUTURES:
        try:
            cancel_all_orders(exchange, instrument_id)
            log.info("Cleared resting orders on %s after connect.", instrument_id)
        except Exception as exc:
            log.warning("Failed clearing resting orders on %s: %s", instrument_id, exc)
            if not is_connected(exchange):
                raise


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


def get_top_of_book(exchange: Exchange, instrument_id: str):
    book = exchange.get_last_price_book(instrument_id)
    if not book or not book.bids or not book.asks:
        return None
    return book, book.bids[0].price, book.bids[0].volume, book.asks[0].price, book.asks[0].volume


def xom_quote_plan(exchange: Exchange, current_pos: int):
    global xom_anchor
    top = get_top_of_book(exchange, XOM)
    if top is None:
        return []
    book, best_bid, best_bid_volume, best_ask, best_ask_volume = top
    spread = best_ask - best_bid
    if spread <= 0 or spread > XOM_MAX_SPREAD:
        return []

    mid = (best_bid + best_ask) / 2.0
    xom_anchor = mid if xom_anchor is None else (1 - XOM_ANCHOR_EW) * xom_anchor + XOM_ANCHOR_EW * mid

    obi = (best_bid_volume - best_ask_volume) / max(best_bid_volume + best_ask_volume, 1)
    skew_ticks = int(round((current_pos / 100.0) * XOM_SKEW_TICKS_PER_100))
    if obi < -XOM_OBI_PULL:
        skew_ticks += 1
    elif obi > XOM_OBI_PULL:
        skew_ticks -= 1

    bid_near = round_to_tick(best_bid - (XOM_NEAR_OFFSET_TICKS + max(skew_ticks, 0)) * TICK_SIZE)
    ask_near = round_to_tick(best_ask + (XOM_NEAR_OFFSET_TICKS + max(-skew_ticks, 0)) * TICK_SIZE)
    bid_deep = round_to_tick(best_bid - (XOM_DEEP_OFFSET_TICKS + max(skew_ticks, 0)) * TICK_SIZE)
    ask_deep = round_to_tick(best_ask + (XOM_DEEP_OFFSET_TICKS + max(-skew_ticks, 0)) * TICK_SIZE)

    plan = []
    buy_room = min(XOM_MAX_ABS_POSITION - current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER - current_pos)
    sell_room = min(XOM_MAX_ABS_POSITION + current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER + current_pos)

    near_bid_size = XOM_NEAR_SIZE
    deep_bid_size = XOM_DEEP_SIZE
    near_ask_size = XOM_NEAR_SIZE
    deep_ask_size = XOM_DEEP_SIZE
    if current_pos > XOM_HARD_BRAKE:
        near_bid_size = deep_bid_size = 0
    elif current_pos > XOM_ADD_BRAKE:
        near_bid_size = max(1, XOM_NEAR_SIZE // 3)
        deep_bid_size = max(1, XOM_DEEP_SIZE // 3)
    if current_pos < -XOM_HARD_BRAKE:
        near_ask_size = deep_ask_size = 0
    elif current_pos < -XOM_ADD_BRAKE:
        near_ask_size = max(1, XOM_NEAR_SIZE // 3)
        deep_ask_size = max(1, XOM_DEEP_SIZE // 3)

    if buy_room > 0 and bid_near < best_ask:
        plan.append(("bid", bid_near, min(near_bid_size, buy_room)))
    if sell_room > 0 and ask_near > best_bid:
        plan.append(("ask", ask_near, min(near_ask_size, sell_room)))
    if buy_room > near_bid_size and bid_deep < bid_near:
        plan.append(("bid", bid_deep, min(deep_bid_size, buy_room - near_bid_size)))
    if sell_room > near_ask_size and ask_deep > ask_near:
        plan.append(("ask", ask_deep, min(deep_ask_size, sell_room - near_ask_size)))
    return [(side, price, int(volume)) for side, price, volume in plan if int(volume) > 0]


def quote_xom_mm(exchange: Exchange, current_pos: int):
    global xom_last_requote
    now = time.monotonic()
    outstanding = exchange.get_outstanding_orders(XOM)
    existing = sorted((o.side, round_to_tick(o.price), int(o.volume)) for o in outstanding.values())
    plan = sorted(xom_quote_plan(exchange, current_pos))

    if not plan:
        if outstanding:
            cancel_all_orders(exchange, XOM)
        xom_last_requote = now
        return

    if existing == plan and now - xom_last_requote < XOM_REQUOTE_SECONDS:
        return
    if now - xom_last_requote < XOM_REQUOTE_SECONDS and existing:
        return

    if outstanding:
        if not cancel_and_confirm(exchange, XOM, list(outstanding.values())):
            return

    used_volume = 0
    max_volume = OUTSTANDING_VOLUME_LIMIT - OUTSTANDING_VOLUME_SAFETY_MARGIN
    for side, price, volume in plan:
        headroom = max_volume - used_volume
        if headroom <= 0:
            break
        volume = min(volume, headroom)
        if volume <= 0:
            continue
        rate_limited_insert(exchange, XOM, price=price, volume=volume, side=side, order_type="limit")
        used_volume += volume
    xom_last_requote = now


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
        ensure_connected(exchange)
        positions = exchange.get_positions()  # single call, reused all iteration

        if ENABLE.get("FUTURES_MM", True):
            for instrument_id in FUTURES:
                try:
                    quote_future(exchange, instrument_id, positions.get(instrument_id, 0))
                except Exception as exc:
                    log.warning("Quoting failed for %s: %s", instrument_id, exc)
                    # Once disconnected, every further call this iteration will
                    # fail the same way -- stop spamming and let main()'s
                    # reconnect loop take over.
                    if not is_connected(exchange):
                        raise ConnectionError(
                            f"disconnected while quoting {instrument_id}: {exc}"
                        ) from exc

        if ENABLE.get("XOMMM", False):
            try:
                quote_xom_mm(exchange, positions.get(XOM, 0))
            except Exception as exc:
                log.warning("XOM quoting failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while quoting XOM: {exc}") from exc

        cash_delta, breakdown = compute_cash_delta(positions, exchange, FUTURES)
        log.info("Cash delta: %.2f | breakdown: %s", cash_delta, breakdown)

        try:
            hedge_cash_delta(exchange, cash_delta, positions.get(HEDGE_FUTURE, 0))
        except Exception as exc:
            log.warning("Hedging failed: %s", exc)
            if not is_connected(exchange):
                raise ConnectionError(f"disconnected while hedging: {exc}") from exc

        try:
            #EVAL
            pnl = exchange.get_pnl()
            log.info("Current PnL: %.2f", pnl)
            EVAL.record(pnl)
            line = EVAL.maybe_report()
            if line:
                log.info(line)
        except Exception:
            if not is_connected(exchange):
                raise ConnectionError("disconnected while reading pnl")

        # Once we've been up for a while without error, treat the connection
        # as healthy -- this is checked by main(), not reset here.
        time.sleep(LOOP_SLEEP_SECONDS)


def main():
    attempts = 0
    reconnect_n = 0

    # Never give up mid-session: loop forever until KeyboardInterrupt.
    # `attempts` only shapes backoff; a long stable run resets it.
    while True:
        exchange = Exchange()
        connected_at = None
        try:
            exchange.connect()
            connected_at = time.monotonic()
            reconnect_n += 1
            log.info(
                "Connected to exchange (session #%d, backoff_level=%d).",
                reconnect_n, attempts,
            )
            # Drop orphans left by a prior disconnect / rate-limit kick
            # before we resume quoting.
            clear_resting_futures(exchange)
            if ENABLE.get("XOMMM", False):
                cancel_all_orders(exchange, XOM)
                log.info("Cleared resting orders on %s after connect.", XOM)
            trading_loop(exchange, connected_at)

        except KeyboardInterrupt:
            log.info("Interrupted by user, shutting down, cancelling all outstanding orders...")
            try:
                if is_connected(exchange):
                    for inst in FUTURES:
                        cancel_all_orders(exchange, inst)
                    if ENABLE.get("XOMMM", False):
                        cancel_all_orders(exchange, XOM)
                    exchange.disconnect()
            except Exception:
                pass
            #EVAL
            log.info("\n%s", EVAL.format_summary())
            EVAL.write_summary()
            EVAL.close()
            return

        except Exception as e:
            uptime = (time.monotonic() - connected_at) if connected_at else 0
            # Only count this as a "real" failed attempt if the connection
            # didn't stay up for a meaningful amount of time -- a drop after
            # a long stable run shouldn't keep stacking backoff.
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
                "Reconnect #%d in %.1fs (backoff_level=%d)...",
                type(e).__name__, e, uptime, reconnect_n + 1, backoff, attempts,
            )
            try:
                exchange.disconnect()
            except Exception:
                pass
            time.sleep(backoff)


if __name__ == "__main__":
    main()
