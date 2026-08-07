# decide which orders to send based on edge
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
# Do NOT mid-MM the hedge future. Resting hedges live on 612; if quote_future
# also runs there it cancels the hedge every loop and forces IOC take-style
# prints — the opposite of J&G&W's maker-heavy tape.
QUOTE_FUTURES = [f for f in FUTURES if f != HEDGE_FUTURE]

XOM = "XOM"
AMZN = "AMZN"
JPM = "JPM"
STOCK_MM_INSTRUMENTS = [XOM, AMZN, JPM]

ENABLE = {
    "FUTURES_MM": True,
    "STOCKMM": True,  # XOM + AMZN resting ladders (was XOMMM)
}

TICK_SIZE = 0.10
QUOTE_OFFSET_TICKS = 2

BASE_QUOTE_SIZE = 5
MAX_QUOTE_SIZE = 50          # leave room so BOTH sides can rest under vol=100

REQUOTE_MIN_TICK_MOVE = 2  # was 1 — fewer delete+insert pairs per mid flicker

# Skip inserts whose edge vs fair is too thin, and skip requotes that do not
# improve edge enough to justify 2 updates (delete + insert). This is the
# main prevention for "Max requests / update amount" disconnects: reconnect
# only recovers after the exchange already kicked us.
FUTURES_MIN_EDGE = QUOTE_OFFSET_TICKS * TICK_SIZE * 0.5  # 0.15
STOCK_MIN_EDGE = 0.05
MIN_REQUOTE_EDGE_DELTA = TICK_SIZE  # must gain >= 1 tick of edge to requote

# Lot-based gate (cash_delta / mid). Passive rest-at-touch by default;
# IOC only on emergencies — J&G&W look active because they quote, not lift.
HEDGE_LOT_THRESHOLD = 25
HEDGE_EMERGENCY_MULT = 3.0
HEDGE_AGGRESSIVENESS = 0.6
HEDGE_PERSIST_LOOPS = 2
MIN_HEDGE_INTERVAL_S = 0.4

POSITION_LIMIT = 500
SAFETY_POSITION_BUFFER = 100

OUTSTANDING_VOLUME_LIMIT = 100
OUTSTANDING_VOLUME_SAFETY_MARGIN = 20  # was 40 — more room for resting size

MAX_UPDATES_PER_SEC = 25
RATE_SAFETY_MARGIN = 0.8
LOOP_SLEEP_SECONDS = 0.15

BASE_RECONNECT_BACKOFF_SECONDS = 1
MAX_RECONNECT_BACKOFF_SECONDS = 3
STABLE_CONNECTION_SECONDS = 30

STOCK_MAX_SPREAD = 0.50
STOCK_NEAR_OFFSET_TICKS = 0      # 0 = join best bid/ask
STOCK_DEEP_OFFSET_TICKS = 2
STOCK_JOIN_INSIDE_WHEN_WIDE = True
STOCK_NEAR_SIZE = 40
STOCK_DEEP_SIZE = 20
STOCK_REQUOTE_SECONDS = 0.70     # was 0.25 — stay under update budget
STOCK_ANCHOR_EW = 0.04
STOCK_SKEW_TICKS_PER_100 = 1.0
STOCK_OBI_PULL = 0.55
STOCK_LIMITS = {
    XOM:  {"max_abs": 220, "add_brake": 140, "hard_brake": 200},
    AMZN: {"max_abs": 220, "add_brake": 140, "hard_brake": 200},
    JPM: {"max_abs": 220, "add_brake": 140, "hard_brake": 200},
}

stock_anchor = {}
stock_last_requote = {}
_hedge_above_count = 0
_last_hedge_time = 0.0

# Legacy names kept so old toggles still work if referenced elsewhere.
XOM_MAX_ABS_POSITION = STOCK_LIMITS[XOM]["max_abs"]
XOM_REQUOTE_SECONDS = STOCK_REQUOTE_SECONDS
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

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def try_take(self) -> bool:
        """Non-blocking: False means skip this update rather than sleep-and-burst."""
        self._refill()
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True

    def take(self):
        """Blocking take — only for cancels that must land to avoid volume stack."""
        self._refill()
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


def quote_edge(side: str, price: float, fair: float) -> float:
    """Signed edge vs fair: positive = resting on the advantageous side of mid."""
    if side == "bid":
        return fair - price
    return price - fair


def is_order_worth_sending(
    side: str,
    price: float,
    fair: float,
    *,
    min_edge: float,
    resting_price: float = None,
    min_improve: float = MIN_REQUOTE_EDGE_DELTA,
) -> bool:
    """Return False when an insert (or requote) is not worth an update slot.

    - New quote must clear `min_edge` vs fair.
    - Replacing an existing quote must improve edge by at least `min_improve`
      (one delete + one insert = 2 updates toward the 25/s cap).
    Orphans / wrong-sided rests (negative old edge) always pass so we can fix them.
    """
    if fair is None or price is None:
        return False
    edge = quote_edge(side, price, fair)
    if edge + 1e-9 < min_edge:
        return False
    if resting_price is not None:
        old_edge = quote_edge(side, resting_price, fair)
        if old_edge >= 0 and edge + 1e-9 < old_edge + min_improve:
            return False
    return True


def rate_limited_insert(exchange: Exchange, instrument_id, **kwargs) -> bool:
    """Skip the insert (return False) when the local update budget is empty.
    Prefer dropping a quote over sleeping into a burst that trips 25/s."""
    if not order_rate_limiter.try_take():
        return False
    exchange.insert_order(instrument_id, **kwargs)
    return True


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

    # Edge gate: even if price moved a tick, skip the delete+insert unless
    # the new quote improves edge enough (or we have orphans to clean).
    if bid_stale and len(resting_bids) == 1:
        if not is_order_worth_sending(
            "bid", target_bid, fair,
            min_edge=FUTURES_MIN_EDGE,
            resting_price=resting_bids[0].price,
        ):
            bid_stale = False
    if ask_stale and len(resting_asks) == 1:
        if not is_order_worth_sending(
            "ask", target_ask, fair,
            min_edge=FUTURES_MIN_EDGE,
            resting_price=resting_asks[0].price,
        ):
            ask_stale = False

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
        if bid_size > 0 and is_order_worth_sending(
            "bid", target_bid, fair, min_edge=FUTURES_MIN_EDGE,
        ):
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
        if ask_size > 0 and is_order_worth_sending(
            "ask", target_ask, fair, min_edge=FUTURES_MIN_EDGE,
        ):
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


def stock_quote_plan(exchange: Exchange, instrument_id: str, current_pos: int):
    lim = STOCK_LIMITS.get(instrument_id)
    if lim is None:
        return []
    top = get_top_of_book(exchange, instrument_id)
    if top is None:
        return []
    book, best_bid, best_bid_volume, best_ask, best_ask_volume = top
    spread = best_ask - best_bid
    if spread <= 0 or spread > STOCK_MAX_SPREAD:
        return []

    mid = (best_bid + best_ask) / 2.0
    prev = stock_anchor.get(instrument_id)
    stock_anchor[instrument_id] = (
        mid if prev is None else (1 - STOCK_ANCHOR_EW) * prev + STOCK_ANCHOR_EW * mid
    )

    obi = (best_bid_volume - best_ask_volume) / max(best_bid_volume + best_ask_volume, 1)
    skew_ticks = int(round((current_pos / 100.0) * STOCK_SKEW_TICKS_PER_100))
    if obi < -STOCK_OBI_PULL:
        skew_ticks += 1
    elif obi > STOCK_OBI_PULL:
        skew_ticks -= 1

    near_off = STOCK_NEAR_OFFSET_TICKS
    if STOCK_JOIN_INSIDE_WHEN_WIDE and spread >= 3 * TICK_SIZE:
        near_off = -1  # improve 1 tick toward mid
    bid_near = round_to_tick(best_bid - (near_off + max(skew_ticks, 0)) * TICK_SIZE)
    ask_near = round_to_tick(best_ask + (near_off + max(-skew_ticks, 0)) * TICK_SIZE)
    bid_deep = round_to_tick(best_bid - (STOCK_DEEP_OFFSET_TICKS + max(skew_ticks, 0)) * TICK_SIZE)
    ask_deep = round_to_tick(best_ask + (STOCK_DEEP_OFFSET_TICKS + max(-skew_ticks, 0)) * TICK_SIZE)

    if bid_near >= best_ask:
        bid_near = round_to_tick(best_ask - TICK_SIZE)
    if ask_near <= best_bid:
        ask_near = round_to_tick(best_bid + TICK_SIZE)
    if bid_near >= ask_near:
        return []

    plan = []
    buy_room = min(lim["max_abs"] - current_pos,
                   POSITION_LIMIT - SAFETY_POSITION_BUFFER - current_pos)
    sell_room = min(lim["max_abs"] + current_pos,
                    POSITION_LIMIT - SAFETY_POSITION_BUFFER + current_pos)

    near_bid_size = STOCK_NEAR_SIZE
    deep_bid_size = STOCK_DEEP_SIZE
    near_ask_size = STOCK_NEAR_SIZE
    deep_ask_size = STOCK_DEEP_SIZE
    if current_pos > lim["hard_brake"]:
        near_bid_size = deep_bid_size = 0
    elif current_pos > lim["add_brake"]:
        near_bid_size = max(1, STOCK_NEAR_SIZE // 3)
        deep_bid_size = max(1, STOCK_DEEP_SIZE // 3)
    if current_pos < -lim["hard_brake"]:
        near_ask_size = deep_ask_size = 0
    elif current_pos < -lim["add_brake"]:
        near_ask_size = max(1, STOCK_NEAR_SIZE // 3)
        deep_ask_size = max(1, STOCK_DEEP_SIZE // 3)

    if buy_room > 0 and bid_near < best_ask:
        plan.append(("bid", bid_near, min(near_bid_size, buy_room)))
    if sell_room > 0 and ask_near > best_bid:
        plan.append(("ask", ask_near, min(near_ask_size, sell_room)))
    if buy_room > near_bid_size and bid_deep < bid_near:
        plan.append(("bid", bid_deep, min(deep_bid_size, buy_room - near_bid_size)))
    if sell_room > near_ask_size and ask_deep > ask_near:
        plan.append(("ask", ask_deep, min(deep_ask_size, sell_room - near_ask_size)))
    return [(side, price, int(volume)) for side, price, volume in plan if int(volume) > 0]


def quote_stock_mm(exchange: Exchange, instrument_id: str, current_pos: int):
    now = time.monotonic()
    outstanding = exchange.get_outstanding_orders(instrument_id)
    existing = sorted((o.side, round_to_tick(o.price), int(o.volume)) for o in outstanding.values())
    plan = sorted(stock_quote_plan(exchange, instrument_id, current_pos))
    last = stock_last_requote.get(instrument_id, 0.0)
    fair = get_mid_price(exchange, instrument_id)

    if not plan:
        if outstanding:
            cancel_all_orders(exchange, instrument_id)
        stock_last_requote[instrument_id] = now
        return

    # Drop thin-edge levels before deciding whether to refresh — avoids
    # burning a full cancel+reinsert cycle for quotes that won't clear min edge.
    if fair is not None:
        plan = [
            (side, price, vol) for side, price, vol in plan
            if is_order_worth_sending(side, price, fair, min_edge=STOCK_MIN_EDGE)
        ]
        if not plan:
            if outstanding:
                cancel_all_orders(exchange, instrument_id)
            stock_last_requote[instrument_id] = now
            return

    # Only skip if plan unchanged AND still inside throttle. Always refresh
    # when prices drifted so we stay competitive at the touch (maker activity).
    if existing == plan and now - last < STOCK_REQUOTE_SECONDS:
        return

    # Even when the plan differs, skip requote if no level improves edge enough
    # vs what we already rest (and throttle hasn't expired for a full refresh).
    if outstanding and fair is not None and now - last < STOCK_REQUOTE_SECONDS:
        by_side = {}
        for o in outstanding.values():
            by_side.setdefault(o.side, []).append(o.price)
        improved = False
        for side, price, _vol in plan:
            rests = by_side.get(side, [])
            if not rests:
                improved = True
                break
            if any(
                is_order_worth_sending(
                    side, price, fair,
                    min_edge=STOCK_MIN_EDGE,
                    resting_price=rp,
                )
                for rp in rests
            ):
                improved = True
                break
        if not improved:
            return

    if outstanding:
        if not cancel_and_confirm(exchange, instrument_id, list(outstanding.values())):
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
        if fair is not None and not is_order_worth_sending(
            side, price, fair, min_edge=STOCK_MIN_EDGE,
        ):
            continue
        if rate_limited_insert(exchange, instrument_id, price=price, volume=volume,
                               side=side, order_type="limit"):
            used_volume += volume
    stock_last_requote[instrument_id] = now


def xom_quote_plan(exchange: Exchange, current_pos: int):
    return stock_quote_plan(exchange, XOM, current_pos)


def quote_xom_mm(exchange: Exchange, current_pos: int):
    return quote_stock_mm(exchange, XOM, current_pos)


# ---------------------------------------------------------------------------
# Cash-delta hedging — passive (maker) by default, IOC only on spikes
# ---------------------------------------------------------------------------

def _resting_hedges(exchange: Exchange, side: str):
    return [o for o in exchange.get_outstanding_orders(HEDGE_FUTURE).values()
            if o.side == side]


def hedge_cash_delta(exchange: Exchange, cash_delta: float, hedge_pos: int):
    """Rest at the touch on 612 (maker). IOC only if delta is an emergency.

    This is the main reason J&G&W look more 'active quoting' on tape: their
    inventory management still prints as resting bids/asks, not lifts/hits.
    """
    global _hedge_above_count, _last_hedge_time

    hedge_price = get_mid_price(exchange, HEDGE_FUTURE)
    if hedge_price is None or hedge_price <= 0:
        log.warning("No price available for hedge future %s", HEDGE_FUTURE)
        return

    delta_lots = cash_delta / hedge_price
    book = exchange.get_last_price_book(HEDGE_FUTURE)

    if abs(delta_lots) < HEDGE_LOT_THRESHOLD:
        _hedge_above_count = 0
        for side in ("bid", "ask"):
            resting = _resting_hedges(exchange, side)
            if resting:
                cancel_and_confirm(exchange, HEDGE_FUTURE, resting)
        return

    _hedge_above_count += 1
    emergency = abs(delta_lots) >= HEDGE_LOT_THRESHOLD * HEDGE_EMERGENCY_MULT

    if not emergency:
        if _hedge_above_count < HEDGE_PERSIST_LOOPS:
            return
        if time.monotonic() - _last_hedge_time < MIN_HEDGE_INTERVAL_S:
            return

    volume = int(round(abs(delta_lots) * HEDGE_AGGRESSIVENESS))
    if volume <= 0:
        return
    if cash_delta > 0:
        volume = min(volume, POSITION_LIMIT - SAFETY_POSITION_BUFFER + hedge_pos)
        side = "ask"
        touch = book.asks[0].price if book and book.asks else round_to_tick(hedge_price)
        take_px = book.bids[0].price if book and book.bids else hedge_price
        clear_side = "bid"
    else:
        volume = min(volume, POSITION_LIMIT - SAFETY_POSITION_BUFFER - hedge_pos)
        side = "bid"
        touch = book.bids[0].price if book and book.bids else round_to_tick(hedge_price)
        take_px = book.asks[0].price if book and book.asks else hedge_price
        clear_side = "ask"
    if volume <= 0:
        return

    other = "bid" if side == "ask" else "ask"
    if _resting_hedges(exchange, other):
        if not cancel_and_confirm(exchange, HEDGE_FUTURE, _resting_hedges(exchange, other)):
            return

    same = _resting_hedges(exchange, side)
    resting_volume = outstanding_volume(
        exchange.get_outstanding_orders(HEDGE_FUTURE).values())
    headroom = (
        OUTSTANDING_VOLUME_LIMIT - OUTSTANDING_VOLUME_SAFETY_MARGIN
        - resting_volume + outstanding_volume(same)
    )
    if headroom <= 0:
        log.warning("No outstanding-volume headroom on %s to hedge.", HEDGE_FUTURE)
        return
    volume = min(volume, headroom)

    if emergency:
        if not cancel_and_confirm(exchange, HEDGE_FUTURE, _resting_hedges(exchange, clear_side)):
            return
        if same and not cancel_and_confirm(exchange, HEDGE_FUTURE, same):
            return
        rate_limited_insert(exchange, HEDGE_FUTURE, price=take_px, volume=volume,
                            side=side, order_type="ioc")
        log.info("EMERGENCY IOC hedge %.1f lots: %s %d %s @ %.2f",
                 delta_lots, side, volume, HEDGE_FUTURE, take_px)
        _last_hedge_time = time.monotonic()
        _hedge_above_count = 0
        return

    needs_refresh = (
        len(same) != 1
        or same[0].volume != volume
        or abs(same[0].price - touch) > TICK_SIZE * 0.5
    )
    if not needs_refresh:
        return
    if same and not cancel_and_confirm(exchange, HEDGE_FUTURE, same):
        return
    rate_limited_insert(exchange, HEDGE_FUTURE, price=touch, volume=volume,
                        side=side, order_type="limit")
    log.info("Passive hedge %.1f lots: rest %s %d %s @ %.2f",
             delta_lots, side, volume, HEDGE_FUTURE, touch)
    _last_hedge_time = time.monotonic()
    _hedge_above_count = 0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def trading_loop(exchange: Exchange, connected_at: float):
    while True:
        ensure_connected(exchange)
        positions = exchange.get_positions()  # single call, reused all iteration

        if ENABLE.get("FUTURES_MM", True):
            for instrument_id in QUOTE_FUTURES:
                try:
                    quote_future(exchange, instrument_id, positions.get(instrument_id, 0))
                except Exception as exc:
                    log.warning("Quoting failed for %s: %s", instrument_id, exc)
                    if not is_connected(exchange):
                        raise ConnectionError(
                            f"disconnected while quoting {instrument_id}: {exc}"
                        ) from exc

        if ENABLE.get("STOCKMM", False) or ENABLE.get("XOMMM", False):
            for iid in STOCK_MM_INSTRUMENTS:
                try:
                    quote_stock_mm(exchange, iid, positions.get(iid, 0))
                except Exception as exc:
                    log.warning("%s quoting failed: %s", iid, exc)
                    if not is_connected(exchange):
                        raise ConnectionError(
                            f"disconnected while quoting {iid}: {exc}"
                        ) from exc

        cash_delta, breakdown = compute_cash_delta(positions, exchange, FUTURES)
        hedge_mid = get_mid_price(exchange, HEDGE_FUTURE)
        delta_lots = (cash_delta / hedge_mid) if hedge_mid else 0.0
        log.info(
            "Cash delta: %.2f (%.1f lots) | %s | XOM=%s AMZN=%s",
            cash_delta, delta_lots, breakdown,
            positions.get(XOM, 0), positions.get(AMZN, 0),
        )

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
            if ENABLE.get("STOCKMM", False) or ENABLE.get("XOMMM", False):
                for iid in STOCK_MM_INSTRUMENTS:
                    cancel_all_orders(exchange, iid)
                    log.info("Cleared resting orders on %s after connect.", iid)
            log.info(
                "Maker mode: quote %s | passive hedge on %s | stocks %s",
                QUOTE_FUTURES, HEDGE_FUTURE, STOCK_MM_INSTRUMENTS,
            )
            trading_loop(exchange, connected_at)

        except KeyboardInterrupt:
            log.info("Interrupted by user, shutting down, cancelling all outstanding orders...")
            try:
                if is_connected(exchange):
                    for inst in FUTURES:
                        cancel_all_orders(exchange, inst)
                    if ENABLE.get("STOCKMM", False) or ENABLE.get("XOMMM", False):
                        for iid in STOCK_MM_INSTRUMENTS:
                            cancel_all_orders(exchange, iid)
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
