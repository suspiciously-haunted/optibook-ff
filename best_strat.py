# futures mm, all stocks mm, can control which strat to use
import time
import random
import logging
import math
import datetime

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
AMZN = "AMZN"
JPM = "JPM"
ETF = "OB5X_ETF"
PRIMARY_STOCKS = ["NVDA", "NVO"]
DUAL_PAIRS = {
    "NVDA_DUAL": "NVDA",
    "NVO_DUAL": "NVO",
}
FUTURE_EXPIRIES = {
    "OB5X_202612_F": datetime.datetime(2026, 12, 18, 12),
    "OB5X_202703_F": datetime.datetime(2027, 3, 19, 12),
    "OB5X_202706_F": datetime.datetime(2027, 6, 18, 12),
}
RISK_FREE_RATE = 0.03

ENABLE = {
    "FUTURES_MM": True,
    "XOMMM": True,
    "ETFARB": True,
    "DUALARB": True,
    "PAIR_STOCKMM": True,
    "AMZNMM": True,
    "JPMMM": False,
}

TICK_SIZE = 0.10
QUOTE_OFFSET_TICKS = 2

BASE_QUOTE_SIZE = 1
MAX_QUOTE_SIZE = 60

REQUOTE_MIN_TICK_MOVE = 1

CASH_DELTA_HEDGE_THRESHOLD = 100
HEDGE_AGGRESSIVENESS = 0.25

POSITION_LIMIT = 500
SAFETY_POSITION_BUFFER = 50

OUTSTANDING_VOLUME_LIMIT = 100
OUTSTANDING_VOLUME_SAFETY_MARGIN = 40

MAX_UPDATES_PER_SEC = 25
RATE_SAFETY_MARGIN = 0.8  # effective cap = MAX_UPDATES_PER_SEC * this
LOOP_SLEEP_SECONDS = 0.15

BASE_RECONNECT_BACKOFF_SECONDS = 0.5
MAX_RECONNECT_BACKOFF_SECONDS = 3
STABLE_CONNECTION_SECONDS = 30 

LIQUID_STOCK_MM_CONFIG = {
    AMZN: {
        "max_spread": 0.20,
        "near_offset_ticks": 0,
        "deep_offset_ticks": 1,
        "near_size": 14,
        "deep_size": 8,
        "max_abs_position": 170,
        "add_brake": 100,
        "hard_brake": 155,
        "requote_seconds": 0.50,
        "obi_pull": 0.70,
        "skew_ticks_per_100": 1.10,
    },
    JPM: {
        "max_spread": 0.30,
        "near_offset_ticks": 0,
        "deep_offset_ticks": 1,
        "near_size": 10,
        "deep_size": 6,
        "max_abs_position": 140,
        "add_brake": 85,
        "hard_brake": 125,
        "requote_seconds": 0.60,
        "obi_pull": 0.65,
        "skew_ticks_per_100": 1.25,
    },
    XOM: {
        "max_spread": 0.20,
        "near_offset_ticks": 0,
        "deep_offset_ticks": 1,
        "near_size": 20,
        "deep_size": 10,
        "max_abs_position": 220,
        "add_brake": 140,
        "hard_brake": 200,
        "requote_seconds": 0.45,
        "obi_pull": 0.60,
        "skew_ticks_per_100": 1.00,
    },
}
liquid_stock_last_requote = {instrument_id: 0.0 for instrument_id in LIQUID_STOCK_MM_CONFIG}

ETF_ARB_RATIO = 4
ETF_ARB_MIN_EDGE = 0.85
ETF_ARB_CLIP = 6
ETF_ARB_MAX_ETF_POS = 240
ETF_ARB_MAX_FUT_POS = 140
ETF_ARB_MAX_ETF_SPREAD = 0.40
ETF_ARB_MAX_FUT_SPREAD = 0.50
ETF_ARB_COOLDOWN_SECONDS = 0.25

DUAL_ARB_MIN_EDGE = {
    "NVDA_DUAL": 0.70,
    "NVO_DUAL": 0.60,
}
DUAL_ARB_BASE_CLIP = {
    "NVDA_DUAL": 12,
    "NVO_DUAL": 12,
}
DUAL_ARB_MAX_CLIP = {
    "NVDA_DUAL": 30,
    "NVO_DUAL": 30,
}
DUAL_ARB_HIGH_EDGE = {
    "NVDA_DUAL": 1.50,
    "NVO_DUAL": 1.50,
}
DUAL_ARB_MAX_PAIR_POS = {
    "NVDA_DUAL": 260,
    "NVO_DUAL": 260,
}
DUAL_ARB_MAX_DUAL_SPREAD = {
    "NVDA_DUAL": 0.80,
    "NVO_DUAL": 0.80,
}
DUAL_ARB_MAX_PRIMARY_SPREAD = {
    "NVDA_DUAL": 0.30,
    "NVO_DUAL": 0.30,
}
DUAL_ARB_COOLDOWN_SECONDS = 0.20

PAIR_STOCK_MM_INSTRUMENTS = ["NVDA", "NVO"]
PAIR_STOCK_MM_MAX_SPREAD = {
    "NVDA": 0.20,
    "NVO": 0.20,
}
PAIR_STOCK_MM_SIZE = {
    "NVDA": 10,
    "NVO": 12,
}
PAIR_STOCK_MM_MAX_ABS_POSITION = {
    "NVDA": 160,
    "NVO": 180,
}
PAIR_STOCK_MM_ADD_BRAKE = {
    "NVDA": 90,
    "NVO": 110,
}
PAIR_STOCK_MM_HARD_BRAKE = {
    "NVDA": 145,
    "NVO": 165,
}
PAIR_STOCK_MM_REQUOTE_SECONDS = 0.55
PAIR_STOCK_MM_OBI_PULL = 0.65
PAIR_STOCK_MM_SKEW_TICKS_PER_100 = 1.0

last_etf_arb_time = 0.0
last_dual_arb_time = {dual_id: 0.0 for dual_id in DUAL_PAIRS}
pair_stock_last_requote = {instrument_id: 0.0 for instrument_id in PAIR_STOCK_MM_INSTRUMENTS}


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


def liquid_stock_quote_plan(exchange: Exchange, instrument_id: str, current_pos: int):
    cfg = LIQUID_STOCK_MM_CONFIG[instrument_id]
    top = get_top_of_book(exchange, instrument_id)
    if top is None:
        return []
    _, best_bid, best_bid_volume, best_ask, best_ask_volume = top
    spread = best_ask - best_bid
    if spread <= 0 or spread > cfg["max_spread"]:
        return []

    obi = (best_bid_volume - best_ask_volume) / max(best_bid_volume + best_ask_volume, 1)
    skew_ticks = int(round((current_pos / 100.0) * cfg["skew_ticks_per_100"]))
    if obi < -cfg["obi_pull"]:
        skew_ticks += 1
    elif obi > cfg["obi_pull"]:
        skew_ticks -= 1

    near_bid = round_to_tick(best_bid - (cfg["near_offset_ticks"] + max(skew_ticks, 0)) * TICK_SIZE)
    near_ask = round_to_tick(best_ask + (cfg["near_offset_ticks"] + max(-skew_ticks, 0)) * TICK_SIZE)
    deep_bid = round_to_tick(best_bid - (cfg["deep_offset_ticks"] + max(skew_ticks, 0)) * TICK_SIZE)
    deep_ask = round_to_tick(best_ask + (cfg["deep_offset_ticks"] + max(-skew_ticks, 0)) * TICK_SIZE)

    max_abs = cfg["max_abs_position"]
    buy_room = min(max_abs - current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER - current_pos)
    sell_room = min(max_abs + current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER + current_pos)

    near_bid_size = cfg["near_size"]
    near_ask_size = cfg["near_size"]
    deep_bid_size = cfg["deep_size"]
    deep_ask_size = cfg["deep_size"]

    if current_pos > cfg["hard_brake"]:
        near_bid_size = deep_bid_size = 0
    elif current_pos > cfg["add_brake"]:
        near_bid_size = max(1, near_bid_size // 3)
        deep_bid_size = max(1, deep_bid_size // 3)
    if current_pos < -cfg["hard_brake"]:
        near_ask_size = deep_ask_size = 0
    elif current_pos < -cfg["add_brake"]:
        near_ask_size = max(1, near_ask_size // 3)
        deep_ask_size = max(1, deep_ask_size // 3)

    plan = []
    if buy_room > 0 and near_bid_size > 0 and near_bid < best_ask:
        plan.append(("bid", near_bid, min(near_bid_size, buy_room)))
    if sell_room > 0 and near_ask_size > 0 and near_ask > best_bid:
        plan.append(("ask", near_ask, min(near_ask_size, sell_room)))
    if buy_room > near_bid_size and deep_bid_size > 0 and deep_bid < near_bid:
        plan.append(("bid", deep_bid, min(deep_bid_size, buy_room - near_bid_size)))
    if sell_room > near_ask_size and deep_ask_size > 0 and deep_ask > near_ask:
        plan.append(("ask", deep_ask, min(deep_ask_size, sell_room - near_ask_size)))
    return [(side, price, int(volume)) for side, price, volume in plan if int(volume) > 0]


def quote_liquid_stock_mm(exchange: Exchange, instrument_id: str, current_pos: int):
    now = time.monotonic()
    cfg = LIQUID_STOCK_MM_CONFIG[instrument_id]
    outstanding = exchange.get_outstanding_orders(instrument_id)
    existing = sorted((o.side, round_to_tick(o.price), int(o.volume)) for o in outstanding.values())
    plan = sorted(liquid_stock_quote_plan(exchange, instrument_id, current_pos))

    if not plan:
        if outstanding:
            cancel_all_orders(exchange, instrument_id)
        liquid_stock_last_requote[instrument_id] = now
        return

    if existing == plan and now - liquid_stock_last_requote[instrument_id] < cfg["requote_seconds"]:
        return
    if now - liquid_stock_last_requote[instrument_id] < cfg["requote_seconds"] and existing:
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
        rate_limited_insert(
            exchange,
            instrument_id,
            price=price,
            volume=volume,
            side=side,
            order_type="limit",
        )
        used_volume += volume
    liquid_stock_last_requote[instrument_id] = now


def pair_stock_quote_plan(exchange: Exchange, instrument_id: str, current_pos: int):
    top = get_top_of_book(exchange, instrument_id)
    if top is None:
        return []
    _, best_bid, best_bid_volume, best_ask, best_ask_volume = top
    spread = best_ask - best_bid
    if spread <= 0 or spread > PAIR_STOCK_MM_MAX_SPREAD[instrument_id]:
        return []

    obi = (best_bid_volume - best_ask_volume) / max(best_bid_volume + best_ask_volume, 1)
    skew_ticks = int(round((current_pos / 100.0) * PAIR_STOCK_MM_SKEW_TICKS_PER_100))
    if obi < -PAIR_STOCK_MM_OBI_PULL:
        skew_ticks += 1
    elif obi > PAIR_STOCK_MM_OBI_PULL:
        skew_ticks -= 1

    bid_price = round_to_tick(best_bid - max(skew_ticks, 0) * TICK_SIZE)
    ask_price = round_to_tick(best_ask + max(-skew_ticks, 0) * TICK_SIZE)

    max_abs = PAIR_STOCK_MM_MAX_ABS_POSITION[instrument_id]
    buy_room = min(max_abs - current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER - current_pos)
    sell_room = min(max_abs + current_pos, POSITION_LIMIT - SAFETY_POSITION_BUFFER + current_pos)

    bid_size = PAIR_STOCK_MM_SIZE[instrument_id]
    ask_size = PAIR_STOCK_MM_SIZE[instrument_id]
    add_brake = PAIR_STOCK_MM_ADD_BRAKE[instrument_id]
    hard_brake = PAIR_STOCK_MM_HARD_BRAKE[instrument_id]

    if current_pos > hard_brake:
        bid_size = 0
    elif current_pos > add_brake:
        bid_size = max(1, bid_size // 3)
    if current_pos < -hard_brake:
        ask_size = 0
    elif current_pos < -add_brake:
        ask_size = max(1, ask_size // 3)

    plan = []
    if buy_room > 0 and bid_size > 0 and bid_price < best_ask:
        plan.append(("bid", bid_price, min(bid_size, buy_room)))
    if sell_room > 0 and ask_size > 0 and ask_price > best_bid:
        plan.append(("ask", ask_price, min(ask_size, sell_room)))
    return [(side, price, int(volume)) for side, price, volume in plan if int(volume) > 0]


def quote_pair_stock_mm(exchange: Exchange, instrument_id: str, current_pos: int):
    now = time.monotonic()
    outstanding = exchange.get_outstanding_orders(instrument_id)
    existing = sorted((o.side, round_to_tick(o.price), int(o.volume)) for o in outstanding.values())
    plan = sorted(pair_stock_quote_plan(exchange, instrument_id, current_pos))

    if not plan:
        if outstanding:
            cancel_all_orders(exchange, instrument_id)
        pair_stock_last_requote[instrument_id] = now
        return

    if existing == plan and now - pair_stock_last_requote[instrument_id] < PAIR_STOCK_MM_REQUOTE_SECONDS:
        return
    if now - pair_stock_last_requote[instrument_id] < PAIR_STOCK_MM_REQUOTE_SECONDS and existing:
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
        rate_limited_insert(
            exchange,
            instrument_id,
            price=price,
            volume=volume,
            side=side,
            order_type="limit",
        )
        used_volume += volume
    pair_stock_last_requote[instrument_id] = now


def position_room(positions, instrument_id: str, side: str, max_abs_position: int) -> int:
    current_pos = int(positions.get(instrument_id, 0))
    if side == "bid":
        return max(0, min(POSITION_LIMIT - SAFETY_POSITION_BUFFER, max_abs_position) - current_pos)
    return max(0, min(POSITION_LIMIT - SAFETY_POSITION_BUFFER, max_abs_position) + current_pos)


def cancel_side(exchange: Exchange, instrument_id: str, side: str) -> bool:
    outstanding = exchange.get_outstanding_orders(instrument_id)
    side_orders = [order for order in outstanding.values() if order.side == side]
    if not side_orders:
        return True
    return cancel_and_confirm(exchange, instrument_id, side_orders)


def submit_ioc(exchange: Exchange, instrument_id: str, side: str, price: float, volume: int):
    volume = int(volume)
    if volume <= 0:
        return
    rate_limited_insert(
        exchange,
        instrument_id,
        price=round_to_tick(price),
        volume=volume,
        side=side,
        order_type="ioc",
    )


def future_discount_factor(future_id: str) -> float:
    expiry = FUTURE_EXPIRIES[future_id]
    tau = max((expiry - datetime.datetime.utcnow()).total_seconds(), 0) / (365.25 * 86400)
    return math.exp(RISK_FREE_RATE * tau)


def dual_arb_clip_for_edge(dual_id: str, edge: float) -> int:
    base_clip = DUAL_ARB_BASE_CLIP[dual_id]
    if edge >= DUAL_ARB_HIGH_EDGE[dual_id]:
        return DUAL_ARB_MAX_CLIP[dual_id]
    return base_clip


def best_etf_arb(exchange: Exchange, positions):
    etf_top = get_top_of_book(exchange, ETF)
    if etf_top is None:
        return None
    _, etf_bid, etf_bid_volume, etf_ask, etf_ask_volume = etf_top
    if etf_ask - etf_bid > ETF_ARB_MAX_ETF_SPREAD:
        return None

    best = None
    for future_id in FUTURES:
        future_top = get_top_of_book(exchange, future_id)
        if future_top is None:
            continue
        _, fut_bid, fut_bid_volume, fut_ask, fut_ask_volume = future_top
        if fut_ask - fut_bid > ETF_ARB_MAX_FUT_SPREAD:
            continue
        df = future_discount_factor(future_id)

        # ETF cheap: buy 4 ETF at ask, sell 1 future at bid.
        edge = fut_bid - ETF_ARB_RATIO * df * (etf_ask - 2.50)
        volume = min(
            ETF_ARB_CLIP,
            int(fut_bid_volume),
            int(etf_ask_volume) // ETF_ARB_RATIO,
            position_room(positions, future_id, "ask", ETF_ARB_MAX_FUT_POS),
            position_room(positions, ETF, "bid", ETF_ARB_MAX_ETF_POS) // ETF_ARB_RATIO,
        )
        if edge >= ETF_ARB_MIN_EDGE and volume > 0:
            candidate = (edge, "buy_etf_sell_future", future_id, etf_ask, fut_bid, int(volume))
            if best is None or candidate[0] > best[0]:
                best = candidate

        # ETF rich: sell 4 ETF at bid, buy 1 future at ask.
        edge = ETF_ARB_RATIO * df * (etf_bid - 2.50) - fut_ask
        volume = min(
            ETF_ARB_CLIP,
            int(fut_ask_volume),
            int(etf_bid_volume) // ETF_ARB_RATIO,
            position_room(positions, future_id, "bid", ETF_ARB_MAX_FUT_POS),
            position_room(positions, ETF, "ask", ETF_ARB_MAX_ETF_POS) // ETF_ARB_RATIO,
        )
        if edge >= ETF_ARB_MIN_EDGE and volume > 0:
            candidate = (edge, "sell_etf_buy_future", future_id, etf_bid, fut_ask, int(volume))
            if best is None or candidate[0] > best[0]:
                best = candidate
    return best


def run_etf_arb(exchange: Exchange, positions):
    global last_etf_arb_time
    now = time.monotonic()
    if now - last_etf_arb_time < ETF_ARB_COOLDOWN_SECONDS:
        return
    opportunity = best_etf_arb(exchange, positions)
    if opportunity is None:
        return

    edge, mode, future_id, etf_price, fut_price, volume = opportunity
    if mode == "buy_etf_sell_future":
        if not cancel_side(exchange, future_id, "bid"):
            return
        submit_ioc(exchange, future_id, "ask", fut_price, volume)
        submit_ioc(exchange, ETF, "bid", etf_price, volume * ETF_ARB_RATIO)
    else:
        if not cancel_side(exchange, future_id, "ask"):
            return
        submit_ioc(exchange, future_id, "bid", fut_price, volume)
        submit_ioc(exchange, ETF, "ask", etf_price, volume * ETF_ARB_RATIO)
    last_etf_arb_time = now
    log.info("ETFARB %s %s vol=%d edge=%.2f", mode, future_id, volume, edge)


def best_dual_arb(exchange: Exchange, positions):
    best = None
    for dual_id, primary_id in DUAL_PAIRS.items():
        dual_top = get_top_of_book(exchange, dual_id)
        primary_top = get_top_of_book(exchange, primary_id)
        if dual_top is None or primary_top is None:
            continue
        _, dual_bid, dual_bid_volume, dual_ask, dual_ask_volume = dual_top
        _, primary_bid, primary_bid_volume, primary_ask, primary_ask_volume = primary_top
        if dual_ask - dual_bid > DUAL_ARB_MAX_DUAL_SPREAD[dual_id]:
            continue
        if primary_ask - primary_bid > DUAL_ARB_MAX_PRIMARY_SPREAD[dual_id]:
            continue
        min_edge = DUAL_ARB_MIN_EDGE[dual_id]
        max_pair_pos = DUAL_ARB_MAX_PAIR_POS[dual_id]

        # Dual rich: sell dual at bid, buy primary at ask.
        edge = dual_bid - primary_ask
        clip = dual_arb_clip_for_edge(dual_id, edge)
        volume = min(
            clip,
            int(dual_bid_volume),
            int(primary_ask_volume),
            position_room(positions, dual_id, "ask", max_pair_pos),
            position_room(positions, primary_id, "bid", max_pair_pos),
        )
        if edge >= min_edge and volume > 0:
            candidate = (edge, "sell_dual_buy_primary", dual_id, primary_id, dual_bid, primary_ask, int(volume))
            if best is None or candidate[0] > best[0]:
                best = candidate

        # Dual cheap: buy dual at ask, sell primary at bid.
        edge = primary_bid - dual_ask
        clip = dual_arb_clip_for_edge(dual_id, edge)
        volume = min(
            clip,
            int(dual_ask_volume),
            int(primary_bid_volume),
            position_room(positions, dual_id, "bid", max_pair_pos),
            position_room(positions, primary_id, "ask", max_pair_pos),
        )
        if edge >= min_edge and volume > 0:
            candidate = (edge, "buy_dual_sell_primary", dual_id, primary_id, dual_ask, primary_bid, int(volume))
            if best is None or candidate[0] > best[0]:
                best = candidate
    return best


def run_dual_arb(exchange: Exchange, positions):
    opportunity = best_dual_arb(exchange, positions)
    if opportunity is None:
        return
    edge, mode, dual_id, primary_id, dual_price, primary_price, volume = opportunity
    now = time.monotonic()
    if now - last_dual_arb_time[dual_id] < DUAL_ARB_COOLDOWN_SECONDS:
        return

    if mode == "sell_dual_buy_primary":
        if not cancel_side(exchange, dual_id, "bid"):
            return
        if not cancel_side(exchange, primary_id, "ask"):
            return
        submit_ioc(exchange, dual_id, "ask", dual_price, volume)
        submit_ioc(exchange, primary_id, "bid", primary_price, volume)
    else:
        if not cancel_side(exchange, dual_id, "ask"):
            return
        if not cancel_side(exchange, primary_id, "bid"):
            return
        submit_ioc(exchange, dual_id, "bid", dual_price, volume)
        submit_ioc(exchange, primary_id, "ask", primary_price, volume)
    last_dual_arb_time[dual_id] = now
    log.info("DUALARB %s %s/%s vol=%d edge=%.2f", mode, dual_id, primary_id, volume, edge)


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

        if ENABLE.get("ETFARB", False):
            try:
                run_etf_arb(exchange, positions)
            except Exception as exc:
                log.warning("ETF arb failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while running ETFARB: {exc}") from exc

        if ENABLE.get("DUALARB", False):
            try:
                run_dual_arb(exchange, positions)
            except Exception as exc:
                log.warning("Dual arb failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while running DUALARB: {exc}") from exc

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
                quote_liquid_stock_mm(exchange, XOM, positions.get(XOM, 0))
            except Exception as exc:
                log.warning("XOM quoting failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while quoting XOM: {exc}") from exc

        if ENABLE.get("AMZNMM", False):
            try:
                quote_liquid_stock_mm(exchange, AMZN, positions.get(AMZN, 0))
            except Exception as exc:
                log.warning("AMZN quoting failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while quoting AMZN: {exc}") from exc

        if ENABLE.get("JPMMM", False):
            try:
                quote_liquid_stock_mm(exchange, JPM, positions.get(JPM, 0))
            except Exception as exc:
                log.warning("JPM quoting failed: %s", exc)
                if not is_connected(exchange):
                    raise ConnectionError(f"disconnected while quoting JPM: {exc}") from exc

        if ENABLE.get("PAIR_STOCKMM", False):
            for instrument_id in PAIR_STOCK_MM_INSTRUMENTS:
                try:
                    quote_pair_stock_mm(exchange, instrument_id, positions.get(instrument_id, 0))
                except Exception as exc:
                    log.warning("Pair stock quoting failed for %s: %s", instrument_id, exc)
                    if not is_connected(exchange):
                        raise ConnectionError(
                            f"disconnected while quoting pair stock {instrument_id}: {exc}"
                        ) from exc

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


def enabled_liquid_stock_ids():
    instruments = []
    if ENABLE.get("XOMMM", False):
        instruments.append(XOM)
    if ENABLE.get("AMZNMM", False):
        instruments.append(AMZN)
    if ENABLE.get("JPMMM", False):
        instruments.append(JPM)
    return instruments


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
            for inst in enabled_liquid_stock_ids():
                cancel_all_orders(exchange, inst)
                log.info("Cleared resting orders on %s after connect.", inst)
            for inst in [ETF] + list(DUAL_PAIRS) + list(DUAL_PAIRS.values()):
                try:
                    cancel_all_orders(exchange, inst)
                except Exception:
                    pass
            trading_loop(exchange, connected_at)

        except KeyboardInterrupt:
            log.info("Interrupted by user, shutting down, cancelling all outstanding orders...")
            try:
                if is_connected(exchange):
                    for inst in FUTURES:
                        cancel_all_orders(exchange, inst)
                    for inst in enabled_liquid_stock_ids():
                        cancel_all_orders(exchange, inst)
                    for inst in [ETF] + list(DUAL_PAIRS) + list(DUAL_PAIRS.values()):
                        try:
                            cancel_all_orders(exchange, inst)
                        except Exception:
                            pass
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
