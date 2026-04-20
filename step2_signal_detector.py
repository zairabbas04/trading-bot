"""
STEP 2 OF 4 — Signal Detector
==============================
Receives every candle close from Step 1 (CandleEngine).
Checks all entry conditions for both strategies.
Emits SignalEvent objects when a valid trade setup is found.

STRATEGY 1 — EMA 9/26 Cross + 6 Filters  (LONG + SHORT)
  F1  EMA9 crosses EMA26
  F2  Candle confirms direction + closes beyond both EMAs
  F3  Close on correct side of EMA200
  F4  ADX(14) > 25
  F5  DI direction aligned
  F6  MACD line/signal/histogram aligned
  Entry: close of crossover candle (or N+1 if N fails F2-F6)
  One trade per crossover. New signals blocked while trade open.

STRATEGY 2 — MA44 Bounce  (SHORT ONLY, two-step)
  Step 1 candle (setup): bearish, MA44 falling 8 bars consecutively,
    body below MA44, F1-F8 pass
  Step 2 candle (trigger): next candle opens below MA44 → entry at open
  F1  body_ratio >= 0.60
  F2  dist zone A 0.20-0.35%  OR  zone B 0.50-0.65%
  F4  wick range 0.35-1.00%
  F5  slope 8-bar abs >= 0.10%  HARD REJECT
  F6  ma_accel: slope_recent < slope_prior < 0
  F7  ATR(14)/close < 0.60%
  F8  4H MA44 must be FALLING  (fetched via REST, cached per 4h bucket)
  F9  2 consecutive losses -> 8h pause (tracked here, enforced in Step 3)
  Cooldown: 4h between signals on same symbol

How it connects:
  from step2_signal_detector import SignalDetector, SignalEvent
  detector = SignalDetector()
  engine   = CandleEngine(SYMBOLS, callback=detector.on_candle_close)
  engine.start()

  # To receive signals, set a handler:
  detector.on_signal = my_handler   # called with (SignalEvent,)

Dependencies:
  requests  (already installed from Step 1)
"""

import sys
import io
import threading
import time
import requests
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable

# Fix Windows console encoding — must happen before any print or logging
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        # reconfigure not available on older Python — use wrapper
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

log = logging.getLogger('signal_detector')

# ============================================================================
# CONFIGURATION — must match backtest parameters exactly
# ============================================================================

# Strategy 1
S1_ADX_MIN        = 25.0
S1_SL_PCT         = 0.5
S1_TP_PCT         = 1.5

# Strategy 2
S2_SL_PCT         = 2.0
S2_TP_PCT         = 6.0
S2_COOLDOWN_SEC   = 4 * 3600          # 4 hours between S2 signals per symbol
S2_MIN_BODY_RATIO = 0.60
S2_DIST_A_MIN     = 0.0020            # 0.20%
S2_DIST_A_MAX     = 0.0035            # 0.35%
S2_DIST_B_MIN     = 0.0050            # 0.50%
S2_DIST_B_MAX     = 0.0065            # 0.65%
S2_MIN_WICK_PCT   = 0.0035            # 0.35%
S2_MAX_WICK_PCT   = 0.0100            # 1.00%
S2_MA_SLOPE_MIN   = 0.10              # abs 8-bar slope >= 0.10%
S2_ATR_MAX_PCT    = 0.60              # ATR% < 0.60%
S2_CONSEC_LOSS_MAX = 2                # losses before pause
S2_PAUSE_SEC      = 8 * 3600          # 8h pause after consec losses

# 4H MA44 direction gate (REST fetch) — futures endpoint for all symbols
H4_MA_PERIOD      = 44
H4_SLOPE_BARS     = 4
H4_CACHE_SEC      = 3600              # re-fetch 4H direction at most once per hour
REST_DATA_BASE    = "https://fapi.binance.com/fapi"

# ============================================================================
# SIGNAL EVENT — the object passed to the order manager
# ============================================================================

@dataclass
class SignalEvent:
    """
    Emitted when a valid trade setup is detected.
    Step 3 (OrderManager) receives this and places the trade.
    """
    strategy:    str          # 'S1_EMA_CROSS' or 'S2_MA44_BOUNCE'
    symbol:      str          # e.g. 'BTCUSDT'
    direction:   str          # 'LONG' or 'SHORT'
    entry_price: float        # suggested entry (close of signal candle for S1,
                              # open of trigger candle for S2)
    sl_price:    float        # stop loss price
    tp_price:    float        # take profit price
    signal_ts:   int          # candle close timestamp (ms)
    signal_time: str          # human-readable UTC string
    reason:      str          # short human description of why signal fired

    # Indicator snapshot at signal candle (for logging/audit)
    indicators:  dict = field(default_factory=dict)


# ============================================================================
# PER-SYMBOL STATE — tracked independently for each symbol
# ============================================================================

class SymbolState:
    """All mutable state for one symbol across both strategies."""

    def __init__(self, symbol: str):
        self.symbol = symbol

        # S1 state
        self.s1_trade_open      = False   # blocks new S1 signals while True
        self.s1_last_cross_dir  = None    # direction of last crossover (prevents re-use)
        self.s1_cross_candle_ts = None    # timestamp of crossover candle
        self.s1_pending_dir     = None    # crossover detected, waiting for N+1 confirm
        self.s1_pending_ts      = None    # ts of the crossover candle

        # S2 state
        self.s2_trade_open      = False
        self.s2_pending_setup   = False   # setup candle passed, waiting for trigger
        self.s2_setup_ts        = None    # timestamp of setup candle
        self.s2_setup_snap      = {}      # indicator snapshot at setup candle
        self.s2_last_signal_ts  = -99999  # for 4h cooldown; -99999 = never signalled
        self.s2_consec_losses   = 0       # consecutive loss counter
        self.s2_pause_until     = 0       # epoch seconds — no signals before this

    def s2_in_cooldown(self, now_sec: float) -> bool:
        if self.s2_last_signal_ts < 0:  # never signalled
            return False
        return now_sec < self.s2_last_signal_ts + S2_COOLDOWN_SEC

    def s2_in_pause(self, now_sec: float) -> bool:
        return now_sec < self.s2_pause_until

    def s2_record_loss(self, now_sec: float):
        self.s2_consec_losses += 1
        log.info(f"{self.symbol} S2: consecutive losses = {self.s2_consec_losses}")
        if self.s2_consec_losses >= S2_CONSEC_LOSS_MAX:
            self.s2_pause_until = now_sec + S2_PAUSE_SEC
            self.s2_consec_losses = 0
            resume = datetime.fromtimestamp(self.s2_pause_until, tz=timezone.utc).strftime('%H:%M UTC')
            log.warning(f"{self.symbol} S2: {S2_CONSEC_LOSS_MAX} consecutive losses "
                        f"-> 8h pause until {resume}")

    def s2_record_win(self):
        self.s2_consec_losses = 0


# ============================================================================
# 4H MA44 DIRECTION CACHE
# ============================================================================

class H4DirectionCache:
    """
    Fetches the 4H MA44 slope (rising/falling) for a symbol.
    Cached per (symbol, 4h_bucket) to avoid hammering the REST API.
    Thread-safe.
    """

    def __init__(self):
        self._lock  = threading.Lock()
        self._cache = {}   # key: (symbol, bucket_ts) -> (bool|None, fetched_at)

    def get(self, symbol: str, candle_ts_ms: int) -> Optional[bool]:
        """
        Returns True (rising), False (falling), or None (unavailable).
        candle_ts_ms: timestamp of the 15m signal candle in milliseconds.
        """
        bucket = (candle_ts_ms // (4 * 3600 * 1000)) * (4 * 3600 * 1000)
        key    = (symbol, bucket)
        now    = time.time()

        with self._lock:
            if key in self._cache:
                result, fetched_at = self._cache[key]
                if now - fetched_at < H4_CACHE_SEC:
                    return result

        result = self._fetch(symbol, candle_ts_ms)

        with self._lock:
            self._cache[key] = (result, now)

        return result

    def _fetch(self, symbol: str, end_ts_ms: int) -> Optional[bool]:
        try:
            resp = requests.get(
                f"{REST_DATA_BASE}/v1/klines",
                params={
                    'symbol':   symbol,
                    'interval': '4h',
                    'endTime':  end_ts_ms,
                    'limit':    H4_MA_PERIOD + H4_SLOPE_BARS + 5,
                },
                timeout=10
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, list) or len(data) < H4_MA_PERIOD + H4_SLOPE_BARS:
                return None
            closes = [float(c[4]) for c in data]
            ma_now  = sum(closes[-H4_MA_PERIOD:])              / H4_MA_PERIOD
            ma_prev = sum(closes[-H4_MA_PERIOD-H4_SLOPE_BARS:-H4_SLOPE_BARS]) / H4_MA_PERIOD
            return ma_now > ma_prev   # True = rising, False = falling
        except Exception as e:
            log.warning(f"H4 fetch failed for {symbol}: {e}")
            return None


# ============================================================================
# MA44 MONOTONIC SLOPE CHECK
# (Step 1 only gives the current slope value, not the per-bar series.
#  We need to verify MA44 fell on every one of the last 8 bars.
#  We do this using the candle store snapshot passed from the engine.)
# ============================================================================

def _check_ma44_monotonic_falling(candle_list: list, ma_period: int = 44, lookback: int = 8) -> bool:
    """
    Returns True if MA44 has fallen on every one of the last `lookback` bars.
    candle_list: list of candle dicts {'c': float, ...} ordered oldest→newest.
    """
    if len(candle_list) < ma_period + lookback:
        return False

    closes = [c['c'] for c in candle_list]
    n = len(closes)

    # Build the MA44 value for positions [n-lookback-1 .. n-1]
    ma_vals = []
    for offset in range(lookback + 1):
        idx = n - 1 - (lookback - offset)   # idx goes from n-1-lookback to n-1
        if idx < ma_period - 1:
            return False
        ma = sum(closes[idx - ma_period + 1: idx + 1]) / ma_period
        ma_vals.append(ma)

    # ma_vals[0] = oldest, ma_vals[-1] = newest
    # Must be strictly decreasing
    for i in range(1, len(ma_vals)):
        if ma_vals[i] >= ma_vals[i - 1]:
            return False
    return True


# ============================================================================
# SIGNAL DETECTOR — main class
# ============================================================================

class SignalDetector:
    """
    Plug this into CandleEngine as the callback.

        detector = SignalDetector()
        detector.on_signal = my_trade_handler
        engine = CandleEngine(SYMBOLS, callback=detector.on_candle_close)

    on_signal is called with a SignalEvent whenever conditions are met.
    It is called from the WebSocket message thread — keep it fast or
    dispatch to a queue (Step 3 does this).
    """

    def __init__(self):
        self._states  = {}                      # symbol -> SymbolState
        self._h4      = H4DirectionCache()
        self._lock    = threading.Lock()
        self.on_signal: Optional[Callable] = None
        # candle_list cache: updated by engine via set_candle_list()
        self._candle_lists = {}                 # symbol -> list[dict]

    # ── Called by CandleEngine to share candle history ────────────────────────
    def set_candle_list(self, symbol: str, candle_list: list):
        """CandleEngine calls this after each push so we have access to history."""
        with self._lock:
            self._candle_lists[symbol] = candle_list

    def _get_state(self, symbol: str) -> SymbolState:
        if symbol not in self._states:
            self._states[symbol] = SymbolState(symbol)
        return self._states[symbol]

    # ── Main entry point — called by CandleEngine on every closed candle ──────
    def on_candle_close(self, symbol: str, candle: dict, ind: dict):
        """
        symbol  : e.g. 'BTCUSDT'
        candle  : {'t': ms, 'o': float, 'h': float, 'l': float, 'c': float}
        ind     : output of compute_indicators() from Step 1
        """
        # Guard: skip if any critical indicator is None
        required = ['ema9', 'ema26', 'ema200', 'ema9_prev', 'ema26_prev',
                    'adx', 'di_plus', 'di_minus',
                    'macd', 'macd_sig', 'macd_hist',
                    'ma44', 'ma44_slope_8bar', 'ma44_accel', 'atr_pct']
        if any(ind.get(k) is None for k in required):
            return

        state    = self._get_state(symbol)
        now_sec  = candle['t'] / 1000.0

        self._check_s1(symbol, candle, ind, state)
        self._check_s2(symbol, candle, ind, state, now_sec)

    # ==========================================================================
    # STRATEGY 1 — EMA 9/26 Cross
    # ==========================================================================

    def _check_s1(self, symbol: str, candle: dict, ind: dict, state: SymbolState):
        # Block new signals while a trade is open on this symbol
        if state.s1_trade_open:
            return

        c_close = candle['c']
        c_open  = candle['o']
        ts      = candle['t']

        ema9       = ind['ema9'];      ema9_prev  = ind['ema9_prev']
        ema26      = ind['ema26'];     ema26_prev = ind['ema26_prev']
        ema200     = ind['ema200']
        adx        = ind['adx']
        di_plus    = ind['di_plus'];   di_minus   = ind['di_minus']
        macd       = ind['macd'];      macd_sig   = ind['macd_sig']
        macd_hist  = ind['macd_hist']

        # ── Detect crossover on THIS candle ───────────────────────────────────
        bullish_cross = (ema9_prev <= ema26_prev) and (ema9 > ema26)
        bearish_cross = (ema9_prev >= ema26_prev) and (ema9 < ema26)

        if bullish_cross or bearish_cross:
            direction = 'LONG' if bullish_cross else 'SHORT'
            # Store as pending — will attempt to confirm on this candle (N)
            # and next candle (N+1) if this one fails
            state.s1_pending_dir = direction
            state.s1_pending_ts  = ts
            log.debug(f"{symbol} S1: {direction} crossover detected at {_fmt_ts(ts)}")

        # ── Attempt to confirm a pending crossover (candle N or N+1) ─────────
        if state.s1_pending_dir is None:
            return

        direction = state.s1_pending_dir

        # Only try for 2 candles (N and N+1) — if this is candle N+2, cancel
        if ts > state.s1_pending_ts + 2 * 15 * 60 * 1000:
            log.debug(f"{symbol} S1: crossover expired (no confirmation in 2 candles)")
            state.s1_pending_dir = None
            state.s1_pending_ts  = None
            return

        # F2 — candle confirm
        if direction == 'LONG':
            f2 = (c_close > c_open) and (c_close > ema9) and (c_close > ema26)
        else:
            f2 = (c_close < c_open) and (c_close < ema9) and (c_close < ema26)

        if not f2:
            return   # try again next candle (N+1)

        # F3 — EMA200 trend gate
        if direction == 'LONG'  and c_close <= ema200: return
        if direction == 'SHORT' and c_close >= ema200: return

        # F4 — ADX strength
        if adx <= S1_ADX_MIN: return

        # F5 — DI direction
        if direction == 'LONG'  and not (di_plus > di_minus): return
        if direction == 'SHORT' and not (di_minus > di_plus): return

        # F6 — MACD momentum
        if direction == 'LONG'  and not (macd > macd_sig and macd_hist > 0): return
        if direction == 'SHORT' and not (macd < macd_sig and macd_hist < 0): return

        # ── All 6 filters passed ─────────────────────────────────────────────
        entry = c_close
        sl    = entry * (1 - S1_SL_PCT/100) if direction == 'LONG' else entry * (1 + S1_SL_PCT/100)
        tp    = entry * (1 + S1_TP_PCT/100) if direction == 'LONG' else entry * (1 - S1_TP_PCT/100)

        candle_label = 'N' if ts == state.s1_pending_ts else 'N+1'

        signal = SignalEvent(
            strategy    = 'S1_EMA_CROSS',
            symbol      = symbol,
            direction   = direction,
            entry_price = entry,
            sl_price    = sl,
            tp_price    = tp,
            signal_ts   = ts,
            signal_time = _fmt_ts(ts),
            reason      = (f"EMA{9}/{26} {direction} cross confirmed on candle {candle_label} | "
                           f"ADX={adx:.1f} DI+={di_plus:.1f} DI-={di_minus:.1f} | "
                           f"MACD={macd:.4f} Hist={macd_hist:.4f}"),
            indicators  = {
                'ema9': ema9, 'ema26': ema26, 'ema200': ema200,
                'adx': adx, 'di_plus': di_plus, 'di_minus': di_minus,
                'macd': macd, 'macd_sig': macd_sig, 'macd_hist': macd_hist,
                'candle_label': candle_label,
            }
        )

        # Mark crossover consumed — open trade gate
        state.s1_pending_dir = None
        state.s1_pending_ts  = None
        state.s1_trade_open  = True

        log.info(f"[SIGNAL] {symbol} S1 {direction} | entry={entry:.4f} "
                 f"SL={sl:.4f} TP={tp:.4f} | {signal.reason}")

        self._emit(signal)

    # ==========================================================================
    # STRATEGY 2 — MA44 Bounce (SHORT ONLY, two-step)
    # ==========================================================================

    def _check_s2(self, symbol: str, candle: dict, ind: dict,
                  state: SymbolState, now_sec: float):

        # ── STEP 2: check if we have a pending setup and this is the trigger ──
        if state.s2_pending_setup:
            self._try_s2_trigger(symbol, candle, ind, state, now_sec)
            # Always clear pending after one attempt (whether triggered or not)
            state.s2_pending_setup = False
            state.s2_setup_ts      = None
            state.s2_setup_snap    = {}
            return   # don't also check step 1 on same candle

        # ── Block new setups while trade open / paused / cooldown ────────────
        if state.s2_trade_open:
            return
        if state.s2_in_pause(now_sec):
            remaining = state.s2_pause_until - now_sec
            log.debug(f"{symbol} S2: in pause ({remaining/3600:.1f}h remaining)")
            return
        if state.s2_in_cooldown(now_sec):
            return

        # ── STEP 1: check setup candle ────────────────────────────────────────
        self._check_s2_setup(symbol, candle, ind, state, now_sec)

    def _check_s2_setup(self, symbol: str, candle: dict, ind: dict,
                        state: SymbolState, now_sec: float):
        c_close = candle['c']
        c_open  = candle['o']
        c_high  = candle['h']
        ts      = candle['t']

        # Must be bearish
        if c_close >= c_open:
            return

        ma44          = ind['ma44']
        slope_8bar    = ind['ma44_slope_8bar']   # (MA44[now] - MA44[-8]) / MA44[now] * 100
        ma44_accel    = ind['ma44_accel']        # slope_recent - slope_prior
        atr_pct       = ind['atr_pct']

        # ── F5: slope magnitude HARD REJECT ──────────────────────────────────
        if abs(slope_8bar) < S2_MA_SLOPE_MIN:
            return

        # ── F5: monotonic — MA44 must have fallen every bar for 8 bars ───────
        with self._lock:
            candle_list = self._candle_lists.get(symbol, [])
        if not _check_ma44_monotonic_falling(candle_list):
            return

        # ── F6: acceleration — slope_recent < slope_prior < 0 ────────────────
        # ma44_accel = slope_recent - slope_prior
        # We need both slopes negative AND recent steeper than prior.
        # slope_recent = MA44[now] - MA44[-4]
        # slope_prior  = MA44[-4]  - MA44[-8]
        # If slope_recent < slope_prior < 0 then accel < 0 and prior < 0
        # We can check via indicators: slope_8bar < 0 AND accel < 0
        # (accel = slope_recent - slope_prior; if both negative and recent steeper,
        #  slope_recent < slope_prior means accel = slope_recent - slope_prior < 0)
        if slope_8bar >= 0:     # MA44 not falling overall
            return
        if ma44_accel >= 0:     # not accelerating downward
            return

        # ── Candle geometry ───────────────────────────────────────────────────
        body_top    = max(c_open, c_close)
        body_bottom = min(c_open, c_close)
        candle_size = c_high - candle['l']
        body_size   = body_top - body_bottom
        upper_wick  = c_high - body_top

        if candle_size == 0:
            return

        wick_pct   = candle_size / c_high if c_high > 0 else 0
        body_ratio = body_size / candle_size

        # F1 — body ratio
        if body_ratio < S2_MIN_BODY_RATIO:
            return

        # F4 — wick range
        if wick_pct < S2_MIN_WICK_PCT or wick_pct > S2_MAX_WICK_PCT:
            return

        # Body must be strictly below MA44
        if body_top >= ma44:
            return

        # F2 — distance zone (A or B, reject middle band)
        dist_pct = (ma44 - body_top) / ma44
        in_zone_a = S2_DIST_A_MIN <= dist_pct <= S2_DIST_A_MAX
        in_zone_b = S2_DIST_B_MIN <= dist_pct <= S2_DIST_B_MAX
        if not (in_zone_a or in_zone_b):
            return

        # F7 — ATR%
        if atr_pct >= S2_ATR_MAX_PCT:
            return

        # F8 — 4H MA44 direction gate (must be falling for SHORT)
        h4_rising = self._h4.get(symbol, ts)
        if h4_rising is True:   # rising = reject SHORT
            return
        h4_dir = 'FALLING' if h4_rising is False else 'UNKNOWN'

        # ── Setup candle passed all filters — wait for trigger ────────────────
        zone = 'A' if in_zone_a else 'B'
        log.info(f"{symbol} S2: setup candle passed at {_fmt_ts(ts)} | "
                 f"MA44={ma44:.4f} dist={dist_pct*100:.3f}%(zone {zone}) "
                 f"slope={slope_8bar:+.3f}% accel={ma44_accel:+.5f} "
                 f"ATR={atr_pct:.3f}% H4={h4_dir}")

        state.s2_pending_setup = True
        state.s2_setup_ts      = ts
        state.s2_setup_snap    = {
            'ma44': ma44, 'dist_pct': dist_pct * 100,
            'zone': zone, 'slope_8bar': slope_8bar,
            'ma44_accel': ma44_accel, 'atr_pct': atr_pct,
            'h4_dir': h4_dir, 'body_ratio': body_ratio,
            'wick_pct': wick_pct * 100,
            'setup_open': c_open, 'setup_close': c_close,
            'setup_high': c_high, 'setup_low': candle['l'],
        }

    def _try_s2_trigger(self, symbol: str, candle: dict, ind: dict,
                        state: SymbolState, now_sec: float):
        """
        Called on the candle AFTER the setup candle.
        Trigger condition: this candle's OPEN is below MA44.
        Entry price = open of this candle.
        """
        ma44  = ind['ma44']
        c_open = candle['o']
        ts     = candle['t']

        if c_open >= ma44:
            log.debug(f"{symbol} S2: trigger failed (open {c_open:.4f} >= MA44 {ma44:.4f})")
            return

        # Check cooldown / pause again (time has passed)
        if state.s2_in_cooldown(now_sec) or state.s2_in_pause(now_sec):
            log.debug(f"{symbol} S2: trigger valid but in cooldown/pause")
            return

        entry = c_open
        sl    = entry * (1 + S2_SL_PCT / 100)
        tp    = entry * (1 - S2_TP_PCT / 100)
        snap  = state.s2_setup_snap

        signal = SignalEvent(
            strategy    = 'S2_MA44_BOUNCE',
            symbol      = symbol,
            direction   = 'SHORT',
            entry_price = entry,
            sl_price    = sl,
            tp_price    = tp,
            signal_ts   = ts,
            signal_time = _fmt_ts(ts),
            reason      = (f"MA44 bounce SHORT | setup={_fmt_ts(state.s2_setup_ts)} "
                           f"dist={snap.get('dist_pct', 0):.3f}%(zone {snap.get('zone','?')}) | "
                           f"slope={snap.get('slope_8bar', 0):+.3f}% "
                           f"H4={snap.get('h4_dir','?')}"),
            indicators  = snap,
        )

        state.s2_trade_open     = True
        state.s2_last_signal_ts = now_sec

        log.info(f"[SIGNAL] {symbol} S2 SHORT | entry={entry:.4f} "
                 f"SL={sl:.4f} TP={tp:.4f} | {signal.reason}")

        self._emit(signal)

    # ==========================================================================
    # TRADE OUTCOME FEEDBACK — called by Step 3 when a trade closes
    # ==========================================================================

    def on_trade_closed(self, symbol: str, strategy: str, outcome: str):
        """
        Step 3 calls this when SL or TP is hit so the detector can:
          - Clear the trade_open flag (allow new signals)
          - Update S2 consecutive loss counter
          - Trigger S2 pause if needed

        outcome: 'WIN' | 'LOSS'
        """
        state   = self._get_state(symbol)
        now_sec = time.time()

        if strategy == 'S1_EMA_CROSS':
            state.s1_trade_open = False
            log.info(f"{symbol} S1: trade closed ({outcome}) — gate open")

        elif strategy == 'S2_MA44_BOUNCE':
            state.s2_trade_open = False
            if outcome == 'WIN':
                state.s2_record_win()
                log.info(f"{symbol} S2: trade closed (WIN) — consecutive losses reset")
            elif outcome == 'LOSS':
                state.s2_record_loss(now_sec)
            log.info(f"{symbol} S2: trade closed ({outcome}) — gate open")

    # ==========================================================================
    # INTERNAL HELPERS
    # ==========================================================================

    def _emit(self, signal: SignalEvent):
        """Call the registered signal handler."""
        if self.on_signal:
            try:
                self.on_signal(signal)
            except Exception as e:
                log.error(f"Signal handler error: {e}", exc_info=True)
        else:
            # No handler set yet — just log it
            log.warning(f"Signal emitted but no handler set: {signal.symbol} "
                        f"{signal.strategy} {signal.direction}")


# ============================================================================
# HELPER
# ============================================================================

def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


# ============================================================================
# STANDALONE TEST — wire Step 1 + Step 2 together and watch for signals
# ============================================================================

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s  %(levelname)-7s  %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        handlers = [
            logging.FileHandler('bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ]
    )

    # Import Step 1
    try:
        from step1_candle_engine import CandleEngine, SYMBOLS
    except ImportError:
        print("ERROR: step1_candle_engine.py must be in the same folder.")
        sys.exit(1)

    # ── Signal handler (prints to terminal) ───────────────────────────────────
    def handle_signal(sig: SignalEvent):
        print(f"""
+{'='*62}+
|  *** SIGNAL DETECTED ***
|  Strategy : {sig.strategy}
|  Symbol   : {sig.symbol}
|  Direction: {sig.direction}
|  Time     : {sig.signal_time}
|  Entry    : {sig.entry_price:.4f}
|  SL       : {sig.sl_price:.4f}
|  TP       : {sig.tp_price:.4f}
|  Reason   : {sig.reason[:55]}
+{'='*62}+
""")

    # ── Wire Step 1 -> Step 2 ────────────────────────────────────────────────
    detector = SignalDetector()
    detector.on_signal = handle_signal

    # Patch CandleEngine to also share candle lists with detector
    # (needed for S2 monotonic MA44 check)
    original_on_message = None

    def patched_callback(symbol, candle, indicators):
        # Share candle list with detector before calling on_candle_close
        from step1_candle_engine import CandleEngine as CE
        candle_list = engine.store.get_list(symbol)
        detector.set_candle_list(symbol, candle_list)
        detector.on_candle_close(symbol, candle, indicators)

    TEST_SYMBOLS = SYMBOLS   # watch all symbols
    engine = CandleEngine(TEST_SYMBOLS, callback=patched_callback)

    print(f"""
+------------------------------------------------------+
|  STEP 2 -- Signal Detector  (test mode)              |
|                                                      |
|  Watching {len(TEST_SYMBOLS)} symbols on 15m candles            |
|  S1: EMA 9/26 Cross + 6 filters (LONG + SHORT)       |
|  S2: MA44 Bounce  (SHORT only, two-step)             |
|                                                      |
|  Signals print here when detected.                   |
|  Also logged to bot.log                              |
|  Press Ctrl+C to stop.                               |
+------------------------------------------------------+
""")

    try:
        engine.start()
    except KeyboardInterrupt:
        print("\nStopped.")