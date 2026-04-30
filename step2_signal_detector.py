"""
STEP 2 OF 4 — Signal Detector
==============================
Receives every candle close from Step 1 (CandleEngine).
Checks S1 entry conditions.
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

# ============================================================================
# SIGNAL EVENT — the object passed to the order manager
# ============================================================================

@dataclass
class SignalEvent:
    """
    Emitted when a valid trade setup is detected.
    Step 3 (OrderManager) receives this and places the trade.
    """
    strategy:    str          # 'S1_EMA_CROSS'
    symbol:      str          # e.g. 'BTCUSDT'
    direction:   str          # 'LONG' or 'SHORT'
    entry_price: float        # suggested entry (close of signal candle for S1)
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
    """All mutable state for one symbol."""

    def __init__(self, symbol: str):
        self.symbol = symbol

        # S1 state
        self.s1_trade_open      = False   # blocks new S1 signals while True
        self.s1_last_cross_dir  = None    # direction of last crossover (prevents re-use)
        self.s1_cross_candle_ts = None    # timestamp of crossover candle
        self.s1_pending_dir     = None    # crossover detected, waiting for N+1 confirm
        self.s1_pending_ts      = None    # ts of the crossover candle


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
                    'macd', 'macd_sig', 'macd_hist']
        if any(ind.get(k) is None for k in required):
            return

        state    = self._get_state(symbol)

        self._check_s1(symbol, candle, ind, state)

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

        # F3 — EMA200 trend gate  (DISABLED — was rejecting too many counter-trend signals)
        # if direction == 'LONG'  and c_close <= ema200: return
        # if direction == 'SHORT' and c_close >= ema200: return

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
    # TRADE OUTCOME FEEDBACK — called by Step 3 when a trade closes
    # ==========================================================================

    def on_trade_closed(self, symbol: str, strategy: str, outcome: str):
        """
        Step 3 calls this when SL or TP is hit so the detector can:
          - Clear the trade_open flag (allow new signals)

        outcome: 'WIN' | 'LOSS'
        """
        state = self._get_state(symbol)

        if strategy == 'S1_EMA_CROSS':
            state.s1_trade_open = False
            log.info(f"{symbol} S1: trade closed ({outcome}) — gate open")

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
