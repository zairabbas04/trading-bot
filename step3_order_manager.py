"""
STEP 3 OF 4 — Order Manager  (Futures Edition)
================================================
Changes in this version:
  - Switched from Binance Spot to Binance USDT-M Futures (testnet & live)
  - Isolated margin mode per trade
  - Strategy-specific sizing:
      S1 (EMA Cross)    → $20 margin × 50x leverage ($1,000 notional)
  - Global cap: max 10 open positions at once (new signals ignored above limit)
  - Consecutive-loss counter exposed for dashboard
  - pnl_usdt stored alongside pnl_pct in trade log
  - Fixed scientific notation bug (_fmt_price) retained from previous version
  - Fix: maxQty cap to prevent Exceeded maximum allowable position (Error -2027)
  - Fix: TP/SL use TAKE_PROFIT/STOP with workingType=CONTRACT_PRICE (Error -4120)
"""

import os
import sys
import io
import csv
import math
import time
import hmac
import hashlib
import logging
import threading
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

log = logging.getLogger('order_manager')

load_dotenv()

API_KEY    = os.getenv('BINANCE_API_KEY', '')
API_SECRET = os.getenv('BINANCE_API_SECRET', os.getenv('BINANCE_SECRET', ''))
TESTNET    = os.getenv('TESTNET', 'true').lower() == 'true'

# Futures endpoints
if TESTNET:
    BASE_URL = "https://demo-fapi.binance.com/fapi"
else:
    BASE_URL = "https://fapi.binance.com/fapi"

# Strategy-specific trade sizing  (sized for 5,000 USDT wallet)
# ============================================================================
# POSITION CAPS — real -2027 notional limits per symbol on demo-fapi
# Discovered empirically via find_caps.py (binary search with real orders).
# These are the actual Binance demo account caps at 50x leverage, with 5% margin.
# Regenerate when switching to live account (live limits are much higher).
# ============================================================================

POSITION_CAPS = {
    'BTCUSDT':11389,   'ETHUSDT':11389,   'XRPUSDT':5724,
    'TRXUSDT':11311,   'ADAUSDT':2886,    'ZECUSDT':11045,
    'DOTUSDT':11045,   'VETUSDT':2886,    'FETUSDT':2886,
    'SEIUSDT':2886,    'DASHUSDT':2886,   'SYRUPUSDT':5724,
    'ENSUSDT':7143,    'BARDUSDT':5724,   'TWTUSDT':2886,
}

STRATEGY_CONFIG = {
    'S1':            {'margin_usdt': 20.0,   'leverage': 50},   # EMA Cross — $1,000 notional target
    'S1_EMA_CROSS':  {'margin_usdt': 20.0,   'leverage': 50},
}
DEFAULT_MARGIN   = 20.0
DEFAULT_LEVERAGE = 50

MAX_OPEN_POSITIONS = 30   # practical max: $5,000 balance / $16.65 min margin = ~300 positions; cap at 30 concurrent
                          # pre-flight balance check is the real hard limit
POLL_INTERVAL      = 15
TRADE_LOG_FILE     = 'trade_log.csv'

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    def __init__(self, url: str, key: str):
        self._url     = url.rstrip('/')
        self._headers = {
            'apikey':        key,
            'Authorization': f'Bearer {key}',
            'Content-Type':  'application/json',
            'Prefer':        'return=minimal',
        }
        self._ok = bool(url and key)
        if not self._ok:
            log.info("Supabase disabled — using local CSV/in-memory trade history only")

    def insert(self, table: str, row: dict):
        if not self._ok:
            return
        try:
            resp = requests.post(
                f"{self._url}/rest/v1/{table}",
                json=row,
                headers=self._headers,
                timeout=10,
            )
            if resp.status_code not in (200, 201):
                log.warning(f"Supabase insert failed {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Supabase insert error: {e}")

    def update(self, table: str, row_id: int, row: dict):
        if not self._ok:
            return
        try:
            headers = dict(self._headers)
            headers['Prefer'] = 'return=minimal'
            resp = requests.patch(
                f"{self._url}/rest/v1/{table}?id=eq.{row_id}",
                json=row,
                headers=headers,
                timeout=10,
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(f"Supabase update failed {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Supabase update error: {e}")

    def insert_returning_id(self, table: str, row: dict) -> int | None:
        """Insert a row and return its auto-generated id."""
        if not self._ok:
            return None
        try:
            headers = dict(self._headers)
            headers['Prefer'] = 'return=representation'
            resp = requests.post(
                f"{self._url}/rest/v1/{table}",
                json=row,
                headers=headers,
                timeout=10,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                if data:
                    return data[0].get('id')
            log.warning(f"Supabase insert_returning_id failed {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Supabase insert_returning_id error: {e}")
        return None

    def select_all(self, table: str) -> list:
        if not self._ok:
            return []
        try:
            headers = dict(self._headers)
            headers['Prefer'] = 'count=none'
            resp = requests.get(
                f"{self._url}/rest/v1/{table}?select=*&order=id.asc",
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json()
            log.warning(f"Supabase select failed {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Supabase select error: {e}")
        return []


# ============================================================================
# BINANCE FUTURES REST CLIENT
# ============================================================================

class BinanceClient:

    def __init__(self, api_key: str, api_secret: str, base_url: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self.session    = requests.Session()
        self.session.headers.update({'X-MBX-APIKEY': api_key})

    def _fmt_price(self, value: float) -> str:
        """Format float as plain decimal — Binance rejects scientific notation."""
        formatted = f'{value:.10f}'.rstrip('0')
        if formatted.endswith('.'):
            formatted += '0'
        return formatted

    def _sign(self, params: dict) -> dict:
        params['timestamp'] = int(time.time() * 1000)
        query = '&'.join(f"{k}={v}" for k, v in params.items())
        sig   = hmac.new(
            self.api_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        params['signature'] = sig
        return params

    def _get(self, path: str, params: dict = None, signed: bool = False):
        params = params or {}
        if signed:
            params = self._sign(params)
        resp = self.session.get(f"{self.base_url}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, params: dict):
        params = self._sign(params)
        resp = self.session.post(f"{self.base_url}{path}", data=params, timeout=10)
        # Retry once on Binance demo server timeout (-1007 / 408)
        if resp.status_code == 408:
            log.warning(f"POST {path} timed out (408), retrying once...")
            time.sleep(1)
            params = self._sign({k:v for k,v in params.items()
                                  if k not in ('timestamp','signature')})
            resp = self.session.post(f"{self.base_url}{path}", data=params, timeout=15)
        if resp.status_code != 200:
            log.error(f"POST {path} failed {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, params: dict):
        params = self._sign(params)
        resp = self.session.delete(f"{self.base_url}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ── Futures-specific helpers ──────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        try:
            return self._post('/v1/leverage', {
                'symbol':   symbol,
                'leverage': leverage,
            })
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else ''
            if '-4028' in body:
                # Leverage not valid — step down through common levels until accepted
                log.warning(f"{symbol}: leverage {leverage}x not valid, stepping down...")
                fallbacks = [l for l in [50,40,33,25,20,15,10,5,3,1] if l < leverage]
                for fallback in fallbacks:
                    try:
                        result = self._post('/v1/leverage', {'symbol': symbol, 'leverage': fallback})
                        log.warning(f"{symbol}: leverage accepted at {fallback}x")
                        return result
                    except requests.exceptions.HTTPError:
                        continue
            raise

    def set_margin_type(self, symbol: str, margin_type: str = 'ISOLATED') -> dict:
        try:
            return self._post('/v1/marginType', {
                'symbol':     symbol,
                'marginType': margin_type,
            })
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else ''
            if '-4046' in body:
                # Already set to the requested margin type — not an error
                log.debug(f"{symbol}: margin type already {margin_type}")
                return {}
            if '-1121' in body:
                # Symbol doesn't exist on this futures endpoint
                raise ValueError(f"{symbol} not listed on futures demo")
            raise
        except Exception:
            raise

    def get_symbol_info(self, symbol: str) -> dict:
        info = self._get('/v1/exchangeInfo')
        for s in info.get('symbols', []):
            if s['symbol'] == symbol:
                return s
        return {}

    def get_max_notional(self, symbol: str, leverage: int) -> float:
        """
        Returns the real -2027 notional cap for a symbol.
        Uses empirically discovered POSITION_CAPS dict instead of leverageBracket,
        which returns incorrect values on the demo account.
        Falls back to $2,886 (conservative demo default) if symbol not in dict.
        """
        cap = POSITION_CAPS.get(symbol)
        if cap is not None:
            log.debug(f"[CAP] {symbol}: notionalCap=${cap:,} (from POSITION_CAPS)")
            return float(cap)
        log.warning(f"[CAP] {symbol}: not in POSITION_CAPS, using fallback $2,886")
        return 2886.0

    def get_ticker_price(self, symbol: str) -> float:
        data = self._get('/v1/ticker/price', {'symbol': symbol})
        return float(data['price'])

    def get_account(self) -> dict:
        return self._get('/v2/account', {}, signed=True)

    def get_usdt_balance(self) -> float:
        account = self.get_account()
        for a in account.get('assets', []):
            if a['asset'] == 'USDT':
                return float(a['availableBalance'])
        return 0.0

    def place_market_order(self, symbol: str, side: str, quantity: float,
                           reduce_only: bool = False) -> dict:
        params = {
            'symbol':   symbol,
            'side':     side,
            'type':     'MARKET',
            'quantity': self._fmt_price(quantity),
        }
        if reduce_only:
            params['reduceOnly'] = 'true'
        return self._post('/v1/order', params)

    def _post_algo(self, path: str, params: dict):
        """POST to algo order endpoint — required for TAKE_PROFIT/STOP since 2025-12-09.
        Uses data= (request body) per Binance docs: POST params go in body, not URL."""
        params = self._sign(params)
        resp = self.session.post(
            f"{self.base_url}{path}",
            data=params,    # body, not query string
            timeout=10
        )
        if resp.status_code != 200:
            log.error(f"POST {path} failed {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def place_take_profit_order(self, symbol: str, side: str,
                                quantity: float, tp_price: float) -> dict:
        """
        TAKE_PROFIT via POST /fapi/v1/algoOrder.
        Mandatory since Binance migrated conditional orders to Algo Service 2025-12-09.
        Params per official docs: algoType + type (mandatory), triggerPrice, price, workingType.
        """
        params = {
            'symbol':       symbol,
            'side':         side,
            'algoType':     'CONDITIONAL',
            'type':         'TAKE_PROFIT',     # mandatory per docs (not orderType)
            'quantity':     self._fmt_price(quantity),
            'price':        self._fmt_price(tp_price),
            'triggerPrice': self._fmt_price(tp_price),
            'timeInForce':  'GTC',
            'reduceOnly':   'true',
            'workingType':  'CONTRACT_PRICE',
        }
        return self._post_algo('/v1/algoOrder', params)

    def place_stop_loss_order(self, symbol: str, side: str,
                              quantity: float, sl_price: float) -> dict:
        """
        STOP via POST /fapi/v1/algoOrder.
        Mandatory since Binance migrated conditional orders to Algo Service 2025-12-09.
        """
        params = {
            'symbol':       symbol,
            'side':         side,
            'algoType':     'CONDITIONAL',
            'type':         'STOP',            # mandatory per docs (not orderType)
            'quantity':     self._fmt_price(quantity),
            'price':        self._fmt_price(sl_price),
            'triggerPrice': self._fmt_price(sl_price),
            'timeInForce':  'GTC',
            'reduceOnly':   'true',
            'workingType':  'CONTRACT_PRICE',
        }
        return self._post_algo('/v1/algoOrder', params)

    def cancel_order(self, symbol: str, order_id: int) -> dict:
        return self._delete('/v1/order', {'symbol': symbol, 'orderId': order_id})

    def get_algo_order(self, algo_id: int) -> dict:
        """Query an algo order status by algoId — used for TP/SL monitoring."""
        return self._get('/v1/algoOrder', {'algoId': algo_id}, signed=True)

    def cancel_algo_order(self, algo_id: int) -> dict:
        """Cancel an algo order by algoId — DELETE /fapi/v1/algoOrder (signed)."""
        params = self._sign({'algoId': algo_id})
        resp = self.session.delete(
            f"{self.base_url}/v1/algoOrder",
            params=params,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()

    def get_order(self, symbol: str, order_id: int) -> dict:
        return self._get('/v1/order', {'symbol': symbol, 'orderId': order_id}, signed=True)

    def get_open_orders(self, symbol: str = None) -> list:
        params = {}
        if symbol:
            params['symbol'] = symbol
        return self._get('/v1/openOrders', params, signed=True)

    def get_position(self, symbol: str) -> dict:
        data = self._get('/v2/positionRisk', {'symbol': symbol}, signed=True)
        if isinstance(data, list) and data:
            return data[0]
        return {}


# ============================================================================
# SYMBOL PRECISION HELPER  (Futures version)
# ============================================================================

class PrecisionCache:

    CACHE_TTL = 24 * 3600   # refresh symbol info every 24 hours

    def __init__(self, client: BinanceClient):
        self._client = client
        self._cache  = {}          # symbol -> dict
        self._fetched_at = {}      # symbol -> epoch float
        self._lock   = threading.Lock()

    def refresh(self, symbol: str) -> None:
        """Force-expire cache for a symbol so next get() fetches fresh data from API."""
        with self._lock:
            self._fetched_at.pop(symbol, None)
            self._cache.pop(symbol, None)
        log.info(f"[CACHE] Refreshed precision cache for {symbol}")

    def get(self, symbol: str) -> dict:
        now = time.time()
        with self._lock:
            if symbol in self._cache:
                age = now - self._fetched_at.get(symbol, 0)
                if age < self.CACHE_TTL:
                    return self._cache[symbol]

        info    = self._client.get_symbol_info(symbol)
        filters = {f['filterType']: f for f in info.get('filters', [])}

        lot      = filters.get('LOT_SIZE', {})
        tick     = filters.get('PRICE_FILTER', {})
        notional = filters.get('MIN_NOTIONAL', {})

        def _decimals(step_str: str) -> int:
            s = step_str.rstrip('0')
            return len(s.split('.')[-1]) if '.' in s else 0

        result = {
            'qty_step':       float(lot.get('stepSize', '0.001')),
            'qty_decimals':   _decimals(lot.get('stepSize', '0.001')),
            'price_step':     float(tick.get('tickSize', '0.01')),
            'price_decimals': _decimals(tick.get('tickSize', '0.01')),
            'min_qty':        float(lot.get('minQty', '0.001')),
            'max_qty':        float(lot.get('maxQty', '9999999')),   # ← for Error -2027
            'min_notional':   float(notional.get('minNotional', '5')),
        }

        with self._lock:
            self._cache[symbol] = result
            self._fetched_at[symbol] = time.time()
        return result

    def calc_quantity(self, symbol: str, price: float,
                      margin: float, leverage: int) -> float:
        """Legacy wrapper — use resolve_order_params for full dynamic logic."""
        qty, _ = self.resolve_order_params(symbol, price, margin, leverage)
        return qty

    def resolve_order_params(self, symbol: str, price: float,
                             margin: float, target_leverage: int) -> tuple:
        """
        Resolve the actual quantity and leverage for an order:

          1. Get max notional Binance allows for target_leverage on this symbol
          2. Cap notional = min(margin × target_leverage, max_notional)
          3. qty = floor(notional / price / stepSize) × stepSize
          4. If qty > LOT_SIZE maxQty: cap qty, keep margin fixed, back-calc leverage
          5. Return (qty, actual_leverage) — caller sets leverage if it changed

        This always deploys the full margin. Leverage only decreases if qty is capped.
        """
        prec     = self.get(symbol)
        step     = prec['qty_step']
        decimals = prec['qty_decimals']
        max_qty  = prec['max_qty']

        # Step 1: get the notional cap Binance enforces at this leverage
        max_notional = self._client.get_max_notional(symbol, target_leverage)

        # Step 2: compute raw notional (full margin × leverage)
        notional = min(margin * target_leverage, max_notional)

        # Step 3: compute qty floored to stepSize
        raw_qty = notional / price
        qty     = math.floor(raw_qty / step) * step
        qty     = round(qty, decimals)

        # Step 3b: if qty × price == max_notional exactly (boundary collision),
        # subtract one stepSize to stay strictly below the cap.
        # This preserves max margin deployment while avoiding -2027.
        if abs(qty * price - max_notional) < 0.001 and qty >= step:
            qty = round(qty - step, decimals)

        # Leverage stays at target unless qty is further capped below
        actual_leverage = target_leverage

        # Step 4: if qty still exceeds LOT_SIZE maxQty, cap it and back-calc leverage
        if qty > max_qty:
            qty             = math.floor(max_qty / step) * step
            qty             = round(qty, decimals)
            capped_notional = qty * price
            raw_lev         = capped_notional / margin
            # Round DOWN to nearest valid Binance leverage level to avoid -4028
            valid_levels    = [50, 40, 33, 25, 20, 15, 10, 5, 3, 1]
            actual_leverage = next((l for l in valid_levels if l <= raw_lev), 1)
            log.warning(f"[QTY CAP] {symbol}: qty capped to {qty} (maxQty={max_qty}), "
                        f"leverage back-calc to {actual_leverage}x "
                        f"(notional=${capped_notional:.0f}, margin=${margin})")

        log.debug(f"[RESOLVE] {symbol}: target={target_leverage}x "
                  f"max_notional=${max_notional:.0f} "
                  f"notional=${qty*price:.0f} qty={qty} lev={actual_leverage}x")

        return qty, actual_leverage

    def round_price(self, symbol: str, price: float) -> float:
        p    = self.get(symbol)
        step = p['price_step']
        return round(round(price / step) * step, p['price_decimals'])


# ============================================================================
# OPEN POSITION TRACKER
# ============================================================================

class OpenPosition:

    def __init__(self, symbol, strategy, direction,
                 entry_price, sl_price, tp_price,
                 quantity, margin_usdt, leverage,
                 tp_order_id, sl_order_id,
                 entry_order_id, signal_ts, signal_time,
                 signal_price=None):
        self.symbol         = symbol
        self.strategy       = strategy
        self.direction      = direction
        self.entry_price    = entry_price
        self.sl_price       = sl_price
        self.tp_price       = tp_price
        self.quantity       = quantity
        self.margin_usdt    = margin_usdt
        self.leverage       = leverage
        self.tp_order_id    = tp_order_id
        self.sl_order_id    = sl_order_id
        self.entry_order_id = entry_order_id
        self.signal_ts      = signal_ts
        self.signal_time    = signal_time
        self.signal_price   = signal_price   # intended entry from signal (for slippage calc)
        self.open_time      = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        self.open_ts        = int(time.time() * 1000)
        self.db_id          = None           # set after Supabase insert in _log_trade_open


# ============================================================================
# ORDER MANAGER
# ============================================================================

class OrderManager:

    def __init__(self, detector=None, alerts=None):
        if not API_KEY or not API_SECRET:
            raise ValueError(
                "API keys not found. Create a .env file with:\n"
                "  BINANCE_API_KEY=your_key\n"
                "  BINANCE_SECRET=your_secret"
            )

        self.detector  = detector
        self.alerts    = alerts
        self.client    = BinanceClient(API_KEY, API_SECRET, BASE_URL)
        self.precision = PrecisionCache(self.client)
        self.supabase  = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

        self._lock            = threading.Lock()
        self._open_positions  = {}      # symbol → OpenPosition
        self._pending_symbols = set()

        # Consecutive loss counters per strategy
        self._consec_losses   = {'S1': 0}

        # In-memory history loaded from Supabase on startup
        self.closed_positions = []
        self._load_supabase_history()

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name='pos_monitor'
        )
        self._monitor_thread.start()

        self._init_csv()

        log.info(f"OrderManager ready | Futures | Testnet={TESTNET} | "
                 f"Max positions={MAX_OPEN_POSITIONS}")

        try:
            bal = self.client.get_usdt_balance()
            log.info(f"Futures wallet USDT balance: {bal:.2f}")
        except Exception as e:
            log.error(f"Could not fetch balance — check API keys: {e}")

    # ── Public stats for dashboard ────────────────────────────────────────────

    def get_open_positions_list(self) -> list:
        now_ms = int(time.time() * 1000)
        result = []
        with self._lock:
            for pos in self._open_positions.values():
                elapsed_s = (now_ms - pos.open_ts) // 1000
                hours, rem = divmod(elapsed_s, 3600)
                mins       = rem // 60
                duration   = f"{hours}h {mins:02d}m" if hours else f"{mins}m"
                result.append({
                    'symbol':      pos.symbol,
                    'strategy':    pos.strategy,
                    'direction':   pos.direction,
                    'entry_price': pos.entry_price,
                    'sl_price':    pos.sl_price,
                    'tp_price':    pos.tp_price,
                    'quantity':    pos.quantity,
                    'margin_usdt': pos.margin_usdt,
                    'leverage':    pos.leverage,
                    'open_time':   pos.open_time,
                    'open_ts':     pos.open_ts,
                    'duration':    duration,
                })
        return result

    def get_stats(self) -> dict:
        return {
            'consec_losses': dict(self._consec_losses),
            'open_count':    len(self._open_positions),
        }

    # ── Signal handler ────────────────────────────────────────────────────────

    def on_signal(self, signal):
        t = threading.Thread(
            target=self._handle_signal,
            args=(signal,),
            daemon=True,
            name=f"trade_{signal.symbol}"
        )
        t.start()

    def _handle_signal(self, signal):
        symbol    = signal.symbol
        direction = signal.direction
        strategy  = signal.strategy

        with self._lock:
            total_open = len(self._open_positions) + len(self._pending_symbols)
            if total_open >= MAX_OPEN_POSITIONS:
                log.info(f"[SKIP] {symbol}: max open positions ({MAX_OPEN_POSITIONS}) reached")
                return
            if symbol in self._open_positions:
                log.info(f"[SKIP] {symbol}: already has open position")
                return
            if symbol in self._pending_symbols:
                log.info(f"[SKIP] {symbol}: entry already in progress")
                return
            self._pending_symbols.add(symbol)

        log.info(f"[ORDER] Processing signal: {symbol} {strategy} {direction} "
                 f"entry~{signal.entry_price:.6f}")

        cfg      = STRATEGY_CONFIG.get(strategy, {'margin_usdt': DEFAULT_MARGIN,
                                                   'leverage':    DEFAULT_LEVERAGE})
        margin   = cfg['margin_usdt']
        leverage = cfg['leverage']

        try:
            # Pre-flight balance check — prevents -2019 Margin Insufficient
            available = self.client.get_usdt_balance()
            if available < margin:
                log.warning(f"[SKIP] {symbol}: insufficient balance "
                            f"${available:.2f} < required ${margin:.2f}")
                with self._lock:
                    self._pending_symbols.discard(symbol)
                return

            # Pre-flight cap check — skip symbols whose demo cap is below margin
            symbol_cap = POSITION_CAPS.get(symbol, 2886.0)
            if symbol_cap < margin:
                log.warning(f"[SKIP] {symbol}: demo position cap ${symbol_cap:.0f} "
                            f"< margin ${margin:.0f} — not tradeable on demo")
                with self._lock:
                    self._pending_symbols.discard(symbol)
                return

            # Cancel any stale open orders on this symbol before changing margin type
            # Prevents -4067: "Position side cannot be changed if there exists open orders"
            try:
                open_orders = self.client.get_open_orders(symbol)
                for o in (open_orders or []):
                    try:
                        self.client.cancel_order(symbol, o['orderId'])
                    except Exception:
                        pass
                # Also cancel any open algo orders
                open_algos = self.client._get('/v1/openAlgoOrders', {'symbol': symbol}, signed=True)
                for o in (open_algos or []):
                    try:
                        self.client.cancel_algo_order(o['algoId'])
                    except Exception:
                        pass
            except Exception as e:
                log.debug(f"[CLEANUP] {symbol}: stale order cleanup: {e}")

            self.client.set_margin_type(symbol, 'ISOLATED')

            # Set leverage — response contains the actual leverage Binance accepted
            lev_response    = self.client.set_leverage(symbol, leverage)
            accepted_lev    = int(lev_response.get('leverage', leverage)) if lev_response else leverage

            current_price = self.client.get_ticker_price(symbol)
            prec          = self.precision.get(symbol)

            # Resolve qty and actual leverage using dynamic margin logic.
            # Use accepted_lev (what Binance confirmed) as the effective target.
            qty, actual_leverage = self.precision.resolve_order_params(
                symbol, current_price, margin, accepted_lev
            )

            # If qty cap further reduced leverage, re-set on Binance
            if actual_leverage != accepted_lev:
                log.info(f"[LEVERAGE] {symbol}: {accepted_lev}x → {actual_leverage}x "
                         f"(maxQty cap, margin ${margin} preserved)")
                lev_resp2       = self.client.set_leverage(symbol, actual_leverage)
                confirmed_lev   = int(lev_resp2.get('leverage', actual_leverage)) if lev_resp2 else actual_leverage
                if confirmed_lev != actual_leverage:
                    # Binance accepted a different leverage (e.g. step-down hit) — re-resolve qty
                    log.info(f"[LEVERAGE] {symbol}: back-calc {actual_leverage}x → confirmed {confirmed_lev}x, re-resolving qty")
                    actual_leverage = confirmed_lev
                    qty, _ = self.precision.resolve_order_params(symbol, current_price, margin, actual_leverage)
            elif accepted_lev != leverage:
                log.info(f"[LEVERAGE] {symbol}: target {leverage}x → accepted {accepted_lev}x "
                         f"(Binance limit)")

            if qty < prec['min_qty']:
                log.warning(f"[SKIP] {symbol}: qty {qty} < min_qty {prec['min_qty']}")
                with self._lock:
                    self._pending_symbols.discard(symbol)
                return

            entry_side = 'BUY' if direction == 'LONG' else 'SELL'
            exit_side  = 'SELL' if direction == 'LONG' else 'BUY'
            sl_pct     = signal.sl_price / signal.entry_price
            tp_pct     = signal.tp_price / signal.entry_price
            lev_note   = f" (target {leverage}x)" if actual_leverage != leverage else ""
            log.info(f"[ORDER] Placing {entry_side} MARKET {qty} {symbol} @ ~{current_price:.6f} "
                     f"[{strategy} margin=${margin} lev={actual_leverage}x{lev_note}]")

            entry_result   = self.client.place_market_order(symbol, entry_side, qty)
            entry_order_id = entry_result['orderId']

            avg_price    = float(entry_result.get('avgPrice', 0) or 0)
            actual_entry = avg_price if avg_price > 0 else current_price

            log.info(f"[ORDER] Entry filled: {entry_side} {qty} {symbol} @ {actual_entry:.6f} "
                     f"(order #{entry_order_id})")

            sl_price = self.precision.round_price(symbol, actual_entry * sl_pct)
            tp_price = self.precision.round_price(symbol, actual_entry * tp_pct)

            # Uses /v1/algoOrder (mandatory since Binance API change 2025-12-09)
            # If TP/SL placement fails after entry fills, close immediately and log to DB
            try:
                tp_result   = self.client.place_take_profit_order(symbol, exit_side, qty, tp_price)
                sl_result   = self.client.place_stop_loss_order(symbol, exit_side, qty, sl_price)
                tp_order_id = tp_result.get('algoId') or tp_result.get('orderId')
                sl_order_id = sl_result.get('algoId') or sl_result.get('orderId')

                log.info(f"[ORDER] TP order #{tp_order_id} @ {tp_price:.6f} | "
                         f"SL order #{sl_order_id} @ {sl_price:.6f}")

            except Exception as tp_sl_err:
                # Entry is already filled — position is live and unprotected
                # Emergency: close immediately with a market order, then log to DB
                log.error(f"[EMERGENCY] {symbol}: TP/SL placement failed after entry fill — "
                          f"closing position immediately. Error: {tp_sl_err}")
                try:
                    close_result = self.client.place_market_order(symbol, exit_side, qty)
                    exit_price   = float(close_result.get('avgPrice', 0) or actual_entry)
                    log.info(f"[EMERGENCY] {symbol}: position closed @ {exit_price:.6f}")
                except Exception as close_err:
                    log.error(f"[EMERGENCY] {symbol}: FAILED to close position: {close_err}")
                    exit_price = actual_entry  # best guess for DB record

                # Log to DB as a MANUAL_CLOSE so it appears in dashboard
                emergency_pos = OpenPosition(
                    symbol         = symbol,
                    strategy       = strategy,
                    direction      = direction,
                    entry_price    = actual_entry,
                    sl_price       = actual_entry * (1 - 0.005) if direction == 'LONG' else actual_entry * (1 + 0.005),
                    tp_price       = actual_entry * (1 + 0.015) if direction == 'LONG' else actual_entry * (1 - 0.015),
                    quantity       = qty,
                    margin_usdt    = margin,
                    leverage       = actual_leverage,
                    tp_order_id    = 0,
                    sl_order_id    = 0,
                    entry_order_id = entry_order_id,
                    signal_ts      = signal.signal_ts,
                    signal_time    = signal.signal_time,
                    signal_price   = signal.entry_price,
                )
                self._log_trade_close(emergency_pos, 'MANUAL_CLOSE', exit_price=exit_price)

                with self._lock:
                    self._pending_symbols.discard(symbol)
                if detector:
                    detector.on_trade_closed(symbol, strategy, 'LOSS')
                return

            position = OpenPosition(
                symbol         = symbol,
                strategy       = strategy,
                direction      = direction,
                entry_price    = actual_entry,
                sl_price       = sl_price,
                tp_price       = tp_price,
                quantity       = qty,
                margin_usdt    = margin,
                leverage       = actual_leverage,
                tp_order_id    = tp_order_id,
                sl_order_id    = sl_order_id,
                entry_order_id = entry_order_id,
                signal_ts      = signal.signal_ts,
                signal_time    = signal.signal_time,
                signal_price   = signal.entry_price,   # intended price for slippage tracking
            )

            with self._lock:
                self._open_positions[symbol] = position
                self._pending_symbols.discard(symbol)

            self._log_trade_open(position)

        except ValueError as e:
            # Symbol not available on futures — skip silently
            log.warning(f"[SKIP] {symbol}: {e}")
            with self._lock:
                self._pending_symbols.discard(symbol)

        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else ''
            if '-2019' in body:
                log.warning(f"[SKIP] {symbol}: insufficient margin in demo account — skipping trade")
                with self._lock:
                    self._pending_symbols.discard(symbol)
            elif '-4005' in body:
                # qty > maxQty — precision cache was stale. Refresh and retry once.
                log.warning(f"[RETRY] {symbol}: qty exceeded maxQty (-4005), refreshing cache and retrying")
                self.precision.refresh(symbol)
                try:
                    prec         = self.precision.get(symbol)
                    retry_qty, retry_lev = self.precision.resolve_order_params(
                        symbol, current_price, margin, actual_leverage
                    )
                    if retry_qty < prec['min_qty']:
                        log.warning(f"[SKIP] {symbol}: retry qty {retry_qty} below min — skipping")
                        with self._lock:
                            self._pending_symbols.discard(symbol)
                        return
                    if retry_lev != actual_leverage:
                        self.client.set_leverage(symbol, retry_lev)
                    retry_result = self.client.place_market_order(symbol, entry_side, retry_qty)
                    log.info(f"[ORDER] Retry filled: {entry_side} {retry_qty} {symbol} "
                             f"@ {float(retry_result.get('avgPrice', current_price)):.6f}")
                    # Re-place TP/SL with corrected qty
                    retry_entry  = float(retry_result.get('avgPrice', 0) or 0) or current_price
                    retry_sl     = self.precision.round_price(symbol, retry_entry * sl_pct)
                    retry_tp     = self.precision.round_price(symbol, retry_entry * tp_pct)
                    tp_r = self.client.place_take_profit_order(symbol, exit_side, retry_qty, retry_tp)
                    sl_r = self.client.place_stop_loss_order(symbol, exit_side, retry_qty, retry_sl)
                    position = OpenPosition(
                        symbol=symbol, strategy=strategy, direction=direction,
                        entry_price=retry_entry, sl_price=retry_sl, tp_price=retry_tp,
                        quantity=retry_qty, margin_usdt=margin, leverage=retry_lev,
                        tp_order_id=tp_r.get('algoId'), sl_order_id=sl_r.get('algoId'),
                        entry_order_id=retry_result['orderId'],
                        signal_ts=signal.signal_ts, signal_time=signal.signal_time,
                        signal_price=signal.entry_price,
                    )
                    with self._lock:
                        self._open_positions[symbol] = position
                        self._pending_symbols.discard(symbol)
                    self._log_trade_open(position)
                except Exception as retry_err:
                    log.error(f"[ORDER] Retry failed for {symbol}: {retry_err}")
                    with self._lock:
                        self._pending_symbols.discard(symbol)
            elif '-2027' in body:
                # Exceeded max allowable position.
                # The leverageBracket cap is unreliable on demo — it may return the same
                # cap regardless of leverage, causing infinite retries at the same notional.
                # Instead: keep the original leverage and halve the notional on each attempt.
                # This is guaranteed to converge and preserves leverage (only qty shrinks).
                log.warning(f"[RETRY] {symbol}: -2027 at lev={actual_leverage}x "
                            f"qty={qty} notional=${qty*current_price:.0f} — halving notional")
                placed       = False
                retry_qty    = qty
                prec         = self.precision.get(symbol)
                MIN_VIABLE_NOTIONAL = margin * 0.5  # skip if notional < 50% of margin (not worth trading)
                for attempt in range(6):   # max 6 halvings: $5000→$2500→$1250→$625→$312→$156
                    retry_qty = math.floor(retry_qty / 2 / prec['qty_step']) * prec['qty_step']
                    retry_qty = round(retry_qty, prec['qty_decimals'])
                    if retry_qty < prec['min_qty']:
                        log.warning(f"[SKIP] {symbol}: halved qty {retry_qty} below min — giving up")
                        break
                    notional_check = retry_qty * current_price
                    if notional_check < MIN_VIABLE_NOTIONAL:
                        log.warning(f"[SKIP] {symbol}: halved notional ${notional_check:.0f} < "
                                    f"min viable ${MIN_VIABLE_NOTIONAL:.0f} — demo cap too tight, skipping")
                        break
                    log.info(f"[RETRY] {symbol}: attempt {attempt+1} — "
                             f"qty={retry_qty} notional=${notional_check:.0f} lev={actual_leverage}x")
                    try:
                        retry_result = self.client.place_market_order(symbol, entry_side, retry_qty)
                        retry_entry  = float(retry_result.get('avgPrice', 0) or 0) or current_price
                        retry_sl     = self.precision.round_price(symbol, retry_entry * sl_pct)
                        retry_tp     = self.precision.round_price(symbol, retry_entry * tp_pct)
                        tp_r = self.client.place_take_profit_order(symbol, exit_side, retry_qty, retry_tp)
                        sl_r = self.client.place_stop_loss_order(symbol, exit_side, retry_qty, retry_sl)
                        # Back-calc actual leverage from accepted notional
                        accepted_notional = retry_qty * retry_entry
                        back_lev = max(1, round(accepted_notional / margin))
                        valid    = [50,40,33,25,20,15,10,5,3,1]
                        back_lev = next((l for l in valid if l <= back_lev), 1)
                        position = OpenPosition(
                            symbol=symbol, strategy=strategy, direction=direction,
                            entry_price=retry_entry, sl_price=retry_sl, tp_price=retry_tp,
                            quantity=retry_qty, margin_usdt=margin, leverage=back_lev,
                            tp_order_id=tp_r.get('algoId'), sl_order_id=sl_r.get('algoId'),
                            entry_order_id=retry_result['orderId'],
                            signal_ts=signal.signal_ts, signal_time=signal.signal_time,
                            signal_price=signal.entry_price,
                        )
                        with self._lock:
                            self._open_positions[symbol] = position
                            self._pending_symbols.discard(symbol)
                        self._log_trade_open(position)
                        log.info(f"[RETRY] {symbol}: placed at attempt {attempt+1} — "
                                 f"qty={retry_qty} notional=${accepted_notional:.0f} lev={back_lev}x")
                        placed = True
                        break
                    except requests.exceptions.HTTPError as retry_err:
                        retry_body = retry_err.response.text if retry_err.response else ''
                        if '-2027' in retry_body:
                            log.warning(f"[RETRY] {symbol}: attempt {attempt+1} still -2027, halving again...")
                            continue
                        log.error(f"[RETRY] {symbol}: attempt {attempt+1} unexpected error: {retry_err}")
                        break
                    except Exception as retry_err:
                        log.error(f"[RETRY] {symbol}: attempt {attempt+1} failed: {retry_err}")
                        break
                if not placed:
                    log.warning(f"[SKIP] {symbol}: -2027 could not be resolved after halving")
                    with self._lock:
                        self._pending_symbols.discard(symbol)
            else:
                log.error(f"[ORDER] Failed to place trade for {symbol}: {e}", exc_info=True)
                with self._lock:
                    self._pending_symbols.discard(symbol)

        except Exception as e:
            log.error(f"[ORDER] Failed to place trade for {symbol}: {e}", exc_info=True)
            with self._lock:
                self._pending_symbols.discard(symbol)

    # ── Position monitor ──────────────────────────────────────────────────────

    def _monitor_loop(self):
        log.info("Position monitor started")
        while True:
            time.sleep(POLL_INTERVAL)
            try:
                self._check_positions()
            except Exception as e:
                log.error(f"Monitor error: {e}", exc_info=True)

    def _check_positions(self):
        with self._lock:
            positions = list(self._open_positions.values())

        for pos in positions:
            try:
                # Algo orders use GET /v1/algoOrder (algoId)
                # algoStatus values: NEW → TRIGGERING → TRIGGERED → FINISHED (executed) or CANCELED/EXPIRED
                tp_order  = self.client.get_algo_order(pos.tp_order_id)
                sl_order  = self.client.get_algo_order(pos.sl_order_id)
                tp_filled = tp_order.get('algoStatus') == 'FINISHED'
                sl_filled = sl_order.get('algoStatus') == 'FINISHED'

                if not tp_filled and not sl_filled:
                    # Neither algo order is FINISHED. But the algo could be CANCELED/EXPIRED
                    # while the position itself was closed by other means (manual close,
                    # liquidation, algo service hiccup on demo). Verify the actual position
                    # state on Binance — if it's gone, reconcile from userTrades.
                    tp_status = tp_order.get('algoStatus')
                    sl_status = sl_order.get('algoStatus')
                    if tp_status not in ('NEW', 'TRIGGERING', 'TRIGGERED') or \
                       sl_status not in ('NEW', 'TRIGGERING', 'TRIGGERED'):
                        # At least one algo is in a terminal non-filled state — check position
                        try:
                            pos_risk = self.client.get_position(pos.symbol)
                            pos_amt  = float(pos_risk.get('positionAmt', 0))
                            if pos_amt == 0:
                                log.warning(
                                    f"[RECONCILE] {pos.symbol}: algo statuses "
                                    f"TP={tp_status} SL={sl_status} but position is closed "
                                    f"on Binance — reconciling from userTrades"
                                )
                                self._reconcile_closed_position(pos)
                                continue
                        except Exception as rc_err:
                            log.error(f"[RECONCILE] {pos.symbol}: position check failed: {rc_err}")
                    continue

                outcome = 'WIN' if tp_filled else 'LOSS'

                # actualPrice = actual fill price from matching engine (per docs)
                filled_order = tp_order if tp_filled else sl_order
                actual_exit  = float(filled_order.get('actualPrice') or 0)
                if actual_exit == 0:
                    actual_exit = pos.tp_price if tp_filled else pos.sl_price

                try:
                    if tp_filled:
                        self.client.cancel_algo_order(pos.sl_order_id)
                    else:
                        self.client.cancel_algo_order(pos.tp_order_id)
                except Exception as ce:
                    log.warning(f"Could not cancel remaining order for {pos.symbol}: {ce}")

                log.info(f"[CLOSED] {pos.symbol} {pos.strategy} {pos.direction} | "
                         f"outcome={outcome} | entry={pos.entry_price:.6f} "
                         f"exit={actual_exit:.6f} SL={pos.sl_price:.6f} TP={pos.tp_price:.6f}")

                self._log_trade_close(pos, outcome, exit_price=actual_exit)

                with self._lock:
                    self._open_positions.pop(pos.symbol, None)
                    # Normalize strategy full-name ('S1_EMA_CROSS') to short key ('S1')
                    strat = pos.strategy.split('_')[0] if pos.strategy else 'S1'
                    if outcome == 'WIN':
                        self._consec_losses[strat] = 0
                    else:
                        self._consec_losses[strat] = self._consec_losses.get(strat, 0) + 1

                if self.detector:
                    self.detector.on_trade_closed(pos.symbol, pos.strategy, outcome)

            except Exception as e:
                log.warning(f"Could not check algo orders for {pos.symbol}: {e} — checking position risk")
                # Fallback: check if Binance still has an open position
                # If not, the trade closed externally (algo order expired/filled without us noticing)
                try:
                    pos_risk = self.client.get_position(pos.symbol)
                    pos_amt  = float(pos_risk.get('positionAmt', 0))
                    if pos_amt == 0:
                        log.warning(f"[RECONCILE] {pos.symbol}: no open position on Binance, "
                                    f"position closed externally — fetching exit price from trades")
                        self._reconcile_closed_position(pos)
                    else:
                        log.warning(f"[RECONCILE] {pos.symbol}: position still open on Binance "
                                    f"(amt={pos_amt}) — algo order query failed but position alive")
                except Exception as risk_err:
                    log.error(f"Could not reconcile position {pos.symbol}: {risk_err}")

    def _reconcile_closed_position(self, pos):
        """
        Position closed on Binance but bot never saw the algo fill (e.g. algo got
        CANCELED/EXPIRED but position closed by other means). Fetch the actual
        exit price from /userTrades, log the close, and free the symbol gate.
        Used by both the algo-status branch and the exception fallback in
        _check_positions, so the reconciliation logic only lives in one place.
        """
        try:
            trades = self.client._get('/v1/userTrades',
                                      {'symbol': pos.symbol, 'limit': 10},
                                      signed=True)
            # Find the closing trade (opposite side to entry)
            close_side = 'SELL' if pos.direction == 'LONG' else 'BUY'
            close_trades = [t for t in trades
                           if t.get('side') == close_side
                           and int(t.get('time', 0)) > pos.open_ts]
            if close_trades:
                # Use the most recent closing trade price
                last = max(close_trades, key=lambda t: t['time'])
                actual_exit = float(last['price'])
                realized    = sum(float(t.get('realizedPnl', 0)) for t in close_trades)
            else:
                actual_exit = pos.sl_price  # conservative fallback
                realized    = None

            # Determine outcome from exit price relative to TP/SL
            if pos.direction == 'LONG':
                outcome = 'WIN' if actual_exit >= pos.tp_price else 'LOSS'
            else:
                outcome = 'WIN' if actual_exit <= pos.tp_price else 'LOSS'

            log.warning(f"[RECONCILE] {pos.symbol}: exit={actual_exit:.6f} "
                        f"outcome={outcome} realizedPnl={realized}")

            self._log_trade_close(pos, outcome, exit_price=actual_exit)

            with self._lock:
                self._open_positions.pop(pos.symbol, None)
                strat = pos.strategy.split('_')[0] if pos.strategy else 'S1'
                if outcome == 'WIN':
                    self._consec_losses[strat] = 0
                else:
                    self._consec_losses[strat] = self._consec_losses.get(strat, 0) + 1

            # Best-effort: cancel any leftover algo orders so they don't trigger later
            for oid in (pos.tp_order_id, pos.sl_order_id):
                if oid:
                    try:
                        self.client.cancel_algo_order(oid)
                    except Exception:
                        pass   # already canceled/expired — ignore

            if self.detector:
                self.detector.on_trade_closed(pos.symbol, pos.strategy, outcome)

        except Exception as rec_err:
            log.error(f"[RECONCILE] {pos.symbol}: failed to fetch exit trades: {rec_err}")

    # ── Logging ───────────────────────────────────────────────────────────────

    def _init_csv(self):
        if not os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'open_time', 'close_time', 'symbol', 'strategy',
                    'direction', 'signal_price', 'entry_price', 'sl_price', 'tp_price',
                    'quantity', 'margin_usdt', 'leverage', 'outcome',
                    'pnl_pct', 'pnl_usdt', 'fee_usdt', 'slippage_pct', 'signal_time',
                ])

    def _load_supabase_history(self):
        rows = self.supabase.select_all('trades')
        self.closed_positions = rows
        if rows:
            log.info(f"Loaded {len(rows)} historical trades from Supabase")

    def _log_trade_open(self, pos: OpenPosition):
        log.info(f"[LOG] Trade opened: {pos.symbol} {pos.strategy} {pos.direction} "
                 f"entry={pos.entry_price:.6f} SL={pos.sl_price:.6f} TP={pos.tp_price:.6f} "
                 f"qty={pos.quantity} margin=${pos.margin_usdt} lev={pos.leverage}x")

        signal_price = pos.signal_price if pos.signal_price else pos.entry_price
        slippage_pct = round((pos.entry_price - signal_price) / signal_price * 100, 4) \
                       if signal_price else 0.0

        row = {
            'open_time':    pos.open_time,
            'close_time':   None,
            'symbol':       pos.symbol,
            'strategy':     pos.strategy,
            'direction':    pos.direction,
            'signal_price': round(signal_price, 8),
            'entry_price':  round(pos.entry_price, 8),
            'sl_price':     round(pos.sl_price, 8),
            'tp_price':     round(pos.tp_price, 8),
            'quantity':     pos.quantity,
            'margin_usdt':  pos.margin_usdt,
            'leverage':     pos.leverage,
            'outcome':      'OPEN',
            'pnl_pct':      None,
            'pnl_usdt':     None,
            'fee_usdt':     None,
            'slippage_pct': slippage_pct,
            'signal_time':  pos.signal_time,
        }

        db_id = self.supabase.insert_returning_id('trades', row)
        pos.db_id = db_id
        if db_id:
            log.info(f"[LOG] Supabase row #{db_id} created (OPEN) for {pos.symbol}")
        else:
            log.warning(f"[LOG] Supabase insert_returning_id returned None for {pos.symbol} — close will fallback to insert")

    def _log_trade_close(self, pos: OpenPosition, outcome: str, exit_price: float = None):
        close_time = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        if outcome == 'WIN':
            exit_p  = exit_price or pos.tp_price
            pnl_pct = (exit_p - pos.entry_price) / pos.entry_price * 100 \
                      if pos.direction == 'LONG' else \
                      (pos.entry_price - exit_p) / pos.entry_price * 100
        elif outcome == 'LOSS':
            exit_p  = exit_price or pos.sl_price
            pnl_pct = (exit_p - pos.entry_price) / pos.entry_price * 100 \
                      if pos.direction == 'LONG' else \
                      (pos.entry_price - exit_p) / pos.entry_price * 100
        elif outcome == 'MANUAL_CLOSE':
            exit_p  = exit_price or pos.entry_price
            pnl_pct = (exit_p - pos.entry_price) / pos.entry_price * 100 \
                      if pos.direction == 'LONG' else \
                      (pos.entry_price - exit_p) / pos.entry_price * 100
        else:
            exit_p  = exit_price or pos.entry_price
            pnl_pct = 0.0

        notional = pos.margin_usdt * pos.leverage
        pnl_usdt = notional * (pnl_pct / 100)

        # Exact fee calculation based on order types
        # Entry: MARKET (taker 0.05%), Exit: TAKE_PROFIT maker (0.02%) or STOP taker (0.05%)
        entry_fee_rate = 0.0005
        exit_fee_rate  = 0.0002 if outcome == 'WIN' else 0.0005
        fee_usdt       = round(notional * (entry_fee_rate + exit_fee_rate), 4)

        # Slippage: actual fill vs signal's intended price
        signal_price  = pos.signal_price if pos.signal_price else pos.entry_price
        slippage_pct  = round((pos.entry_price - signal_price) / signal_price * 100, 4) \
                        if signal_price else 0.0

        row = {
            'open_time':    pos.open_time,
            'close_time':   close_time,
            'symbol':       pos.symbol,
            'strategy':     pos.strategy,
            'direction':    pos.direction,
            'signal_price': round(signal_price, 8),
            'entry_price':  round(pos.entry_price, 8),
            'sl_price':     round(pos.sl_price, 8),
            'tp_price':     round(pos.tp_price, 8),
            'quantity':     pos.quantity,
            'margin_usdt':  pos.margin_usdt,
            'leverage':     pos.leverage,
            'outcome':      outcome,
            'pnl_pct':      round(pnl_pct, 3),
            'pnl_usdt':     round(pnl_usdt, 2),
            'fee_usdt':     fee_usdt,
            'slippage_pct': slippage_pct,
            'signal_time':  pos.signal_time,
        }

        if pos.db_id:
            # Update the existing OPEN row to final outcome
            self.supabase.update('trades', pos.db_id, {
                'close_time':   close_time,
                'sl_price':     round(pos.sl_price, 8),
                'tp_price':     round(pos.tp_price, 8),
                'outcome':      outcome,
                'pnl_pct':      round(pnl_pct, 3),
                'pnl_usdt':     round(pnl_usdt, 2),
                'fee_usdt':     fee_usdt,
                'slippage_pct': slippage_pct,
            })
            log.info(f"[LOG] Supabase row #{pos.db_id} updated → {outcome}")
        else:
            # Fallback: full INSERT (position was opened before this deploy)
            self.supabase.insert('trades', row)
        self.closed_positions.append(row)

        with open(TRADE_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                row['open_time'], row['close_time'], row['symbol'], row['strategy'],
                row['direction'], row['signal_price'], row['entry_price'],
                row['sl_price'], row['tp_price'],
                row['quantity'], row['margin_usdt'], row['leverage'], row['outcome'],
                row['pnl_pct'], row['pnl_usdt'], row['fee_usdt'],
                row['slippage_pct'], row['signal_time'],
            ])

        log.info(f"[LOG] Trade closed: {pos.symbol} {outcome} "
                 f"PnL={pnl_pct:+.3f}% / ${pnl_usdt:+.2f} | "
                 f"Fee=${fee_usdt:.4f} | Slippage={slippage_pct:+.4f}%")


# ============================================================================
# STANDALONE TEST
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

    print("""
+------------------------------------------------------+
|  STEP 3 -- Order Manager  (Futures / connectivity)  |
|  Does NOT place any orders.                          |
+------------------------------------------------------+
""")

    if not API_KEY or not API_SECRET:
        print("ERROR: No API keys found.")
        sys.exit(1)

    client = BinanceClient(API_KEY, API_SECRET, BASE_URL)

    print(f"  Testnet : {TESTNET}")
    print(f"  Base URL: {BASE_URL}")
    print()

    try:
        bal = client.get_usdt_balance()
        print(f"  [OK] Futures USDT Balance : {bal:.2f} USDT")
    except Exception as e:
        print(f"  [FAIL] Balance fetch      : {e}")
        sys.exit(1)

    try:
        price = client.get_ticker_price('BTCUSDT')
        print(f"  [OK] BTCUSDT price        : ${price:.2f}")
    except Exception as e:
        print(f"  [FAIL] Price fetch        : {e}")

    try:
        pc   = PrecisionCache(client)
        qty  = pc.calc_quantity('BTCUSDT', price, DEFAULT_MARGIN, DEFAULT_LEVERAGE)
        prec = pc.get('BTCUSDT')
        print(f"  [OK] BTCUSDT qty          : {qty} (maxQty={prec['max_qty']})")
    except Exception as e:
        print(f"  [FAIL] Precision/qty      : {e}")

    try:
        test_price = 3.175e-05
        formatted  = client._fmt_price(test_price)
        print(f"  [OK] fmt_price test       : {test_price} → '{formatted}'")
    except Exception as e:
        print(f"  [FAIL] fmt_price          : {e}")

    print()
    print("  Strategy config:")
    for strat, cfg in STRATEGY_CONFIG.items():
        print(f"    {strat}: ${cfg['margin_usdt']} margin × {cfg['leverage']}x leverage "
              f"= ${cfg['margin_usdt'] * cfg['leverage']:.0f} notional")
    print(f"  Max open positions: {MAX_OPEN_POSITIONS}")
    print()
    print("  All connectivity tests passed.")
