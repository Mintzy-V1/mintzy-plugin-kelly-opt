"""AutoTrader (strategy B): __init__, main loop (start), and shutdown.

The bulk of AutoTrader's methods live in the mixins imported below; this file
was split out of the original monolithic auto_trader_exposure_expansion.py
verbatim (no logic changes) purely for maintainability.
"""
import csv
import json
import logging
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dt_time

import pandas as pd

from alerts import AlertManager

# ====================================================================
from trading_state import trading_snapshot

print("TRADER snapshot id:", id(trading_snapshot))

from .analysis import AnalysisMixin
from .async_csv_logger import AsyncCsvLogger
from .broker_cash import BrokerCashMixin
from .constants import DEFAULT_MONGO_CONFIG_DB_NAME, MARKET_TZ
from .eod_exit import EodExitMixin
from .exit_tracking import ExitTrackingMixin
from .exposure import ExposureMixin
from .fills import FillsMixin
from .leverage import LeverageMixin
from .live_pnl import LivePnlMixin
from .order_execution import OrderBatcher, OrderRequest
from .paper_state import PaperStateMixin
from .price_feed import PriceFeedMixin
from .pyramid import PyramidMixin
from .pyramid_config import PyramidConfigMixin
from .session import SessionMixin
from .timing import TimingLogger
from .trade_logging import TradeLoggingMixin

class AutoTrader(
    LivePnlMixin,
    ExitTrackingMixin,
    ExposureMixin,
    FillsMixin,
    LeverageMixin,
    PyramidConfigMixin,
    BrokerCashMixin,
    PyramidMixin,
    PaperStateMixin,
    SessionMixin,
    PriceFeedMixin,
    EodExitMixin,
    AnalysisMixin,
    TradeLoggingMixin,
):
    def __init__(self, prediction_client, market_client, broker=None, alerts=None,
                 initial_capital=None,
                 get_access_token=None,
                 log_dir=None,
                 trading_logs_collection=None):
        self._last_executed_candle = None
        self.pred_client = prediction_client
        self.market_client = market_client
        self.broker = broker
        self.alerts = alerts if alerts is not None else AlertManager()
        self.initial_capital = initial_capital if initial_capital is not None else 0.0
        self.current_capital = self.initial_capital
        self.cash_balance = self.initial_capital
        self.get_access_token = get_access_token
        self.trading_logs_collection = trading_logs_collection
        self.config_db_name = (
            os.environ.get("MONGO_CONFIG_DB_NAME")
            or DEFAULT_MONGO_CONFIG_DB_NAME
        )
        self.max_exposure_pct = 1.00
        self.reserved_exposure = {}  
        self.symbol_locks = {}        
        self.broker_pos_lock = threading.Lock()
        self._broker_positions_cache = []

        # ==================== PAPER TRADING STATE ====================
        self._paper_orders = {}
        self._paper_positions = {}
        self._paper_lock = threading.Lock()
        self._paper_order_counter = 0
        self.paper_slippage_pct = 0.0  # configurable later; 0 = no slippage
        self._restored_cycle_count = 0
        self._current_cycle_count = 0
        self.session = None
        self.broker_live_session = None
        self.broker_session_payload = None
        self.session_free_cash = None
        self.configuration_id = None
        self.leverage_multiplier = 1
        self.simulation_logs = True
        # =============================================================
        # ====== CANDLE TIMESTAMP (for correct logging) ======
        self.current_cycle_ts = None
        self.current_cycle_ts_str = None
        # ================ RISK/EXPOSURE MANAGEMENT ================
        self.min_trade_pct = 0.05
        self.max_trade_pct = 0.15
        # ===================================================
        
        default_log_dir = os.environ.get("MINTZY_LOGS_DIR", "logs")
        self.log_dir = os.path.abspath(log_dir or default_log_dir)
        os.makedirs(self.log_dir, exist_ok=True)

        self.log_path = os.path.join(self.log_dir, "trade_log.csv")
        self.portfolio_log = os.path.join(self.log_dir, "portfolio_log.csv")

        self.positions = {}
        self.symbol_allocations = {}
        
        from collections import defaultdict
        self.pending_orders = defaultdict(list)

        self.pending_lock = threading.Lock()
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0.0
        self.total_loss = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        from collections import defaultdict as _dd
        self.realized_pnl_by_symbol = _dd(float)  # cumulative realized PnL per symbol
        self.trade_history = []
        self.stop_event = threading.Event()
        self._eod_exit_done = False
        self._eod_exit_in_progress = False
        self._eod_exit_lock = threading.Lock()
        self._session_open_qty = {}
        self._session_open_qty_lock = threading.Lock()
        self._exit_warning_sent = False
        self.positions_lock = threading.Lock()   #  ADD THIS LINE
        self._exited_symbols = set()  # Symbols manually exited - excluded from future cycles
        self._exited_symbols_index_ready = False

        # ==================== RMS: RISK MANAGEMENT SYSTEM ====================
        self.rms_triggered = False          # True once daily loss limit is hit
        self.rms_loss_limit = 0.0           # Set dynamically from capital at session start
        self.portfolio_max_loss_pct = 0.005 # 0.5% of total allocated capital -> halt ALL trading
        # ======================================================================

        # qty locked at first entry per symbol  reused for entire session
        self.symbol_qty: dict = {}

        # Held qty per symbol = broker-confirmed fill at entry (tradebook/order
        # status value), tracked separately from symbol_qty (the intended target).
        # Never recomputed from LTP. Used as the basis for exit quantity.
        self._confirmed_fill_qty: dict = {}

        # ==================== PARALLEL EXECUTION SETUP ====================
        self.parallel_executor = None
        self.use_parallel_execution = True  # Set False to disable parallel execution
        # ==================================================================

        # ==================== LIVE LTP STREAM (observational) ====================
        # Populated by on_ltp_tick() from a background WS thread.
        # NOT used by the existing PnL math  pure side-channel for the UI.
        self.live_pnl: dict = {}                 # {symbol: {ltp, pnl, qty, entry, side, ts}}
        self.live_pnl_lock = threading.Lock()
        self._live_pnl_last_write: dict = {}     # throttle CSV writes per symbol
        self._live_pnl_last_redis_write: float = 0.0   # throttle Redis writes (max 1/sec globally)
        self.live_pnl_log = os.path.join(self.log_dir, "live_pnl_log.csv")
        self.portfolio_pnl_log = os.path.join(self.log_dir, "portfolio_pnl_log.csv")
        self.rms_events_log = os.path.join(self.log_dir, "rms_events_log.csv")

        # Single async writer for all tick-driven CSVs  keeps the WS thread fast.
        self._csv_logger = AsyncCsvLogger(name=f"AsyncCsv-{getattr(self, 'session_id', 'trader')}")
        self._csv_logger.register(
            self.live_pnl_log,
            ["Timestamp", "Symbol", "Side", "Qty", "Entry_Price", "LTP", "PnL"],
        )
        self._csv_logger.register(
            self.portfolio_pnl_log,
            ["Timestamp", "Total_PnL", "Realized_PnL", "Live_Unrealized",
             "Limit", "Usage_Pct", "Open_Symbols"],
        )
        self._csv_logger.register(
            self.rms_events_log,
            ["Timestamp", "Event_Type", "Symbol", "PnL", "Realized",
             "Unrealized", "Threshold", "Action"],
        )

        # Per-ticker RMS layer (independent of portfolio-level RMS).
        # Trips when gross PnL (realized + unrealized) <= -(1% of entry_price * qty).
        # self.rms_per_ticker_loss_per_share = 9.8
        self.rms_per_ticker_loss_pct = 0.01
        self._rms_exit_inflight: set = set()

        # Tick-driven portfolio RMS  uses the SAME rms_loss_limit set by the
        # existing cycle-level path; only the trigger source differs (live ticks
        # vs end-of-cycle aggregation). Dedup flag distinct from rms_triggered.
        self._live_portfolio_rms_inflight = False

        # Diagnostics for tick-flow visibility (throttled).
        self._tick_first_seen_in_trader: set = set()
        self._last_portfolio_print_ts = 0.0
        self._portfolio_print_interval_sec = 30.0
        self._last_per_ticker_print_ts: dict = {}    # per-symbol last print
        self._per_ticker_print_interval_sec = 30.0
        if not os.path.exists(self.live_pnl_log):
            with open(self.live_pnl_log, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["Timestamp", "Symbol", "Side", "Qty", "Entry_Price", "LTP", "PnL"]
                )
        # =========================================================================

         # ==================== TIMING LOGGER ====================
        # Initialised here so every method can call self.tlog.record(...)
        # The actual log_dir may not exist yet  TimingLogger creates it.
        self.tlog = TimingLogger(self.log_dir)
        # ========================================================

        if not os.path.exists(self.log_path):
            with open(self.log_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Symbol", "Signal", "Change(%)",
                    "Action_Status", "Price",
                    "P&L", "Total_Capital", "Return(%)"
                ])

        if not os.path.exists(self.portfolio_log):
            with open(self.portfolio_log, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Symbol", "Action", "Qty", "Entry_Price",
                    "Exit_Price", "P&L", "Cumulative_P&L", "Total_Capital",
                    "Return_On_Trade(%)", "Portfolio_Return(%)"
                ])
    
    # ---------- MAIN LOOP ----------
    def start(self, symbols, time_frame="5 minutes", candle_for_client=None,
              parameters=["close"], user_positions=None, initial_allocations = None,
              min_required_cash=0.0, stop_on_insufficient=True,
              use_broker_cash_as_capital=True, leverage_multiplier=None):
        
        if leverage_multiplier is not None:
            coerced = self._coerce_leverage_multiplier(leverage_multiplier, "start_kwarg")
            if coerced is not None:
                self.leverage_multiplier = coerced
        resolved_leverage = self._resolve_leverage_multiplier(default=1.0)
        print(f"[LEVERAGE] Active session leverage multiplier: x{resolved_leverage}")

        # ==================== CANDLE NORMALIZATION ====================
        candle = (candle_for_client or "5m").lower().strip()

        if not candle.endswith("m"):
            candle = "5m"

        step = int(candle[:-1])
        # ==============================================================

        # ---- EARLY MARKET-HOURS GUARD (IST) ----
        market_now = self._now_market_time()
        now_time = market_now.time()

        # NSE cash market typical intraday window
        market_open  = dt_time(9, 15)   # 9:15 AM IST
        market_close = dt_time(15, 20)  # 3:20 PM IST (your existing cutoff)

        #temp change 
        # Block weekends or outside this time window
        if market_now.weekday() >= 5 or not (market_open <= now_time <= market_close):
            msg = (
                f"Market closed in IST. Now: "
                f"{market_now.strftime('%Y-%m-%d %H:%M:%S')} AutoTrader will not start."
            )
            print("[INFO]", msg)
            self.alerts.notify(msg)
            return
        # ----------------------------------------

        try:
            self._link_broker()
        except Exception as e:
            print(f"Failed to initialize paper session during start(): {e}")
            self.alerts.notify("Failed to initialize paper trading session")
            return

        free_cash = self._get_free_cash()
        if free_cash is None:
            print("WARNING: Could not determine available cash. Aborting start() for safety.")
            self.alerts.notify("Could not determine available cash. Stopping AutoTrader for safety.")
            return

        print(f"Available broker/paper cash: {free_cash:,.2f}")
        self.alerts.notify(f"Available cash: {free_cash:,.2f}")

        if use_broker_cash_as_capital:
            if not getattr(self, "_restored_cycle_count", 0):
                self.initial_capital = free_cash
                self.current_capital = free_cash
                self.cash_balance = free_cash
            print(f"[INFO] Using broker cash as initial capital: {self.cash_balance:,.2f}")
            self.alerts.notify(f"Initial Capital set to broker cash: {self.cash_balance:,.2f}")
        else:
            if not getattr(self, "_restored_cycle_count", 0):
                self.cash_balance = free_cash
                self.current_capital = free_cash
            print(
                f"[INFO] Simulation paper ledger seeded from broker cash: {self.cash_balance:,.2f} "
                f"(configured default was {self.initial_capital:,.2f})"
            )
            self.alerts.notify(f"Simulation ledger seeded from broker cash: {self.cash_balance:,.2f}")

        if stop_on_insufficient and free_cash < float(min_required_cash):
            self.alerts.notify(
                f"Insufficient funds to start trading: available {free_cash:,.2f} < required {min_required_cash:,.2f}. Halting."
            )
            print(f"Insufficient funds: required {min_required_cash:.2f}, available {free_cash:.2f}. Exiting.")
            return

        batch_size = 3
        symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        
        if initial_allocations:
            self.symbol_allocations = {
                sym: {
                    "capital": alloc["capital"],
                    "stop_loss": alloc.get("stop_loss")
                }
                for sym, alloc in initial_allocations.items()
            }

        # RMS loss limit based on total capital allocated across all symbols
        total_allocated = sum(a["capital"] for a in self.symbol_allocations.values()) if self.symbol_allocations else self.initial_capital
        # self.rms_loss_limit = -(2303.0 / 1_000_000) * total_allocated
        self.rms_loss_limit = -(self.portfolio_max_loss_pct * total_allocated)
        print(f"[RMS] Total allocated capital: Rs{total_allocated:,.2f} | Loss limit: Rs{self.rms_loss_limit:.2f} ({self.portfolio_max_loss_pct*100:.2f}% of capital)")
        self.alerts.notify(f"RMS Loss Limit: Rs{self.rms_loss_limit:.2f} ({self.portfolio_max_loss_pct*100:.2f}% of Rs{total_allocated:,.2f} allocated)")

        # Seed today's already-realized PnL from broker so a same-day restart
        # carries forward prior closed-trade PnL into the RMS calculations.
        self._seed_realized_pnl_from_broker()

        cycle_count = int(getattr(self, "_restored_cycle_count", 0) or 0)
        sync_counter = 0
        
        # ---- START BACKGROUND RECONCILIATION THREAD ----
        if not hasattr(self, "_reconcile_thread"):
            self._reconcile_thread = threading.Thread(
                target=self._reconcile_pending_orders,
                daemon=True
            )
            self._reconcile_thread.start()

        self._last_executed_candle = None

        while True:
            try: 
                if self.stop_event.is_set():
                    print("[AUTO_TRADER] Stop signal mila  shutdown ho raha hoon...")
                    self.shutdown()   #  shutdown call karo, woh khud exit karega
                    break    

                # ==================== FILTER EXITED SYMBOLS ====================
                # If any symbols were manually exited, remove them from the active list
                self._sync_exited_symbols_from_db()
                if self._exited_symbols:
                    before_count = len(symbols)
                    symbols = [
                        s for s in symbols
                        if self._normalize_config_symbol(s) not in self._exited_symbols
                    ]
                    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
                    removed = self._exited_symbols.copy()
                    # Don't clear _exited_symbols - keep them excluded permanently
                    if len(symbols) < before_count:
                        print(f"[SINGLE EXIT] Removed {removed} from active symbols. Remaining: {symbols}")
                
                if not symbols:
                    print("[AUTO_TRADER] All symbols have been exited - no more symbols to trade. Stopping.")
                    self.stop_event.set()
                    break
                # ==============================================================

                print("pending orders:", self.pending_orders)
                # =====================================================
                # DOUBLE-EXECUTION GUARD (ONE EXECUTION PER CANDLE)
                # =====================================================
                now = self._now_market_time()

                #temp change 
                warning_time = dt_time(15, 25)  # 3:25 PM IST
                if now.time() >= warning_time and not self._exit_warning_sent:
                    msg = " 2:25 PM - Market closing in 5 minutes. All positions will be exited at 1:30 PM."
                    print(f"\n{msg}")
                    self.alerts.notify(msg)
                    self._exit_warning_sent = True
                
                # EXIT ALL POSITIONS AT 3:40 PM IST
                market_exit_time = dt_time(15, 40)  # 3:40 PM IST
                
                if now.time() >= market_exit_time:
                    print(f"\n[MARKET CLOSE] Current time: {now.strftime('%H:%M:%S')} - Initiating shutdown")
                    
                    # Exit all positions
                    self._exit_all_positions_and_stop()
                    
                    # Stop the trader
                    self.stop_event.set()
                    break
                # -------- HARD CANDLE BOUNDARY GATE --------
            
                # We only execute when minute is exactly on the boundary AND we're within the first few seconds
                # -------- CANDLE-KEY EXECUTION GATE (NO SKIP IF LATE) --------

                candle_key = self._get_candle_key(now, candle)

                print(
                    f"[DEBUG] now={now.strftime('%H:%M:%S')} "
                    f"candle_key={candle_key} "
                    f"candle={candle}"
                )

                # Prevent executing same candle twice
                if self._last_executed_candle == candle_key:
                    print(f"[SKIP] Candle {candle_key} already executed")
                    self._sleep_until_next_candle(candle)
                    continue

                if self.stop_event.is_set():
                    print(f"[SKIP] Stop requested  skipping candle {candle_key}")
                    break

                #  LOCK CANDLE IMMEDIATELY (IMPORTANT)
                self._last_executed_candle = candle_key

                # Compute lateness (drift) for debugging
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)
                    lateness = (now - candle_dt).total_seconds()
                    if lateness > step * 60:
                        print(f"[DRIFT] Late candle execution: {lateness:.1f}s behind for {candle_key}")
                except Exception:
                    pass

                # ====== CANDLE TIME FIX (this will be used for ALL logs in this cycle) ======
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M")
                    self.current_cycle_ts = candle_dt.replace(tzinfo=MARKET_TZ)
                    self.current_cycle_ts_str = self.current_cycle_ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    self.current_cycle_ts = None
                    self.current_cycle_ts_str = None
                # ==========================================================================

                print(f"[EXECUTE] Candle {candle_key} at {now.strftime('%H:%M:%S')}")
         # ==================== CYCLE TIMING START ====================
                t_cycle_start = time.time()
                self.tlog.start_cycle(cycle_count + 1, candle_key)
                # ============================================================
                if user_positions:
                    for sym, pos in user_positions.items():
                        if sym not in self.positions and pos:
                            self.positions[sym] = pos.copy()
                            
                cycle_count += 1
                self._current_cycle_count = cycle_count
                sync_counter += 1

                if sync_counter >= 5 and len(self.positions) == 0:
                    self._sync_cash_with_broker()
                    sync_counter = 0
                
                # ====== DEBUG START ======
                print(f"\n{'='*80}")
                print(f"[AUTO_TRADER] PREDICTION CYCLE START")
                print(f"[AUTO_TRADER] Candle: {candle_key}")
                print(f"[AUTO_TRADER] symbols input: {symbols}")
                print(f"[AUTO_TRADER] len(symbols): {len(symbols)}")
                print(f"[AUTO_TRADER] symbol_batches: {symbol_batches}")
                print(f"[AUTO_TRADER] len(symbol_batches): {len(symbol_batches)}")
                print(f"[AUTO_TRADER] time_frame: {time_frame}")
                print(f"[AUTO_TRADER] candle: {candle}")
                print(f"[AUTO_TRADER] parameters: {parameters}")
                print(f"{'='*80}\n")
                # ====== DEBUG END ======

                merged_df_list = []
                t_pred_start = time.time()
                print(f"[TIMING] PREDICTION_BATCH_START  at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                #parallel order execution for batches of symbols
                

                candle_step = int(candle[:-1]) if candle.endswith("m") else 5
                prediction_candle = candle if candle_step <= 5 else "5m"
                if prediction_candle != candle:
                    print(f"[CANDLE] candle={candle} > 5m, using prediction_candle={prediction_candle} for _run_batch")
                
                def _run_batch(batch):
                    print("inside the run batch function being called from for loop")
                    t0 = time.time()
                    result = self.pred_client.get_prediction_once(
                        batch, time_frame,
                        parameters=parameters,
                        candle=prediction_candle,
                        single_run=True,
                        debug=False
                    )
                    print("prediction call returned from function now check the response time")
                    print(f"[_run_batch] {batch}  completed in {time.time() - t0:.2f}s")
                    self.tlog.record(
                        f"BATCH_PREDICTION",
                        t0,
                        note=f"symbols={batch}"
                        )
                    return result

                with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                    print("calling run batch function for batches")
                    futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                    for fut in as_completed(futures):
                        batch = futures[fut]
                        try:
                            t_start = time.time()
                            print(f"[BATCH TIMER] {batch}  waiting for result... ({datetime.now().strftime('%H:%M:%S')})")
                            df = fut.result(timeout=300)
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch}  got response in {elapsed:.2f}s ({elapsed/60:.2f} min)")
                            print("response coming from prediction service df with timeout 300", df)
                            if df is None:
                                print("response is none from predicton service df is none")
                                self.alerts.notify(f"No response for batch {batch}")
                                continue
                            if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                                print("response has error column from predicton service ")
                                self.alerts.notify(f"Prediction error for batch {batch}")
                                continue
                            print("df response coming from the predicton service ", df)
                            merged_df_list.append(df)
                            print("merged df list is : ",merged_df_list)
                        except Exception as e:
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch}  FAILED after {elapsed:.2f}s  {e}")

                            self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                            logging.exception("Batch failed")
                

                if not merged_df_list:
                    print("merged df list is empty continue now it will skip the current candle and call the function sleep until next candle")
                    self.alerts.notify("No valid prediction data returned; retrying next cycle...")
                    print("calling sleep_until_next_candle")
                    self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note="EMPTY_RESULT")

                    self._sleep_until_next_candle(candle)
                    continue

                df = pd.concat(merged_df_list, axis=0)

                # =====================================
                # LOAD SESSION TRENDS FROM REDIS
                # =====================================
                session_trends = {}

                t_trend_total = time.time()
                for sym in symbols:
                    try:
                        t_sym = time.time()
                        redis_key = f"TREND:{sym}"  
                        t_redis = time.time()
                        raw = self.market_client.redis_client.get(redis_key)
                        redis_time = time.time() - t_redis
                        print(
                            f"[TREND REDIS] {sym} | "
                            f"latency={redis_time:.4f}s | "
                            f"status={'MISS' if not raw else 'HIT'}"
                        )
                        self.tlog.record(
                                "TREND_REDIS_GET",
                                t_redis,
                                note=f"{sym}|{'MISS' if not raw else 'HIT'}"
                            )

                        if redis_time > 0.05:
                            print(f"SLOW REDIS: {sym} took {redis_time:.3f}s")
                        
                        if not raw:
                            continue

                        trend = json.loads(raw)

                        session_trends[sym] = {
                            "direction": int(trend.get("direction", 0)),
                            "session_open": float(trend.get("session_open", 0.0)),
                            "last_price": float(trend.get("last_price", 0.0)),
                        }
                        total_sym_time = time.time() - t_sym
                        print(
                            f"[TREND TOTAL] {sym} | total_time={total_sym_time:.4f}s"
                        )

                        self.tlog.record(
                            "TREND_PER_SYMBOL_TOTAL",
                            t_sym,
                            note=sym
                        )

                    except Exception as e:
                        print(f"[REDIS WARN] {sym}: {e}")



                print(f"[TREND TOTAL] {sym} | total_time={t_trend_total:.4f}s")

                self.tlog.record(
                                    "TREND_TOTAL",
                                    t_trend_total,
                                    note=sym
                                )
                        
                #  Store candle so _log_trade can fetch correct redis price
                self._current_candle = candle

                #  ONE fresh broker call per cycle  BEFORE all PnL calcs
                t_broker_pos = time.time()
                with self.broker_pos_lock:
                    self._broker_positions_cache = self._get_broker_positions()
                    broker_positions = list(self._broker_positions_cache)
                self.tlog.record("BROKER_POS_FETCH", t_broker_pos, note=f"positions={len(broker_positions)}")

                print(f"[CACHE] {len(broker_positions)} open positions refreshed for cycle")
  
                t_analyze = time.time()
                signals = self._analyze(
                    df, candle,
                    self.positions,
                    session_trends=session_trends
                )
                self._cycle_ltp_cache = {
                    sym: float(info["curr_price"])
                    for sym, info in signals.items()
                    if info.get("curr_price")
                }
                self.tlog.record("ANALYZE_SIGNALS", t_analyze, note=f"symbols={len(signals)}")

                market_now = self._now_market_time()
                t_after_brp_call = time.time()
                print(f"\nCYCLE #{cycle_count} - {market_now.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Cash Balance: {self.cash_balance:,.2f}")
                print("=" * 120)
                print(
                    f"{'Symbol':<10} {'Curr_Price':>12} {'Trajectory%':>12} "
                    f" {'Side':>6} {'Signal':<35} {'Action Taken':<40}"
                )
                print("-" * 120)

                session_id = getattr(self, "ui_session_id", "default")
                ui_rows = []
                cycle_orders_sent = set()

                # ========== PARALLEL ORDER EXECUTION - PHASE 1: COLLECT ORDERS ==========
                order_batcher = OrderBatcher()
                
                # ---- BROKER TRUTH POSITION CHECK ----
                t_lock_wait = time.time()

                 # ----- broker_pos_lock wait timer (second cache read) -----
                t_lock_wait = time.time()
                with self.broker_pos_lock:
                    _lock_elapsed = round(time.time() - t_lock_wait, 4)
                    _ = self._broker_positions_cache  # just access
                if _lock_elapsed > 0.05:
                    print(f"[TIMING] BROKER_POS_LOCK_WAIT (signal loop)     {_lock_elapsed:>7.3f}s  >50ms  potential contention")
                    self.tlog._write("BROKER_POS_LOCK_WAIT", _lock_elapsed, note="signal_loop")

                print("starting the for loop having 400 lines of code in between")
                
                
                for sym, info in signals.items():
                    t_sym_loop = time.time()
                    symbol_action_taken = False
                    try:   
                        has_broker_pos = False
                        broker_pos = None
                        
                        print(f"[DEBUG TOP] sym={sym} has_broker_pos={has_broker_pos}")

                        broker_pos = next(
                            (p for p in broker_positions if p["symbol"] == sym),
                            None
                        )

                        has_broker_pos = bool(broker_pos and broker_pos.get("qty", 0) > 0)

                        # Force internal state to match broker
                        if not has_broker_pos and sym not in self.pending_orders:
                            self.positions.pop(sym, None)  
                            
                        print(f"[DEBUG TOP] sym={sym} has_broker_pos={has_broker_pos}")
                        
                        sig = info["signal"]
                        change_pct = info["change_pct"]
                        curr_price = info["curr_price"]
                        side = broker_pos["side"] if has_broker_pos else "NONE"
                        
                        # ===============================
                        # RISK VETO GUARD (EXIT ONLY)
                        # ===============================
                        risk_veto = info.get("_risk_veto", False)

                        # If risk veto is active, allow ONLY exit / cover actions
                        if risk_veto:
                            # Block all OPEN / REVERSE logic
                            if any(k in sig for k in (
                                "Start Long",
                                "Start Short",
                                "Reverse to Long",
                                "Reverse to Short",
                                "OPEN_LONG",
                                "OPEN_SHORT"
                            )):
                                continue
                        
                        # Avoiding noise.
                        risk_regime = info.get("risk_regime", 0)
                        if risk_regime == 0:
                            continue
                        
                        # SCENARIO 0: Hard Stop Loss (Still handled sequentially for safety)
                        if (
                                has_broker_pos
                                and sym in self.symbol_allocations
                                and self._is_position_settled(broker_pos)
                                and sym not in self.pending_orders
                            ):
                            sl_pct = self.symbol_allocations[sym].get("stop_loss")
                            if sl_pct is not None and sl_pct > 0:
                                entry_price = broker_pos["avg_price"]
                                qty = broker_pos["qty"]
                                position_side = broker_pos["side"]
                                if entry_price <= 0 or qty <= 0 or not position_side:
                                    continue
                                stop_hit = False
                                exit_side = None
                                if position_side == "BUY":
                                    stop_price = entry_price * (1 - sl_pct)
                                    if curr_price <= stop_price:
                                        stop_hit = True
                                        exit_side = "SELL"
                                elif position_side == "SELL":
                                    stop_price = entry_price * (1 + sl_pct)
                                    if curr_price >= stop_price:
                                        stop_hit = True
                                        exit_side = "BUY"
                                if position_side == "BUY" and curr_price > entry_price:
                                    stop_hit = False
                                if position_side == "SELL" and curr_price < entry_price:
                                    stop_hit = False

                                if stop_hit:
                                    print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                    order_batcher.add_order(
                                        OrderRequest(
                                            sym,
                                            exit_side,
                                            qty,
                                            metadata={
                                                "signal": "STOP_LOSS",
                                                "action_type": "STOP_LOSS",
                                                "curr_price": curr_price,
                                                "side": exit_side,
                                                "qty": qty,
                                                "order_value": curr_price * qty,
                                                "position_side": position_side,
                                                "pre_exit_position": self._snapshot_position_for_exit(sym, broker_pos),
                                                "exit_reason": "STOP_LOSS_EXIT",
                                            }
                                        ),
                                        "exit"
                                    )
                                    symbol_action_taken = True
                                    action_taken = "STOP-LOSS ORDER SENT"
                                    symbol_unrealized_pnl = 0.0
                                    symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
                                    symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)
                                    ui_rows.append({
                                        "symbol": sym,
                                        "curr_price": round(curr_price, 2),
                                        "return_pct": round(info["change_pct"], 4),
                                        "side": "NONE",
                                        "signal": "STOP-LOSS",
                                        "action": action_taken,
                                        "qty": int(qty),
                                        "unrealized_pnl": symbol_unrealized_pnl,
                                        "symbol_unrealized_pnl": symbol_unrealized_pnl,
                                        "symbol_realized_pnl": symbol_realized_pnl,
                                        "symbol_pnl": symbol_pnl,
                                        "pnl": symbol_pnl,
                                    })
                                    
                                    print(f"{sym:<10} {curr_price:>12.2f} {info['change_pct']:>12.6f} "
                                        f"{position_side:>6} {'STOP-LOSS':<35} {action_taken:<40}")
                        
                        print(f"[DEBUG TOP] symbol_action_taken : {symbol_action_taken}")
                        # Ensuring One signal per cycle
                        if symbol_action_taken:
                            continue
                        
                        print(f"[DEBUG] risk_veto={risk_veto} sig={sig}")
                            
                        # SCENARIO 1: OPEN LONG
                        if sig == "BUY" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "BUY (Fresh Long Entry)"

                            if sym not in self.symbol_qty:
                                if sym in self.symbol_allocations:
                                    capital = self.symbol_allocations[sym]["capital"]
                                else:
                                    capital = self.cash_balance * 0.1
                                self.symbol_qty[sym] = int(capital / curr_price)

                            qty = self.symbol_qty[sym] # lock the price 
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    print("[DEBUG] exposure blocked", sym, order_value)
                                    continue
                                self._reserve_exposure(sym, order_value)
                                
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "OPEN_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # SCENARIO 2: OPEN SHORT
                        if sig == "SELL" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "SELL (Fresh Short Entry)"

                            if sym not in self.symbol_qty:
                                if sym in self.symbol_allocations:
                                    capital = self.symbol_allocations[sym]["capital"]
                                else:
                                    capital = self.cash_balance * 0.1
                                self.symbol_qty[sym] = int(capital / curr_price)

                            qty = self.symbol_qty[sym]
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "OPEN_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "sell"
                            )
                            continue
                        
                        # SCENARIO 3: EXIT LONG & REVERSE TO SHORT
                        if sig == "SELL" and has_broker_pos and broker_pos["side"] == "BUY" and sym not in self.pending_orders:
                            scenario_name = "SELL (Flip Long to Short)"
                            qty = self._confirmed_fill_qty.get(sym) or broker_pos["qty"]
                            inverted_qty = qty*2                 # EXIT LONG -> OPEN SHORT (same qty)
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN SHORT (1 QTY)
                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    inverted_qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "FLIP_TO_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": inverted_qty,
                                        "order_value": curr_price * inverted_qty
                                    }
                                ),
                                "sell"
                            )
                            continue
                        
                        # SCENARIO 4: EXIT SHORT & REVERSE TO LONG
                        if sig == "BUY" and has_broker_pos and broker_pos["side"] == "SELL" and sym not in self.pending_orders:
                            scenario_name = "BUY (Flip Short to Long)"
                            qty = self._confirmed_fill_qty.get(sym) or broker_pos["qty"]
                            inverted_qty = qty*2                # EXIT SHORT -> OPEN LONG (same qty)
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")
                    
                            # OPEN LONG (1 QTY)
                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    inverted_qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "FLIP_TO_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": inverted_qty,
                                        "order_value": curr_price * inverted_qty
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # ================================
                        # SCENARIO 5: LONG EXPANSION
                        # ================================
                        if sig == "BUY" and has_broker_pos and broker_pos["side"] == "BUY" and sym not in self.pending_orders and risk_regime == 2:
                            scenario_name = "BUY (Position Expansion Long)"
                            qty = self.symbol_qty.get(sym, 0)
                            if qty <= 0:
                                continue
                            
                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "EXPAND_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # ================================
                        # SCENARIO 7: SHORT EXPANSION
                        # ================================
                        if sig == "SELL" and has_broker_pos and broker_pos["side"] == "SELL" and sym not in self.pending_orders and risk_regime == 2:
                            scenario_name = "SELL (Position Expansion Short)"
                            qty = self.symbol_qty.get(sym, 0)
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                                
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "EXPAND_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "sell"
                            )
                            elapsed = time.time() - t_sym_loop

                            print(f"[TRACE] SYMBOL_LOOP {sym}: {elapsed:.4f}s")

                            self.tlog.record("SYMBOL_LOOP_TOTAL", t_sym_loop, note=sym)

                            continue
      
                    except Exception as e:
                        self.alerts.notify(f"[SYMBOL ERROR] {sym}: {e}")
                        continue
                
                # ========== PHASE 2: EXECUTE ALL ORDERS IN PARALLEL ==========
                if not order_batcher.is_empty() and self.use_parallel_execution:
                    print(f"\n[PARALLEL] Executing {order_batcher.get_count()} orders concurrently...")
                    
                    t_parallel_exec = time.time()
                    
                    self._ensure_parallel_executor()
                    all_orders = order_batcher.get_all_orders(priority="exit_first")
                    results = self.parallel_executor.submit_orders(all_orders)
                    
                    self.tlog.record("PARALLEL_ORDER_EXEC", t_parallel_exec, note=f"orders={len(all_orders)}")


                    symbols_to_fetch = [req.symbol for req in all_orders]
                    def _fetch_ltp_safe(sym):
                        try:
                            price = self._get_live_price_redis(sym, candle)
                            print(f"[LTP] inside _fetch_ltp_safe {sym}: {price}")  
                            return sym, price
                        except Exception:
                            return sym, None

                    ltp_cache = {}
                    print('before the thrreadpool executer of fetchltp')
                    tlog_fetch_ltp_start = time.time()  
                    with ThreadPoolExecutor(max_workers=len(symbols_to_fetch)) as pool:
                        print('inside the thrreadpool executer of fetchltp now fetch ltp will be called')
                        futures = {pool.submit(_fetch_ltp_safe, sym): sym for sym in symbols_to_fetch}
                        for future in as_completed(futures):
                            sym, price = future.result()
                            if price:
                                ltp_cache[sym] = price
                            

                    tlog_fetch_ltp_end = time.time()  
                    print("time taken in fetchltp_price",tlog_fetch_ltp_end-tlog_fetch_ltp_start )
                    self.tlog.record("FETCH_LTP", tlog_fetch_ltp_end, note=f"symbols={len(symbols_to_fetch)}")  


                                
                    # ========== PHASE 3: PROCESS RESULTS ==========
                    for result in results:
                        t_result = time.time()
                        sym = result.symbol.upper().replace("-EQ", "")
                        metadata = result.metadata or {}
                        requested_value = metadata.get("order_value", 0.0)
            
                        action_type = metadata.get("action_type", "")
                        sig = metadata.get("signal", "")
                        change_pct = metadata.get("change_pct", 0.0)
                        curr_price = metadata.get("curr_price", 0.0)
                        
                        if result.success and result.filled:
                            cycle_orders_sent.add(sym)
                            avg_price = result.avg_price
                            filled_qty = result.filled_qty
                            pnl = 0.0
                            action_taken = ""
                            
                            if action_type == "EXIT_LONG":
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"CLOSED LONG ({filled_qty}) {avg_price:.2f} | P&L:{pnl:,.2f}"
                            
                            elif action_type == "OPEN_SHORT":
                                action_taken = f"OPEN SHORT ORDER SENT ({filled_qty})"
                                self._confirmed_fill_qty[sym] = int(filled_qty or 0)
                                self._log_trade(
                                    sym,
                                    "OPEN_SHORT",
                                    change_pct,
                                    "filled",
                                    avg_price,
                                    filled_qty,
                                    0.0
                                )
                            
                            elif action_type in ["COVER_SHORT"]:
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"COVERED SHORT ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                            
                            elif action_type == "STOP_LOSS":
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"STOP LOSS EXITED ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                            
                            elif action_type == "OPEN_LONG":
                                action_taken = f"OPEN LONG ORDER SENT ({filled_qty})"
                                self._confirmed_fill_qty[sym] = int(filled_qty or 0)
                                self._log_trade(
                                    sym,
                                    "OPEN_LONG",
                                    change_pct,
                                    "filled",
                                    avg_price,
                                    filled_qty,
                                    0.0
                                )

                            order_value = metadata.get("order_value", 0.0)
                            if order_value:
                                try:
                                    self._release_exposure(sym, order_value)
                                except Exception as _re:
                                    print(f"[FILL] {sym}: release_exposure failed: {_re}")
                                                          
                        else:
                            # Order failed at Angel before getting an order_id
                            # (validation rejects like AB1019 / AB4036, RMS rejects, etc).
                            # Do NOT add to pending_orders  there is nothing to reconcile.
                            # Release the reserved exposure so the symbol can trade again
                            # in later cycles. Log it and move on to the next order.
                            if not result.order_id:
                                order_value = metadata.get("order_value", 0.0)
                                try:
                                    self._release_exposure(sym, order_value)
                                except Exception as _re:
                                    print(f"[REJECT] {sym}: release_exposure failed: {_re}")

                                live_price = ltp_cache.get(sym) or curr_price or 0.0
                                if not live_price:
                                    try:
                                        live_price = self._get_live_price_redis(sym, candle) or 0.0
                                    except Exception:
                                        live_price = 0.0

                                err_msg = result.error or "unknown error"
                                print(f"[REJECT] {sym} {action_type}: {err_msg}  skipping, cycle continues")
                                try:
                                    self.alerts.notify(f"Order rejected for {sym} ({action_type}): {err_msg}")
                                except Exception:
                                    pass
                                try:
                                    self._log_trade(sym, action_type, change_pct, "rejected",
                                                    live_price, metadata.get("qty", 0), 0.0)
                                except Exception as _le:
                                    print(f"[REJECT] {sym}: log_trade failed: {_le}")
                                continue

                            with self.pending_lock:
                                self.pending_orders[sym].append({
                                    "order_id": result.order_id,
                                    "action_type": action_type,
                                    "side": metadata.get("side"),
                                    "qty": metadata.get("qty"),
                                    "order_value": metadata.get("order_value", 0.0),
                                    "position_side": metadata.get("position_side"),
                                    "pre_exit_position": metadata.get("pre_exit_position"),
                                    "exit_reason": metadata.get("exit_reason"),
                                    "placed_at": time.time(),
                                    # "metadata": metadata   # redundant
                                })
                            if action_type in {"EXIT_LONG", "COVER_SHORT", "STOP_LOSS"}:
                                pre_exit = metadata.get("pre_exit_position") or self._snapshot_position_for_exit(sym)
                                self._persist_exit_pending(
                                    sym,
                                    exit_reason=self._resolve_exit_reason(action_type, metadata),
                                    exit_side=metadata.get("side"),
                                    qty=metadata.get("qty"),
                                    entry_price=pre_exit.get("entry_price", 0.0),
                                    exit_order_id=result.order_id,
                                )
                            cycle_orders_sent.add(sym)

                            # Log with live redis price instead of 0.0
                            t_ltp = time.time()

                            live_price = ltp_cache.get(sym) or curr_price
                            if live_price is None:
                                live_price = self._get_live_price_redis(sym, candle) or curr_price

                            ltp_time = time.time() - t_ltp

                            print(f"[TRACE] RESULT_LTP from get_live_price_redis {sym}: {ltp_time:.3f}s")

                            self.tlog.record("RESULT_LTP_FETCH", t_ltp, note=sym)
                            
                            t_log = time.time()

                            self._log_trade(sym, action_type, change_pct, "pending", live_price, metadata.get("qty", 0), 0.0)
                            log_time = time.time() - t_log

                            print(f"[TRACE] LOG_TRADE {sym}: {log_time:.3f}s")

                            self.tlog.record("LOG_TRADE_TIME", t_log, note=sym)

                            print(f"  {sym}: {action_type} PENDING (order sent) @ {live_price:.2f}")

                            result_total = time.time() - t_result

                            print(f"[TRACE] RESULT_TOTAL {sym}: {result_total:.3f}s")

                            self.tlog.record("RESULT_PROCESS_TOTAL", t_result, note=sym)


                with self.pending_lock:
                    pending_syms = set(self.pending_orders.keys())

                # Paper fills are synchronous  refresh positions before hold/UI snapshot.
                with self.broker_pos_lock:
                    self._broker_positions_cache = self._get_broker_positions()
                    broker_positions = list(self._broker_positions_cache)

                # ========== CONTINUE WITH HOLD POSITIONS ==========
                for sym, info in signals.items(): 
                    t_sym = time.time()            
                    broker_pos = next(
                            (p for p in broker_positions if p["symbol"] == sym),
                            None
                        )
                        

                    has_broker_pos = bool(broker_pos and broker_pos.get("qty", 0) > 0)
                    
                    sig = info["signal"]
                    change_pct = info["change_pct"]
                    curr_price = info["curr_price"]
                    side = broker_pos["side"] if has_broker_pos else "NONE"

                    if has_broker_pos:
                        if broker_pos["side"] == "BUY":
                            action_taken = "HOLD (Continue Long)"
                        else:
                            action_taken = "HOLD (Continue Short)"
                    else:
                        action_taken = "HOLD (Flat)"
                    
                    #  Calculate live PnL once here for all hold paths
                    t_pnl = time.time()

                    live_pnl = self._calculate_pnl(sym, curr_price) if has_broker_pos else 0.0
                    
                    pnl_time = time.time() - t_pnl

                    print(f"[TRACE] PNL {sym}: {pnl_time:.4f}s")

                    self.tlog.record("PNL_CALC", t_pnl, note=sym)
                    held_qty = broker_pos.get("qty", 0) if has_broker_pos else 0

                    # SCENARIO 8 : WAIT NO POSITION
                    if (
                        not has_broker_pos
                        and sym not in pending_syms
                        and sym not in cycle_orders_sent
                        and self._stock_exposure(sym) == 0
                    ):
                        action_taken = "WAIT (no position)"
                        self._log_trade(sym, sig, change_pct, "wait", curr_price, 0, 0.0)
                    
                    # SCENARIO 9 : PENDING STATUS (order sent this cycle or awaiting reconcile)
                    elif sym in pending_syms or sym in cycle_orders_sent:
                        action_taken = "PENDING (order sent)"
                        self._log_trade(sym, sig, change_pct, "pending", curr_price, held_qty, live_pnl)

                    #  SCENARIO 10 : HOLD WITH OPEN POSITION  log with live PnL
                    elif has_broker_pos:
                        # self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)
                        t_log = time.time()

                        self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)

                        log_time = time.time() - t_log

                        print(f"[TRACE] LOG_TRADE {sym}: {log_time:.3f}s")

                        self.tlog.record("HOLD_LOG_TRADE", t_log, note=sym) 
                    
                    symbol_unrealized_pnl = round(live_pnl, 2)
                    symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
                    symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)

                    ui_rows.append({
                        "symbol": sym,
                        "curr_price": round(curr_price, 2),
                        "return_pct": round(change_pct, 4),
                        "side": side,
                        "signal": sig,
                        "action": action_taken,
                        "qty": int(held_qty),
                        "unrealized_pnl": symbol_unrealized_pnl,
                        "symbol_unrealized_pnl": symbol_unrealized_pnl,
                        "symbol_realized_pnl": symbol_realized_pnl,
                        "symbol_pnl": symbol_pnl,
                        "pnl": symbol_pnl,
                    })


                    elapsed = time.time() - t_sym

                    print(f"[TRACE] HOLD_LOOP {sym}: {elapsed:.3f}s")

                    self.tlog.record("HOLD_LOOP_PER_SYMBOL", t_sym, note=sym)
                    
                    print(f"{sym:<10} {curr_price:>12.2f} {change_pct:>12.6f} "
                        f"{side:>6} {sig:<35} {action_taken:<40}")

                t_pnl_loop = time.time()

                self.unrealized_pnl = 0.0

                for sym, info in signals.items():
                    t_pnl = time.time()

                    curr_price = info["curr_price"]
                    pnl = self._calculate_pnl(sym, curr_price)

                    self.unrealized_pnl += pnl

                    pnl_time = time.time() - t_pnl

                    print(f"[TRACE] PNL_LOOP {sym}: {pnl_time:.4f}s")

                    self.tlog.record("PNL_PER_SYMBOL", t_pnl, note=sym)

                # total loop
                print(f"[TRACE] PNL_LOOP_TOTAL: {time.time() - t_pnl_loop:.3f}s")

                self.tlog.record("PNL_LOOP_TOTAL", t_pnl_loop)
                self.current_capital = self.cash_balance + self.unrealized_pnl

                # ==================== RMS: DAILY LOSS LIMIT CHECK ====================
                # Cycle-level portfolio RMS removed  handled by tick-driven
                # _check_live_portfolio_rms() in on_ltp_tick (LiveLTPStream thread).
                # rms_loss_limit / rms_triggered are still set/used by that path.
                # ======================================================================

                t_ui = time.time()

                
                self._update_ui_snapshot(
                    session_id=session_id,
                    cycle=cycle_count,
                    rows=ui_rows,
                )

                ui_time = time.time() - t_ui

                print(f"[TRACE] UI_SNAPSHOT: {ui_time:.3f}s")

                self.tlog.record("UI_SNAPSHOT_TOTAL", t_ui)
                
                print(f"Realized PnL: {self.realized_pnl:.2f}")
                print(f"Unrealized PnL: {self.unrealized_pnl:.2f}")
                print(f"Total Equity: {self.current_capital:.2f}")


                # ==================== CYCLE TOTAL TIME ====================
                self.tlog.record("CYCLE_TOTAL", t_cycle_start, note=f"cycle={cycle_count}")
                total_cycle_sec = round(time.time() - t_cycle_start, 2)
                candle_budget_sec = step * 60
                if total_cycle_sec > candle_budget_sec * 0.8:
                    print(
                        f"[TIMING WARNING] Cycle took {total_cycle_sec:.1f}s / budget {candle_budget_sec}s "
                        f"({100*total_cycle_sec/candle_budget_sec:.0f}%)  RISK OF CANDLE SKIP!"
                    )

                # ==============================
                # BACKUP EXIT AT 3:20 PM CHECK
                # ==============================
                market_now = self._now_market_time()
                now_time = market_now.time()
                cutoff_time = dt_time(15, 20)
                print("time after analyse after second broker api call : ", time.time()- t_after_brp_call)
                self.tlog.record("time after analyse after second broker api call" ,t_after_brp_call , note="time analysis of delay")

                if now_time >= cutoff_time:
                    self.alerts.notify("Backup market close triggered (3:20 PM) - This shouldn't happen!")
                    print("\n" + "=" * 70)
                    print("BACKUP MARKET CLOSE - AUTO-TRADING STOPPED")
                    print("=" * 70)

                    print("\n[BACKUP EXIT] Attempting to exit remaining positions...")
                    self._exit_all_positions_and_stop()

                    self.stop_event.set()
                    break
                else:
                    self._sleep_until_next_candle(candle)
                    
            except RuntimeError as e:
                if "SESSION_EXPIRED_RELOGIN_REQUIRED" in str(e):
                    self.alerts.notify("Broker session expired. Manual restart required.")

                    self.stop_event.set()
                    break
                # Any other RuntimeError: log it, keep the cycle alive.
                print(f"[CYCLE WARN] RuntimeError (non-auth): {e}")
                try:
                    print(traceback.format_exc())
                except Exception:
                    pass
                try:
                    self.alerts.notify(f"Cycle warning (continuing): {e}")
                except Exception:
                    pass
                try:
                    self._sleep_until_next_candle(candle)
                except Exception:
                    time.sleep(1)
                continue
            except Exception as e:
                # Catch-all: never let the trading thread die because of a transient
                # parse / network / data error. Log, alert, sleep to next candle.
                print(f"[CYCLE WARN] Unhandled {type(e).__name__}: {e}")
                try:
                    print(traceback.format_exc())
                except Exception:
                    pass
                try:
                    self.alerts.notify(f"Cycle warning (continuing): {type(e).__name__}: {e}")
                except Exception:
                    pass
                try:
                    self._sleep_until_next_candle(candle)
                except Exception:
                    time.sleep(1)
                continue

    def shutdown(self):
        self.stop_event.set()
        print("[SHUTDOWN] Paper stop  applying capital pyramid update (no square-off)...")
        try:
            self._pyramid_handoff_result = self._apply_capital_pyramid_on_stop()
            print(
                f"[PYRAMID] handoff result: live_allowed={self._pyramid_handoff_result.get('live_allowed')} "
                f"reason={self._pyramid_handoff_result.get('reason')} "
                f"profitable_count={self._pyramid_handoff_result.get('profitable_count')}"
            )
        except Exception as e:
            print(f"[SHUTDOWN] Capital pyramid update failed: {e}")
            traceback.print_exc()
            self._pyramid_handoff_result = self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="pyramid_exception",
            )
        
        if hasattr(self, 'parallel_executor') and self.parallel_executor:
            self.parallel_executor.stop()
        self.reserved_exposure.clear()
        self.symbol_locks.clear()
        # Drain async CSV writer so no in-flight rows are lost
        if hasattr(self, '_csv_logger') and self._csv_logger:
            try:
                self._csv_logger.stop()
            except Exception as e:
                print(f"[SHUTDOWN] AsyncCsvLogger stop failed: {e}")


        
