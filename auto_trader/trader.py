import pandas as pd
import time
import csv
import os
import json
import requests
from datetime import datetime, time as dt_time, timedelta, timezone
from alerts import AlertManager
from broker_angle import BrokerConnector
import numpy as np
from orderbook import fetch_todays_intraday_orders
from concurrent.futures import ThreadPoolExecutor, as_completed


# ==================== PARALLEL EXECUTION IMPORTS ====================
import threading
from queue import Queue
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import threading
import logging
from trading_snapshot import insert_trading_snapshot
from utils.session_ledger import (
    apply_fill_to_session_ledger,
    record_engine_order_for_trader,
)
from utils.eod_exit import (
    try_begin_eod_exit,
    mark_eod_exit_done,
    release_eod_exit_in_progress,
    prepare_eod_exit_plan,
    exit_plan_to_broker_positions,
    finalize_eod_shutdown,
)

# ====================================================================

from trading_state import trading_snapshot

print("TRADER snapshot id:", id(trading_snapshot))

from .timing import TimingLogger, MARKET_TZ, load_json
from .order_execution import OrderRequest, OrderResult, RateLimiter, ParallelOrderExecutor, OrderBatcher
from .session_broker import SessionBrokerMixin
from .exposure_risk import ExposureRiskMixin
from .pricing import PricingMixin
from .fills_reconciliation import FillsReconciliationMixin
from .eod_exit import EODExitMixin
from .logging_ import LoggingMixin


class AutoTrader(
    SessionBrokerMixin,
    ExposureRiskMixin,
    PricingMixin,
    FillsReconciliationMixin,
    EODExitMixin,
    LoggingMixin,
):
    def __init__(self, prediction_client, market_client, broker=None, alerts=None,
                 initial_capital=196000,
                 get_access_token=None,
                 log_dir=None,
                 trading_logs_collection=None):
        self._last_executed_candle = None
        self.pred_client = prediction_client
        self.market_client = market_client
        self.broker = broker
        self.alerts = alerts if alerts is not None else AlertManager()
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.cash_balance = initial_capital
        self.get_access_token = get_access_token
        self.trading_logs_collection = trading_logs_collection
        self.max_exposure_pct = 1.00
        self.reserved_exposure = {}  
        self.symbol_locks = {}        
        self.broker_pos_lock = threading.Lock()
        self._broker_positions_cache = []
        # ====== CANDLE TIMESTAMP (for correct logging) ======
        self.current_cycle_ts = None
        self.current_cycle_ts_str = None       
        # ================ RISK/EXPOSURE MANAGEMENT ================
        self.min_trade_pct = 0.05
        # ====================================================
        
        
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
        self.trade_history = []
        self.stop_event = threading.Event()
        self._eod_exit_done = False
        self._eod_exit_in_progress = False
        self._eod_exit_lock = threading.Lock()
        self._session_open_qty = {}
        self._session_open_qty_lock = threading.Lock()
        self._exit_warning_sent = False


        # ==================== PARALLEL EXECUTION SETUP ====================
        self.parallel_executor = None
        self.use_parallel_execution = True  # Set False to disable parallel execution
        # ==================================================================

            # ==================== TIMING LOGGER ====================
        # Initialised here so every method can call self.tlog.record(...)
        # The actual log_dir may not exist yet â€” TimingLogger creates it.
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


    # ---------- TIME HELPERS ----------

    def _now_market_time(self):
        """Return current market datetime in IST."""
        return datetime.now(MARKET_TZ)

    def _sleep_until_next_candle(self, candle):

       

        candle = str(candle or "5m").lower().strip()
        step = int(candle[:-1]) if candle.endswith("m") else 5

        now = self._now_market_time()
        print("printing now: ", now)

        # Floor to current candle boundary
        minute_bucket = (now.minute // step) * step
        current_boundary = now.replace(
            minute=minute_bucket,
            second=0,
            microsecond=0
        )
        print("printing current_boundary: ", current_boundary)
        # Always target NEXT candle
        next_run = current_boundary + timedelta(minutes=step)
        print("Next run calculated as: ", next_run)

        # next_run = current_boundary + timedelta(minutes=step) + timedelta(seconds=60)


        # If we're already late, skip missed candles safely
        if next_run <= now:
            missed = int((now - next_run).total_seconds() // (step * 60)) + 1
            next_run += timedelta(minutes=missed * step)

        sleep_seconds = max(1, (next_run - now).total_seconds())

        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
    )



        deadline = time.time() + sleep_seconds
        while time.time() < deadline:
            if self.stop_event.is_set():
                print("[SCHEDULER] Stop signal mila neend mein â€” uth raha hoon!")
                return   # â† neend se uthta hai, loop pe wapas jaata hai
            time.sleep(1)

        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
        )

        # time.sleep(sleep_seconds)

    def _analyze(self, df, swing_interval, user_positions,session_trends=None):
        signals = {}
        for symbol, group in df.groupby("Ticker"):
            if "Timestamp" in group.columns:
                group["Timestamp"] = pd.to_datetime(group["Timestamp"])
                group = group.sort_values("Timestamp")

            predicted_path = group["Predicted Price"].values

            # Need current + 3 future points
            if len(predicted_path) < 4:
                continue

            # -------------------------------
            # CURRENT PRICE ANCHOR
            # -------------------------------
            live_price = getattr(self, "_cycle_ltp_cache", {}).get(symbol)

            print(
                f"[LIVE RESULT] {symbol} "
                f"live_price={live_price} "
                f"({'FALLBACK  predicted[0]' if live_price is None else 'USING LIVE'})",
                flush=True
            )
            
            # fallback to predicted first point if live fetch fails
            current_price = float(live_price) if live_price else float(predicted_path[0])
            
            print(
                f"[CURR PRICE FINAL] {symbol} curr_price={current_price}",
                flush=True
            )          
            
            # -------------------------------
            traj_col = group["trajectory_pct"].values if "trajectory_pct" in group.columns else None
            regime_col = group["risk_regime"].values if "risk_regime" in group.columns else None

            if traj_col is not None and len(traj_col) > 0 and not np.isnan(traj_col[0]):
                # Use slot 0's trajectory Ã¢â‚¬â€ the most current signal
                trajectory_pct = float(traj_col[0])
                risk_regime     = int(regime_col[0]) if regime_col is not None else (
                    0 if abs(trajectory_pct) < self.min_trade_pct else 1
                )
            else:
                # Fallback: recompute from raw prices
                first_candle   = predicted_path[0]
                last_candle    = predicted_path[3]
                trajectory_pct = ((last_candle - first_candle) / first_candle) * 100 if first_candle != 0 else 0.0
                abs_move       = abs(trajectory_pct)
                risk_regime    = 0 if abs_move < self.min_trade_pct else 1             
            # -------------------------------
            # POSITION CONTEXT
            # -------------------------------
            position = user_positions.get(symbol, {})
            position_side = position.get("side", "NONE")

            # ===============================
            # SESSION TREND VETO (HARD RULE)
            # ===============================
            session_direction = None
            session_open = None

            if session_trends and symbol in session_trends:
                session_direction = session_trends[symbol].get("direction")
                session_open = session_trends[symbol].get("session_open")

            # If LONG but session trend is DOWN FORCE EXIT
            if position_side == "BUY" and session_direction == -1 and session_open and current_price < session_open:
                signal = "SELL (Trend Veto Exit)"
                signals[symbol] = {
                    "signal": signal,
                    "change_pct": trajectory_pct,
                    "curr_price": current_price,
                    "side": position_side,
                    "interval": swing_interval,
                    "risk_regime": risk_regime
                }
                continue

            # If SHORT but session trend is UP FORCE COVER
            if position_side == "SELL" and session_direction == 1 and session_open and current_price > session_open:
                signal = "BUY (Trend Veto Exit)"
                signals[symbol] = {
                    "signal": signal,
                    "change_pct": trajectory_pct,
                    "curr_price": current_price,
                    "side": position_side,
                    "interval": swing_interval,
                    "risk_regime": risk_regime
                }
                continue

            signal = "HOLD"
            
            # =========================================================
            # STRICT NO-EXPANSION SIGNAL LOGIC 
            # =========================================================
            if position_side == "NONE":
                if trajectory_pct > 0:
                    signal = "BUY"
                elif trajectory_pct < 0:
                    signal = "SELL"
                else:
                    signal = "HOLD"

            elif position_side == "BUY":
                if trajectory_pct < 0:
                    signal = "SELL" 
                else:
                    signal = "HOLD"

            elif position_side == "SELL":
                if trajectory_pct > 0:
                    signal = "BUY"  
                else:
                    signal = "HOLD"


            # -------------------------------
            # OUTPUT
            # -------------------------------
            signals[symbol] = {
                "signal": signal,
                "change_pct": trajectory_pct,
                "curr_price": current_price,
                "side": position_side,
                "interval": swing_interval,
                "risk_regime": risk_regime
            }

        return signals        
    def convert_candle_to_seconds(self, c):
        c = str(c).lower().strip()

        if c.endswith("m"):
            return int(c[:-1]) * 60

        return 300

    # ---------- MAIN LOOP ----------

    def start(self, symbols, time_frame="5 minutes", candle_for_client=None,
              parameters=["close"], user_positions=None, initial_allocations = None,
              min_required_cash=0.0, stop_on_insufficient=True,
              use_broker_cash_as_capital=True):
        
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

        # Block weekends or outside this time window #temp block 
        if market_now.weekday() >= 5 or not (market_open <= now_time <= market_close):
            msg = (
                f"Market closed in IST. Now: "
                f"{market_now.strftime('%Y-%m-%d %H:%M:%S')} AutoTrader will not start."
            )
            print("[INFO]", msg)
            self.alerts.notify(msg)
            return
        # ----------------------------------------

        if not getattr(self, "session", None) or not getattr(self, "broker", None):
            try:
                self._link_broker()
            except Exception as e:
                print(f"Failed to link broker during start(): {e}")
                self.alerts.notify("Failed to link broker during start()")
                return

        free_cash = self._get_free_cash()
        if free_cash is None:
            print("WARNING: Could not determine account free cash/margin from broker. Aborting start() for safety.")
            self.alerts.notify("Could not determine account balance. Stopping AutoTrader for safety.")
            return

        print(f"Broker free cash / available margin: {free_cash:,.2f}")
        self.alerts.notify(f"Broker free cash / available margin: {free_cash:,.2f}")

        if use_broker_cash_as_capital:
            self.initial_capital = free_cash
            self.current_capital = free_cash
            self.cash_balance = free_cash
            print(f"[INFO]  Using broker cash as initial capital: {free_cash:,.2f}")
            self.alerts.notify(f"Initial Capital set to broker cash: {free_cash:,.2f}")
        else:
            print(f"[INFO] Using configured initial capital: {self.initial_capital:,.2f} (Broker has {free_cash:,.2f})")
            self.alerts.notify(f"Starting Capital: {self.initial_capital:,.2f}")
        
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

        cycle_count = 0
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
                    print("[AUTO_TRADER] Stop signal mila â€” shutdown ho raha hoon...")
                    self.shutdown()   # â† shutdown call karo, woh khud exit karega
                    break

                print("pending_orders:", self.pending_orders)  # Debug print

                if self.stop_event.is_set():
                    print("[AUTO_TRADER] Stop signal mila â€” loop band kar raha hoon...")
                    break
                
                # =====================================================
                # DOUBLE-EXECUTION GUARD (ONE EXECUTION PER CANDLE)
                # =====================================================
                now = self._now_market_time()
                # warning_time = dt_time(15, 25)  # 3:25 PM IST
                # #temp block 
                # # print("[DEBUG] Temp Bocked time")
                # if now.time() >= warning_time and not self._exit_warning_sent:
                #     msg = " 2:25 PM - Market closing in 5 minutes. All positions will be exited at 1:30 PM."
                #     print(f"\n{msg}")
                #     self.alerts.notify(msg)
                #     self._exit_warning_sent = True
                
                # # EXIT ALL POSITIONS AT 3:30 PM IST
                # market_exit_time = dt_time(15, 30) # 3:30 PM IST
                
                # if now.time() >= market_exit_time:
                #     print(f"\n[MARKET CLOSE] Current time: {now.strftime('%H:%M:%S')} - Initiating shutdown")
                    
                #     # Exit all positions
                #     self._exit_all_positions_and_stop()
                    
                #     # Stop the trader
                #     self.stop_event.set()
                #     break
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

                # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ LOCK CANDLE IMMEDIATELY (IMPORTANT)
                self._last_executed_candle = candle_key

                # Compute lateness (drift) for debugging
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)
                    lateness = (now - candle_dt).total_seconds()
                    if lateness > step * 60:
                        print(f"[DRIFT] Late candle execution: {lateness:.1f}s behind for {candle_key}")
                except Exception:
                    pass

                # # LOCK this candle immediately
                # self._last_executed_candle = candle_key
                # -------------------------------------------------------------


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
                print("intialised empty merged df list")
                merged_df_list = []

                  # ---- PREDICTION BATCH START ----
                t_pred_start = time.time()
                print(f"[TIMING] PREDICTION_BATCH_START  at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")


                #serial executer 
                # for batch_idx, batch in enumerate(symbol_batches):
                #     # ====== DEBUG START ======
                #     print(f"\n[AUTO_TRADER] >>> BATCH {batch_idx+1}/{len(symbol_batches)}: {batch}")
                #     # ====== DEBUG END ======
                #     try:
                #         print(
                #             f"[PRED CALL] batch={batch} "
                #             f"time_frame={time_frame} candle={candle}",
                #             flush=True
                #         )
                #         print(f"[AUTO_TRADER] Calling pred_client.get_prediction_once()...")
                #         df = self.pred_client.get_prediction_once(
                #             batch,
                #             time_frame,
                #             parameters=parameters,
                #             candle=candle,
                #             single_run=True,
                #             debug=False
                #         )
                        
                #         print(f"[AUTO_TRADER] Prediction call returned")
                #         print(f"[AUTO_TRADER]   df is None: {df is None}")
                        
                #         if df is None:
                #             print(f"[AUTO_TRADER] df is None, skipping batch")
                #             self.alerts.notify(
                #                 f"No response from prediction API for batch {batch}; skipping this batch."
                #             )
                #             continue
                        
                #         if isinstance(df, pd.DataFrame) and "Error" in df.columns and df.iloc[0].get("Error"):
                #             print(f"[AUTO_TRADER] df has Error column, skipping batch")
                #             self.alerts.notify(
                #                 f"Prediction API error for batch {batch}: {df.iloc[0].get('Error')}"
                #             )
                #             continue
                        
                #         print(f"[AUTO_TRADER] ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œ Valid df, appending to merged_df_list")
                #         merged_df_list.append(df)
                #         # #locked candle 
                #         # self._last_executed_candle = candle_key


                #     except Exception as e:
                #         self._last_executed_candle = None
                #         self.current_cycle_ts = None
                #         self.current_cycle_ts_str = None
                #         self.alerts.notify(f"[ERROR] Cycle failed: {e}")
                #         logging.exception("Main loop crashed")
                #         self._sleep_until_next_candle(candle)
                #         continue
                # lock candle AFTER all batches completed successfully

                #parallel executer for predictions 
 
                def _run_batch(batch):
                    print("inside the run batch function being called from for loop")
                    t0 = time.time()
                    result = self.pred_client.get_prediction_once(
                        batch, time_frame,
                        parameters=parameters,
                        candle=candle,
                        single_run=True,
                        debug=False
                    )
                    print("prediction call returned from function now check the response time")
                    print(f"[_run_batch] {batch} â†’ completed in {time.time() - t0:.2f}s")
                    self.tlog.record(
                        f"BATCH_PREDICTION",
                        t0,
                        note=f"symbols={batch}"
                        )
                    return result

                # with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                #     print("calling run batch function for batches")
                #     futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                #     for fut in as_completed(futures):
                #         batch = futures[fut]
                #         try:
                #             df = fut.result(timeout=240)
                #             print("response coming from prediction service df with timeout 240",df)

                #             if df is None:
                #                 print("df is none continue")
                #                 self.alerts.notify(f"No response for batch {batch}")
                #                 continue
                #             if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                #                 print("error in columns of df")
                #                 self.alerts.notify(f"Prediction error for batch {batch}")
                #                 continue
                #             print("mergin df list")
                #             merged_df_list.append(df)
                #         except Exception as e:
                #             print("error occured in exception block  ")
                #             self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                #             logging.exception("Batch failed")
                with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                    print("calling run batch function for batches")
                    futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                    for fut in as_completed(futures):
                        batch = futures[fut]
                        try:
                            t_start = time.time()
                            print(f"[BATCH TIMER] {batch} â†’ waiting for result... ({datetime.now().strftime('%H:%M:%S')})")

                            df = fut.result(timeout=300)

                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch} â†’ got response in {elapsed:.2f}s ({elapsed/60:.2f} min)")
                            print("response coming from prediction service df with timeout 300", df)

                            if df is None:
                                print("df is none continue")
                                self.alerts.notify(f"No response for batch {batch}")
                                continue
                            if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                                print("error in columns of df")
                                self.alerts.notify(f"Prediction error for batch {batch}")
                                continue
                            print("mergin df list")
                            merged_df_list.append(df)
                        except Exception as e:
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch} â†’ FAILED after {elapsed:.2f}s â€” {e}")
                            self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                            logging.exception("Batch failed")

                if not merged_df_list:
                    print("merged df list is empty continue now it will skip the current candle and call the function sleep until next candle")
                    self.alerts.notify("No valid prediction data returned; retrying next cycle...")
                    print("calling sleep_until_next_candle")
                    self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note="EMPTY_RESULT")
                    print( "total batch prediction me itna time laga : ",time.time()-t_pred_start)
                    self._sleep_until_next_candle(candle)
                    continue
                self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note=f"batches={len(symbol_batches)}")

                df = pd.concat(merged_df_list, axis=0)

                # =====================================
                # LOAD SESSION TRENDS FROM REDIS
                # =====================================
                session_trends = {}

                for sym in symbols:
                    try:
                        redis_key = f"TREND:{sym}"  
                        raw = self.market_client.redis_client.get(redis_key)

                        if not raw:
                            continue

                        trend = json.loads(raw)

                        session_trends[sym] = {
                            "direction": int(trend.get("direction", 0)),
                            "session_open": float(trend.get("session_open", 0.0)),
                            "last_price": float(trend.get("last_price", 0.0)),
                        }

                    except Exception as e:
                        print(f"[REDIS WARN] {sym}: {e}")

                print("[LTP] Fetching BULK broker prices...")
                t_ltp = time.time()


                self._cycle_ltp_cache = self._get_bulk_broker_ltp(symbols)

                print("[LTP] Received:", self._cycle_ltp_cache)

                # âœ… ONE fresh broker call per cycle â€” BEFORE analyze and BEFORE all PnL calcs
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
                self.tlog.record("ANALYZE_SIGNALS", t_analyze, note=f"symbols={len(signals)}")

                with self.broker_pos_lock:
                    self._broker_positions_cache = self._get_broker_positions()

                market_now = self._now_market_time()
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

                # ========== PARALLEL ORDER EXECUTION - PHASE 1: COLLECT ORDERS ==========
                order_batcher = OrderBatcher()
                
                # ---- BROKER TRUTH POSITION CHECK (already refreshed above) ----
                # broker_positions already set fresh above â€” reuse it here


               
                # ----- broker_pos_lock wait timer (second cache read) -----
                t_lock_wait = time.time()
                with self.broker_pos_lock:
                    _lock_elapsed = round(time.time() - t_lock_wait, 4)
                    _ = self._broker_positions_cache  # just access
                if _lock_elapsed > 0.05:
                    print(f"[TIMING] BROKER_POS_LOCK_WAIT (signal loop)     {_lock_elapsed:>7.3f}s  >50ms â€” potential contention")
                    self.tlog._write("BROKER_POS_LOCK_WAIT", _lock_elapsed, note="signal_loop")
                # -----------------------------------------------------------

                for sym, info in signals.items():
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
                                # Skip this signal entirely
                                continue
                        
                        # Avoiding noise
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
                                                "order_value": curr_price * qty
                                            }
                                        ),
                                        "exit"
                                    )
                                    symbol_action_taken = True
                                    action_taken = "STOP-LOSS ORDER SENT"
                                    ui_rows.append({
                                        "symbol": sym,
                                        "curr_price": round(curr_price, 2),
                                        "return_pct": round(info["change_pct"], 4),
                                        "side": "NONE",
                                        "signal": "STOP-LOSS",
                                        "action": action_taken,
                                        "unrealized_pnl": 0.0
                                    })
                                    
                                    print(f"{sym:<10} {curr_price:>12.2f} {info['change_pct']:>12.6f} "
                                        f"{position_side:>6} {'STOP-LOSS':<35} {action_taken:<40}")
                        
                        print(f"[DEBUG TOP] symbol_action_taken : {symbol_action_taken}")
                        # Ensuring One signal per cycle
                        if symbol_action_taken:
                            continue
                        
                        print(f"[DEBUG] risk_veto={risk_veto} sig={sig}")

                        # SCENARIO 6A: Trend Veto Exit LONG
                        if "SELL (Trend Veto Exit)" in sig:
                            long_pos = next(
                                (p for p in broker_positions if p["symbol"] == sym and p["side"] == "BUY"),
                                None
                            )

                            if long_pos:
                                print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                order_batcher.add_order(
                                    OrderRequest(
                                        sym,
                                        "SELL",
                                        long_pos["qty"],
                                        metadata={
                                            "signal": sig,
                                            "change_pct": change_pct,
                                            "action_type": "TREND_VETO_EXIT_LONG",
                                            "curr_price": curr_price,
                                            "side": "SELL",
                                            "position_side": "BUY",
                                            "qty": long_pos["qty"],
                                            "order_value": curr_price * long_pos["qty"]
                                        }
                                    ),
                                    "exit"
                                )
                                continue            
                            
                        # SCENARIO 6B: Trend Veto Exit SHORT
                        if "BUY (Trend Veto Exit)" in sig:
                                short_pos = next(
                                    (p for p in broker_positions if p["symbol"] == sym and p["side"] == "SELL"),
                                    None
                                )

                                if short_pos:
                                    print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                    order_batcher.add_order(
                                        OrderRequest(
                                            sym,
                                            "BUY",
                                            short_pos["qty"],
                                            metadata={
                                                "signal": sig,
                                                "change_pct": change_pct,
                                                "action_type": "TREND_VETO_EXIT_SHORT",
                                                "curr_price": curr_price,
                                                "side": "BUY",
                                                "position_side": "SELL",
                                                "qty": short_pos["qty"],
                                                "order_value": curr_price * short_pos["qty"]
                                            }
                                        ),
                                        "exit"
                                    )
                                    continue
                        
                        print(f"[DEBUG] before OPEN LONG: symbol_action_taken={symbol_action_taken}")
               
                        # SCENARIO 1: OPEN LONG
                        if sig == "BUY" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "BUY (Fresh Long Entry)"

                            if sym in self.symbol_allocations:
                                capital = self.symbol_allocations[sym]["capital"]
                            else:
                                capital = self.cash_balance * 0.1

                            qty = int(capital / curr_price)
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

                            if sym in self.symbol_allocations:
                                capital = self.symbol_allocations[sym]["capital"]
                            else:
                                capital = self.cash_balance * 0.1

                            qty = int(capital / curr_price)
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
                            qty = broker_pos["qty"]
                            inverted_qty = qty*2                 # EXIT LONG -> OPEN SHORT (DOUBLE QTY)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN SHORT (DOUBLE QTY)
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
                            qty = broker_pos["qty"]
                            inverted_qty = qty*2              # EXIT SHORT -> OPEN LONG (DOUBLE QTY)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN LONG (DOUBLE QTY)
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
                        
                        # ==================================================
                        # SAME-SIDE SIGNAL NO-OP                        
                        # ==================================================
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
                    # ========== PHASE 3: PROCESS RESULTS ==========
                    for result in results:
                        sym = result.symbol
                        metadata = result.metadata or {}
                        requested_value = metadata.get("order_value", 0.0)

                        # filled_value = (
                        #     result.avg_price * result.filled_qty
                        #     if result.success and result.filled
                        #     else 0.0
                        # )
            
                        action_type = metadata.get("action_type", "")
                        sig = metadata.get("signal", "")
                        change_pct = metadata.get("change_pct", 0.0)
                        curr_price = metadata.get("curr_price", 0.0)
                        
                        if result.success and result.filled:
                            avg_price = result.avg_price
                            filled_qty = result.filled_qty
                            pnl = 0.0
                            action_taken = ""
                            
                            if action_type == "EXIT_LONG":
                                pnl = self._close_position(self.session, sym, avg_price)
                                action_taken = f"CLOSED LONG ({filled_qty}) {avg_price:.2f} | P&L:{pnl:,.2f}"
                                self._log_trade(sym, "CLOSE_LONG", change_pct, "filled", avg_price, filled_qty, pnl)
                            
                            elif action_type == "OPEN_SHORT":
                                action_taken = f"OPEN SHORT ORDER SENT ({filled_qty})"
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
                                pnl = self._close_position(self.session, sym, avg_price)
                                action_taken = f"COVERED SHORT ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                                self._log_trade(sym, "CLOSE_SHORT", change_pct, "filled", avg_price, filled_qty, pnl)
                            
                            elif action_type == "OPEN_LONG":
                                action_taken = f"OPEN LONG ORDER SENT ({filled_qty})"
                                self._log_trade(
                                    sym,
                                    "OPEN_LONG",
                                    change_pct,
                                    "filled",
                                    avg_price,
                                    filled_qty,
                                    0.0
                                )

                        else:
                            with self.pending_lock:
                                self.pending_orders[sym].append({
                                    "order_id": result.order_id,
                                    "action_type": action_type,
                                    "side": metadata.get("side"),
                                    "qty": metadata.get("qty"),
                                    "order_value": metadata.get("order_value", 0.0),
                                    "placed_at": time.time(),
                                    # "metadata": metadata   # redundant
                                })

                            # âœ… live price log
                            live_price = getattr(self, "_cycle_ltp_cache", {}).get(sym) or curr_price
                            self._log_trade(sym, action_type, change_pct, "pending", live_price, metadata.get("qty", 0), 0.0)
                            print(f"  {sym}: {action_type} PENDING (order sent) @ {live_price:.2f}")
                
                with self.pending_lock:
                    pending_syms = set(self.pending_orders.keys())


                # ========== CONTINUE WITH HOLD POSITIONS ==========
                for sym, info in signals.items():             
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
                    
                    # âœ… Calculate live PnL once here for all hold paths
                    live_pnl = self._calculate_pnl(sym, curr_price) if has_broker_pos else 0.0
                    held_qty = broker_pos.get("qty", 0) if has_broker_pos else 0

                    # SCENARIO 9 : WAIT NO POSITION
                    if not has_broker_pos and sym not in pending_syms and self._stock_exposure(sym) == 0:
                        action_taken = "WAIT (no position)"
                        self._log_trade(sym, sig, change_pct, "wait", curr_price, 0, 0.0)
                    
                    # SCENARIO 10 : PENDING STATUS
                    elif sym in pending_syms:
                        action_taken = "PENDING (order sent)"
                        self._log_trade(sym, sig, change_pct, "pending", curr_price, held_qty, live_pnl)

                    # âœ… SCENARIO 11 : HOLD WITH OPEN POSITION â€” log with live PnL
                    elif has_broker_pos:
                        self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)
       
                    ui_rows.append({
                        "symbol": sym,
                        "curr_price": round(curr_price, 2),
                        "return_pct": round(change_pct, 4),
                        "side": side,
                        "signal": sig,
                        "action": action_taken,
                        "unrealized_pnl": round(self._calculate_pnl(sym, curr_price), 2),
                    })
                    
                    print(f"{sym:<10} {curr_price:>12.2f} {change_pct:>12.6f} "
                        f"{side:>6} {sig:<35} {action_taken:<40}")

                # ---- BROKER-ALIGNED UNREALIZED PNL (SINGLE SOURCE OF TRUTH) ----
                self.unrealized_pnl = 0.0
                for sym, info in signals.items():
                    curr_price = info["curr_price"]
                    self.unrealized_pnl += self._calculate_pnl(sym, curr_price)


                self.current_capital = self.cash_balance + self.unrealized_pnl
                
                self._update_ui_snapshot(
                    session_id=session_id,
                    cycle=cycle_count,
                    rows=ui_rows,
                )
                
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
                        f"({100*total_cycle_sec/candle_budget_sec:.0f}%) â€” RISK OF CANDLE SKIP!"
                    )

                # ==============================
                # BACKUP EXIT AT 3:20 PM CHECK
                # ==============================
                market_now = self._now_market_time()
                now_time = market_now.time()
                cutoff_time = dt_time(15, 20)

               

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
                raise                       
    # ==================== CLEANUP METHOD ====================
    # def shutdown(self):
    #     """Cleanup method - call this when stopping the bot"""
    #     if hasattr(self, 'parallel_executor') and self.parallel_executor:
    #         print("[CLEANUP] Stopping parallel order executor...")
    #         self.parallel_executor.stop()
    #         print("[CLEANUP]  Parallel executor stopped")
    #     # ---- RESET IN-MEMORY RISK STATE ----
    #     self.reserved_exposure.clear()
    #     self.symbol_locks.clear()
    #     # Optional: prevent reuse without re-init
    #     self.stop_event.set()
    # ========================================================

    def shutdown(self):
        print("[SHUTDOWN] Pehle open positions exit kar raha hoon...")
        try:
            self._exit_all_positions_and_stop()  # â† sirf yahan, ek baar
        except Exception as e:
            print(f"[SHUTDOWN] Exit failed: {e}")
        
        if hasattr(self, 'parallel_executor') and self.parallel_executor:
            self.parallel_executor.stop()
        self.reserved_exposure.clear()
        self.symbol_locks.clear()
        self.stop_event.set()