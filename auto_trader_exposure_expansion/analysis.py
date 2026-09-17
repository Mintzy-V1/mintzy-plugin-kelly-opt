"""AutoTrader mixin: UI snapshot persistence, cash sync, signal analysis, PnL
math, and position close bookkeeping. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
from typing import Optional

import numpy as np
import pandas as pd

from trading_snapshot import insert_trading_snapshot
from trading_state import trading_snapshot


class AnalysisMixin:

    # ---------- CASH / BALANCE ----------

    def _get_free_cash(self):
        broker_cash = self._fetch_broker_free_cash(context="INFO")
        if broker_cash is not None:
            return broker_cash

        free_cash = float(self.cash_balance)
        print(f"[INFO] Paper ledger cash (broker RMS unavailable): {free_cash:,.2f}")
        return free_cash
    
    # trading snapshot update karne ka function
    def _update_ui_snapshot(self, session_id, cycle, rows, persist_db=True):
        snapshot = {
            "cycle": cycle,
            "timestamp": (self.current_cycle_ts_str or self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")),
            "cash_balance": round(self.cash_balance, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "pnl":round(self.unrealized_pnl+self.realized_pnl , 2),
            "total_equity": round(
                self.cash_balance + self.realized_pnl + self.unrealized_pnl, 2
            ),
            "symbols": rows,
            "rms_triggered": self.rms_triggered,
            "rms_message": "Done for the day all positions exitted" if self.rms_triggered else None,
            "simulation_logs": bool(getattr(self, "simulation_logs", True)),
        }

    # LIVE UI (FAST)
        trading_snapshot[session_id] = snapshot

    # DB LOGGING (HISTORY)  once per cycle unless persist_db=False (paper mid-cycle refresh)
        if not persist_db:
            return

        try:
            insert_trading_snapshot(
            trading_logs_collection=self.trading_logs_collection,
            session_id=session_id,
            cycle=cycle,
            snapshot=snapshot,
            rows=rows,
            simulation_logs=bool(getattr(self, "simulation_logs", True)),
        )
        except Exception as e:
            print(f"[DB ERROR] Trading snapshot insert failed: {e}")

    def _sync_cash_with_broker(self):
        print("[SYNC] Paper cash balance (authoritative in-memory ledger)")
        free_cash = self._get_free_cash()
        if free_cash is not None:
            self.cash_balance = free_cash
            print(f"[SYNC]  Cash balance: {free_cash:,.2f}")
            return True
        return False
    
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
            live_price = self._get_live_price_redis(symbol, swing_interval) 

            print(
                f"[LIVE RESULT] {symbol} "
                f"live_price={live_price} "
                f"({'FALLBACK predicted[0]' if live_price is None else 'USING SHARED LTP'})",
                flush=True
            )
            
            # Last resort only if Redis SET NX / GET both failed (keeps Trade logs row)
            current_price = float(live_price) if live_price else float(predicted_path[0])
            
            if live_price is None:
                print(f"\n[DEBUG-VERIFY] {symbol}: shared LTP missing  using predicted_path[0]: {current_price}")

            print(
                f"[CURR PRICE FINAL] {symbol} curr_price={current_price}",
                flush=True
            )          
            
            # -------------------------------
            traj_col = group["trajectory_pct"].values if "trajectory_pct" in group.columns else None
            regime_col = group["risk_regime"].values if "risk_regime" in group.columns else None

            if traj_col is not None and len(traj_col) > 0 and not np.isnan(traj_col[0]):
                # Use slot 0's trajectory  the most current signal
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

            # # ===============================
            # # SESSION TREND VETO (HARD RULE)
            # # ===============================
            # session_direction = None
            # session_open = None

            # if session_trends and symbol in session_trends:
            #     session_direction = session_trends[symbol].get("direction")
            #     session_open = session_trends[symbol].get("session_open")

            # # If LONG but session trend is DOWN FORCE EXIT
            # if position_side == "BUY" and session_direction == -1 and session_open and current_price < session_open:
            #     signal = "SELL (Trend Veto Exit)"
            #     signals[symbol] = {
            #         "signal": signal,
            #         "change_pct": trajectory_pct,
            #         "curr_price": current_price,
            #         "side": position_side,
            #         "interval": swing_interval,
            #         "risk_regime": risk_regime
            #     }
            #     continue

            # # If SHORT but session trend is UP FORCE COVER
            # if position_side == "SELL" and session_direction == 1 and session_open and current_price > session_open:
            #     signal = "BUY (Trend Veto Exit)"
            #     signals[symbol] = {
            #         "signal": signal,
            #         "change_pct": trajectory_pct,
            #         "curr_price": current_price,
            #         "side": position_side,
            #         "interval": swing_interval,
            #         "risk_regime": risk_regime
            #     }
            #     continue

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
                    # signal = "HOLD"
                    signal = "BUY"   # allow adding to winning longs

            elif position_side == "SELL":
                if trajectory_pct > 0:
                    signal = "BUY"  
                else:
                    # signal = "HOLD"
                    signal = "SELL" # allow adding to winning shorts

            # ===============================
            # OUTPUT
            # ===============================
            signals[symbol] = {
                "signal": signal,
                "change_pct": trajectory_pct,
                "curr_price": current_price,
                "side": position_side,
                "interval": swing_interval,
                "risk_regime": risk_regime
            }
        return signals 
    
    def _calculate_pnl(self, symbol, ltp):
        if ltp is None or ltp <= 0:
            return 0.0

        #  ONLY source of truth  set by _handle_filled with actual fill price
        # if symbol not in self.positions:
        #     return 0.0  # No tracked position  no PnL to compute

        with self.positions_lock:
            pos = self.positions.get(symbol)

        if not pos:
            return 0.0

        pos = self.positions.get(symbol)
        try:
            entry_price = float(pos.get("entry_price") or 0.0)
            qty = int(pos.get("qty") or 0)
            side = pos.get("side")
        except Exception:
            return 0.0

        # ================================================================
        # Fallback: entry_price still 0 after _handle_filled?
        # This means _handle_filled returned early (couldn't get fill price).
        # Fix it NOW from broker cache  and permanently patch self.positions
        # so next cycle doesn't repeat this.
        # ================================================================
        if entry_price <= 0:
            with self.broker_pos_lock:
                broker_positions = list(self._broker_positions_cache or [])
            for p in broker_positions:
                if p.get("symbol") != symbol:
                    continue
                fresh_price = float(p.get("avg_price", 0.0))
                if fresh_price > 0:
                    with self.positions_lock:
                        if symbol in self.positions:
                            self.positions[symbol]["entry_price"] = fresh_price
                    entry_price = fresh_price
                    print(f"[ENTRY PRICE PATCH] {symbol}: Rs {fresh_price:.2f} (broker cache fallback  should be rare)")
                break

        # Validation
        if entry_price <= 0:
            print(f"[PNL WARN] {symbol}: entry_price nahi mili  P&L = 0")
            return 0.0
        if qty <= 0:
            return 0.0
        if side not in ("BUY", "SELL"):
            return 0.0

        # P&L Formula
        pnl = (ltp - entry_price) * qty if side == "BUY" else (entry_price - ltp) * qty

        # Sanity check
        if abs(pnl) > (entry_price * qty):
            print(f"[P&L SANITY BREACH] {symbol} | pnl={pnl:.2f}, entry={entry_price:.2f}, qty={qty}, ltp={ltp:.2f}")
            return 0.0

        return round(pnl, 2)

    def convert_candle_to_seconds(self, c):
        c = str(c).lower().strip()

        if c.endswith("m"):
            return int(c[:-1]) * 60

        return 300

    def _close_position(self, session, symbol, exit_price, exit_qty, position_snapshot: Optional[dict] = None):
        # -------------------------------
        # Atomic fetch and update (thread-safe)
        # -------------------------------
        with self.positions_lock:
            pos = self.positions.get(symbol)
            position_was_live = bool(pos)
            if not pos and position_snapshot:
                pos = dict(position_snapshot)
            if not pos:
                return 0.0

            # -------------------------------
            # Extract values
            # -------------------------------
            try:
                current_qty = int(pos.get("qty") or 0)
                entry = float(pos.get("entry_price") or 0.0)
                side = pos.get("side")
                exit_price = float(exit_price or 0.0)
                exit_qty = int(exit_qty or 0)
            except Exception:
                print(f"[CLOSE ERROR] {symbol}: invalid position data {pos}")
                return 0.0

            if current_qty <= 0 or entry <= 0 or exit_price <= 0 or exit_qty <= 0 or side not in ("BUY", "SELL"):
                return 0.0

            # -------------------------------
            # P&L calculation based ONLY on exited quantity
            # -------------------------------
            realized_qty = min(current_qty, exit_qty)
            
            if side == "BUY":
                profit = (exit_price - entry) * realized_qty
                self.cash_balance += exit_qty * exit_price
            else:
                profit = (entry - exit_price) * realized_qty

            # Handle partial exits cleanly and position flips
            remaining_qty = exit_qty - current_qty
            
            if position_was_live and remaining_qty > 0:
                pos["side"] = "SELL" if side == "BUY" else "BUY"
                pos["qty"] = remaining_qty
                pos["entry_price"] = exit_price
                print(f"[POSITION FLIP] {symbol}: Flipped to {pos['side']} {remaining_qty} @ {exit_price}")
            elif position_was_live and remaining_qty == 0:
                self.positions.pop(symbol, None)
            elif position_was_live:
                pos["qty"] -= exit_qty

        # -------------------------------
        # Update realized PnL
        # -------------------------------
        self.realized_pnl += profit
        self.realized_pnl_by_symbol[symbol] += profit

        # Check if realized PnL alone has breached the portfolio limit.
        # Mirrors break_1's register_pnl()  catches breach when all positions
        # close in one batch and no further WS ticks arrive.
        # Portfolio RMS halt disabled.
        # Keep the layer available, but do not let realized portfolio loss
        # exit all stocks. Per-ticker RMS remains active.
        # self._check_realized_portfolio_rms()

        self._persist_paper_state_snapshot(event=f"pnl_close:{symbol}")
        return profit

    def _is_position_settled(self, broker_pos):
        return (
            broker_pos is not None
            and broker_pos.get("qty", 0) > 0
            and broker_pos.get("avg_price", 0) > 0
            and broker_pos.get("side") in ("BUY", "SELL")
        )
