import numpy as np
import pandas as pd


class AnalysisMixin:
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
                # Use slot 0's trajectory - the most current signal
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

    @staticmethod
    def _normalize_config_symbol(symbol: str) -> str:
        return (symbol or "").upper().replace("-EQ", "").strip()
