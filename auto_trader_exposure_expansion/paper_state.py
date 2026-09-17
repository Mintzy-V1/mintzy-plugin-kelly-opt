"""AutoTrader mixin: paper-trading state persistence/restore (MongoDB +
trade_log.csv). Moved verbatim from auto_trader_exposure_expansion.py during the
package split; no logic changes.
"""
import csv
import os


class PaperStateMixin:

    def _persist_paper_state_snapshot(self, event: str = "") -> None:
        """Refresh in-memory UI only  do not append MongoDB rows mid-cycle."""
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None) or "default"
        cycle = getattr(self, "_current_cycle_count", 0) or getattr(self, "_restored_cycle_count", 0)
        rows = self._build_current_ui_rows()
        if not rows:
            return
        try:
            self._update_ui_snapshot(
                session_id=sid,
                cycle=cycle,
                rows=rows,
                persist_db=False,
            )
            if event:
                print(f"[PAPER-PERSIST] in-memory UI updated ({event}) cycle={cycle} rows={len(rows)}")
        except Exception as e:
            print(f"[PAPER-PERSIST] failed ({event}): {e}")

    def _restore_paper_state_from_mongodb(self) -> bool:
        coll = self.trading_logs_collection
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        if coll is None or not sid:
            return False

        try:
            latest = coll.find_one({"session_id": sid}, sort=[("cycle", -1), ("timestamp", -1)])
            if not latest:
                return False

            cycle = int(latest.get("cycle") or 0)
            self._restored_cycle_count = cycle

            cash = latest.get("portfolio_cash_balance", latest.get("cash_balance"))
            if cash is not None:
                self.cash_balance = float(cash)
                self.current_capital = float(cash)

            realized = latest.get("portfolio_realized_pnl", latest.get("realized_pnl"))
            if realized is not None:
                self.realized_pnl = float(realized)

            unrealized = latest.get("portfolio_unrealized_pnl")
            if unrealized is not None:
                self.unrealized_pnl = float(unrealized)

            equity = latest.get("portfolio_total_equity", latest.get("total_equity"))
            if equity is not None:
                self.current_capital = float(equity)

            for doc in coll.find({"session_id": sid, "cycle": cycle}):
                sym = (doc.get("symbol") or "").upper().replace("-EQ", "")
                if not sym:
                    continue
                sym_realized = doc.get("symbol_realized_pnl")
                if sym_realized is not None:
                    self.realized_pnl_by_symbol[sym] = float(sym_realized)

                side = doc.get("side")
                action = doc.get("action") or ""
                if side in ("BUY", "SELL") and "HOLD" in action and "Flat" not in action:
                    curr_price = float(doc.get("curr_price") or 0.0)
                    if curr_price > 0 and sym not in self._paper_positions:
                        qty = int(self.symbol_qty.get(sym) or 0)
                        if qty <= 0:
                            continue
                        self._paper_positions[sym] = {
                            "symbol": sym,
                            "side": side,
                            "qty": qty,
                            "avg_price": curr_price,
                            "ltp": curr_price,
                        }

            self._restore_open_positions_from_trade_log()
            self._sync_internal_positions_from_paper()

            print(
                f"[PAPER-RESTORE] session={sid} cycle={cycle} "
                f"cash={self.cash_balance:.2f} realized={self.realized_pnl:.2f} "
                f"positions={len(self.positions)}"
            )
            return True
        except Exception as e:
            print(f"[PAPER-RESTORE] MongoDB restore failed: {e}")
            return False

    def _restore_open_positions_from_trade_log(self) -> None:
        if not os.path.exists(self.log_path):
            return

        today = self._now_market_time().strftime("%Y-%m-%d")
        last_hold_by_symbol: dict = {}

        try:
            with open(self.log_path, "r", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return
                for row in reader:
                    if len(row) < 7:
                        continue
                    ts, symbol, signal, change, status, price, qty = row[:7]
                    if not str(ts).startswith(today):
                        continue
                    if str(status).lower() != "hold":
                        continue
                    sym = symbol.upper().replace("-EQ", "")
                    try:
                        q = int(float(qty))
                        p = float(price)
                    except (TypeError, ValueError):
                        continue
                    if q <= 0 or p <= 0:
                        continue
                    last_hold_by_symbol[sym] = {"qty": q, "price": p, "signal": signal}
        except Exception as e:
            print(f"[PAPER-RESTORE] trade_log read failed: {e}")
            return

        for sym, info in last_hold_by_symbol.items():
            if sym in self._paper_positions:
                continue
            side = "BUY"
            sig = (info.get("signal") or "").upper()
            if "SHORT" in sig or ("SELL" in sig and "BUY" not in sig):
                side = "SELL"
            entry = float(info["price"])
            qty = int(info["qty"])
            self.positions[sym] = {
                "side": side,
                "qty": qty,
                "entry_price": entry,
            }
            self._paper_positions[sym] = {
                "symbol": sym,
                "side": side,
                "qty": qty,
                "avg_price": entry,
                "ltp": entry,
            }
            if sym not in self.symbol_qty:
                self.symbol_qty[sym] = qty

    def _sync_internal_positions_from_paper(self) -> None:
        with self._paper_lock:
            paper_copy = dict(self._paper_positions)
        for sym, p in paper_copy.items():
            if sym not in self.positions and int(p.get("qty") or 0) > 0:
                self.positions[sym] = {
                    "side": p.get("side"),
                    "qty": int(p.get("qty")),
                    "entry_price": float(p.get("avg_price") or 0.0),
                }

