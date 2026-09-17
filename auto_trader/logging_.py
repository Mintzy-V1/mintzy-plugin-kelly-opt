import csv

from trading_snapshot import insert_trading_snapshot
from trading_state import trading_snapshot


class LoggingMixin:
    # ---------- LOGGING ----------
    def _log_portfolio_action(self, symbol, action, qty, entry_price, exit_price, pnl):
        cumulative_pnl = self.realized_pnl
        portfolio_return = (cumulative_pnl / self.initial_capital) * 100
        trade_return = (pnl / (entry_price * qty)) * 100 if entry_price * qty else 0.0
        with open(self.portfolio_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                self._now_market_time().strftime("%Y-%m-%d %H:%M:%S"),
                symbol, action, qty,
                f"{entry_price:.2f}", f"{exit_price:.2f}", f"{pnl:.2f}",
                f"{cumulative_pnl:.2f}", f"{self.current_capital:.2f}",
                f"{trade_return:.2f}", f"{portfolio_return:.2f}"
            ])

    def _log_trade(self, symbol, signal, change, status, price, qty, pnl):

        #  Always use live LTP cache as price source
        live = getattr(self, "_cycle_ltp_cache", {}).get(symbol)
        if live is not None and live > 0:
            price = float(live)
        elif price is None or price <= 0:
            price = 0.0

        #  Calculate live unrealized PnL from cache if not explicitly passed
        if pnl == 0.0 and price > 0 and status in ("hold", "pending", "wait"):
            unrealized = self._calculate_pnl(symbol, price)
        else:
            unrealized = pnl

        candle_time = getattr(self, "current_cycle_ts_str", None)
        if not candle_time:
            candle_time = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")

        logged_at = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")

        total_equity = round(self.cash_balance + self.unrealized_pnl, 2)
        portfolio_return = round(
            ((total_equity - self.initial_capital) / self.initial_capital) * 100, 4
        ) if self.initial_capital else 0.0

        log_entry = {
            "time": candle_time,
            "logged_at": logged_at,
            "symbol": symbol,
            "signal": signal,
            "change_pct": round(change, 6),
            "status": status,
            "price": price,
            "qty": qty,
            "pnl": round(unrealized, 2),
            "cash_balance": round(self.cash_balance, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "total_equity": total_equity,
        }

        self.trade_history.append(log_entry)

        # âœ… WRITE TO CSV IMMEDIATELY â€” bar by bar, every cycle
        try:
            with open(self.log_path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    candle_time,
                    symbol,
                    signal,
                    round(change, 6),
                    status,
                    round(price, 2),
                    round(unrealized, 2),
                    total_equity,
                    portfolio_return,
                ])
        except Exception as e:
            print(f"[LOG ERROR] Failed to write trade log: {e}")

    # trading snapshot update karne ka function
    def _update_ui_snapshot(self, session_id, cycle, rows):
        snapshot = {
            "cycle": cycle,
            "timestamp": (self.current_cycle_ts_str or self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")),
            "cash_balance": round(self.cash_balance, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "total_equity": round(
                self.cash_balance + self.realized_pnl + self.unrealized_pnl, 2
            ),
            "symbols": rows
        }

    # LIVE UI (FAST)
        trading_snapshot[session_id] = snapshot

    # DB LOGGING (HISTORY)
        try:
            insert_trading_snapshot(
            trading_logs_collection=self.trading_logs_collection,
            session_id=session_id,
            cycle=cycle,
            snapshot=snapshot,
            rows=rows,
        )
        except Exception as e:
            print(f"[DB ERROR] Trading snapshot insert failed: {e}")
