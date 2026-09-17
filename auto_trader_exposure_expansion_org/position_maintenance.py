from typing import Optional

from .constants import STOP_LOCK_TIME


class PositionMaintenanceMixin:
    def _run_stoplock_exits(self, active_symbols: list) -> None:
        """At 14:15 IST exit open symbols with unrealized_pnl < 0; continue with the rest."""
        now = self._now_market_time()
        print(
            f"[STOPLOCK] Check at {now.strftime('%Y-%m-%d %H:%M:%S')} IST "
            f"(trigger>={STOP_LOCK_TIME.strftime('%H:%M')})"
        )

        symbols_to_check = {
            self._normalize_config_symbol(s) for s in (active_symbols or []) if s
        }

        with self.broker_pos_lock:
            self._broker_positions_cache = self._get_broker_positions()
            for pos in self._broker_positions_cache or []:
                sym = self._normalize_config_symbol(pos.get("symbol"))
                if sym:
                    symbols_to_check.add(sym)

        exited = []
        continuing = []
        for sym_key in sorted(symbols_to_check):
            if sym_key in self._exited_symbols:
                continue

            qty = self._get_symbol_position_qty(sym_key)
            if qty == 0:
                continuing.append(f"{sym_key}(flat)")
                continue

            unrealized = self._get_symbol_unrealized_pnl(sym_key)
            if unrealized < 0:
                print(f"[STOPLOCK] {sym_key} unrealized={unrealized:.2f}  placing exit")
                result = self.exit_single_position(sym_key, log_signal="STOP_LOCK")
                if result.get("success"):
                    exited.append(sym_key)
                else:
                    print(
                        f"[STOPLOCK] {sym_key} exit failed: "
                        f"{result.get('message', 'unknown error')}"
                    )
            else:
                continuing.append(f"{sym_key}(unrealized={unrealized:.2f})")

        summary = (
            f"14:15 stop-lock complete  exited: {exited or 'none'}; "
            f"continuing: {continuing}"
        )
        print(f"[STOPLOCK] {summary}")
        self.alerts.notify(summary)

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

        return profit

    def _is_position_settled(self, broker_pos):
        return (
            broker_pos is not None
            and broker_pos.get("qty", 0) > 0
            and broker_pos.get("avg_price", 0) > 0
            and broker_pos.get("side") in ("BUY", "SELL")
        )

    def _strip_exited_from_active(self, symbols: list, batch_size: int):
        """Drop stop-lock / manual exits from this loop's symbol list. No I/O wait."""
        self._sync_exited_symbols_from_db()
        if not self._exited_symbols:
            return symbols, [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        before_count = len(symbols)
        symbols = [
            s for s in symbols
            if self._normalize_config_symbol(s) not in self._exited_symbols
        ]
        symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        if len(symbols) < before_count:
            print(
                f"[SINGLE EXIT] Removed {sorted(self._exited_symbols)} from active symbols. "
                f"Remaining: {symbols}"
            )
        return symbols, symbol_batches

    def _get_symbol_position_qty(self, symbol: str) -> int:
        symbol = self._normalize_config_symbol(symbol)
        with self.positions_lock:
            pos = self.positions.get(symbol)
            if pos:
                return int(pos.get("qty") or 0)
        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if pos.get("symbol") == symbol:
                    return int(pos.get("qty") or 0)
        return 0

    def _get_symbol_unrealized_pnl(self, symbol: str) -> float:
        symbol = self._normalize_config_symbol(symbol)
        if symbol in self._exited_symbols or self._get_exited_symbol_doc(symbol):
            print(f"[PNL-GUARD] {symbol}: exited symbol -> unrealized_pnl forced to 0.00")
            return 0.0

        with self.live_pnl_lock:
            tick = self.live_pnl.get(symbol)
            if tick is not None:
                return float(tick.get("pnl") or 0.0)

        cache = getattr(self, "_cycle_ltp_cache", {}) or {}
        ltp = cache.get(symbol)
        if ltp is not None and float(ltp) > 0:
            return float(self._calculate_pnl(symbol, float(ltp)))

        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if pos.get("symbol") == symbol:
                    ltp = float(pos.get("ltp") or 0.0)
                    if ltp > 0:
                        return float(self._calculate_pnl(symbol, ltp))
        return 0.0
