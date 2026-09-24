import time

from orderbook import fetch_todays_intraday_orders
from utils.session_ledger import record_engine_order_for_trader
from utils.eod_exit import (
    try_begin_eod_exit,
    mark_eod_exit_done,
    release_eod_exit_in_progress,
    prepare_eod_exit_plan,
    exit_plan_to_broker_positions,
    finalize_eod_shutdown,
)
from .order_execution import OrderRequest


class EodExitMixin:
        # time.sleep(sleep_seconds)

    def _exit_all_positions_and_stop(self):
        begin = try_begin_eod_exit(self)
        if begin == "done":
            return True
        if begin == "busy":
            return False

        try:
            angel_orders = fetch_todays_intraday_orders()
            self._generate_final_merged_tradebook(angel_orders=angel_orders)
        except Exception as e:
            print(f"[EOD MERGE ERROR] {e}")

        try:
            return self._run_eod_exit_body()
        except Exception as e:
            print(f"[EOD] Exit failed: {e}")
            self.alerts.notify(f"EOD exit failed: {e}")
            release_eod_exit_in_progress(self)
            return False

    def _run_eod_exit_body(self):
        print("\n" + "=" * 80)
        print("  MARKET CLOSE (3:00 PM IST) - EXITING SESSION POSITIONS")
        print("=" * 80)

        self.alerts.notify("3:00 PM IST - Initiating exit of session positions")
        self._notify_eod_exit_status_to_api(reason="MARKET_CLOSE_15:00_IST")

        with self.broker_pos_lock:
            self._broker_positions_cache = self._get_broker_positions()
            raw_broker_positions = list(self._broker_positions_cache or [])

        def _on_eod_skip_summary(count: int, sample: list, reason: str) -> None:
            if reason == "ledger_fallback":
                self.alerts.notify("EOD: using session ledger keys (Redis meta missing)")
            elif reason == "ledger_zero" and count:
                suffix = f" e.g. {', '.join(sample[:3])}" if sample else ""
                self.alerts.notify(
                    f"EOD skipped {count} manual qty on session symbol(s){suffix}"
                )

        exit_plan, mark_done_empty, status_msg = prepare_eod_exit_plan(
            self,
            raw_broker_positions,
            fallback_symbols=list((self.symbol_allocations or {}).keys()),
            on_skip_summary=_on_eod_skip_summary,
        )

        if not exit_plan:
            if mark_done_empty:
                print(f"[INFO]  {status_msg}")
                self.alerts.notify(" No session positions to exit - Auto trader stopped")
                finalize_eod_shutdown(self, sync_broker=True)
                mark_eod_exit_done(self)
                return True
            print(f"[EOD] {status_msg}")
            self.alerts.notify(status_msg)
            release_eod_exit_in_progress(self)
            return False

        broker_positions = exit_plan_to_broker_positions(exit_plan)
        print(f"[INFO] {status_msg}")

        eod_exit_records = {}
        for pos in broker_positions:
            sym = pos["symbol"]
            position_side = pos["side"]
            exit_side = "SELL" if position_side == "BUY" else "BUY"
            eod_exit_records[sym] = {
                "symbol": sym,
                "position_side": position_side,
                "exit_side": exit_side,
                "qty": pos.get("qty", 0),
                "price": float(pos.get("ltp") or 0.0),
            }
        
        # Collect all exit orders
        exit_orders = []
        
        for pos in broker_positions:
            sym = pos["symbol"]
            side = pos["side"]
            qty = pos["qty"]
            curr_price = pos.get("ltp", 0.0)
            
            # Determine exit side
            exit_side = "SELL" if side == "BUY" else "BUY"
            
            print(f"[EXIT] {sym}: Closing {side} position (qty={qty}) {curr_price:.2f}")
            
            exit_orders.append(
                OrderRequest(
                    symbol=sym,
                    side=exit_side,
                    qty=qty,
                    metadata={
                        "signal": "MARKET_CLOSE_EXIT",
                        "action_type": "MARKET_CLOSE_EXIT",
                        "curr_price": curr_price,
                        "side": exit_side,
                        "qty": qty,
                        "order_value": curr_price * qty,
                        "original_side": side
                    }
                )
            )
        
        # Execute all exit orders in parallel
        if exit_orders:
            print(f"\n[PARALLEL] Executing {len(exit_orders)} exit orders...")
            
            self._ensure_parallel_executor()
            results = self.parallel_executor.submit_orders(exit_orders)
            
            # Process results
            successful_exits = 0
            failed_exits = 0
            
            for result in results:
                sym = result.symbol
                metadata = result.metadata or {}
                original_side = metadata.get("original_side", "UNKNOWN")

                if sym in eod_exit_records:
                    if result.avg_price and float(result.avg_price) > 0:
                        eod_exit_records[sym]["price"] = float(result.avg_price)
                    elif metadata.get("curr_price"):
                        eod_exit_records[sym]["price"] = float(metadata["curr_price"])
                
                if result.success:
                    successful_exits += 1
                    record_engine_order_for_trader(self, result.order_id)

                    # Add to pending for reconciliation
                    with self.pending_lock:
                        self.pending_orders[sym].append({
                            "order_id": result.order_id,
                            "action_type": "MARKET_CLOSE_EXIT",
                            "side": metadata.get("side"),
                            "qty": metadata.get("qty"),
                            "order_value": metadata.get("order_value", 0.0),
                            "placed_at": time.time(),
                        })
                    
                    print(f" {sym}: Exit order sent (closing {original_side} position)")
                else:
                    failed_exits += 1
                    error = result.error or "Unknown error"
                    print(f" {sym}: Exit order failed - {error}")
                    self.alerts.notify(f" Failed to exit {sym}: {error}")
            
            print(f"\n[SUMMARY] Exit orders: {successful_exits} sent, {failed_exits} failed")
            
            # Wait for orders to fill (max 60 seconds)
            print("\n[WAIT] Waiting for exit orders to fill (max 60s)...")
            max_wait = 60
            start_wait = time.time()
            
            while (time.time() - start_wait) < max_wait:
                with self.pending_lock:
                    if not self.pending_orders:
                        print(" All exit orders filled")
                        break
                
                time.sleep(2)
                elapsed = int(time.time() - start_wait)
                remaining = max_wait - elapsed
                print(f"[WAIT] {remaining}s remaining... (pending: {len(self.pending_orders)} symbols)", end='\r')
            
            # Check final status
            with self.pending_lock:
                if self.pending_orders:
                    print(f"\n  Warning: {len(self.pending_orders)} positions still pending after 60s")
                    for sym in self.pending_orders.keys():
                        print(f"  - {sym}: Position may not be fully closed")
                        self.alerts.notify(f" {sym} exit order pending - check manually")
        
        # Final sync
        print("\n[FINAL SYNC] Syncing with broker...")
        self._sync_cash_with_broker()

        try:
            self._persist_eod_exit_trading_logs(list(eod_exit_records.values()))
        except Exception as e:
            print(f"[EOD-LOG] trading_logs insert failed: {e}")
        
        finalize_eod_shutdown(self, clear_positions=True, sync_broker=False)

        print("\n" + "=" * 80)
        print(" ALL POSITIONS EXITED - AUTO TRADER STOPPED")
        print("=" * 80)
        
        # Print final summary
        print(f"\nFinal Summary:")
        print(f"  Cash Balance:{self.cash_balance:,.2f}")
        print(f"  Realized P&L:{self.realized_pnl:,.2f}")
        print(f"  Total Equity:{self.current_capital:,.2f}")
        
        self.alerts.notify(
            f" Auto Trader Stopped\n"
            f"Final Equity:{self.current_capital:,.2f}\n"
            f"Realized P&L:{self.realized_pnl:,.2f}"
        )

        mark_eod_exit_done(self)
        return True

    def exit_single_position(self, symbol: str, log_signal: str = None, exit_reason: str = None) -> dict:
        """
        Exit a single symbol's position.
        - Fetches broker positions for this symbol
        - If position exists  ' places exit order
        - Adds to pending_orders for reconciliation
        - Returns result dict (success/failure)
        """
        symbol = symbol.upper().replace("-EQ", "")
        print(f"\n[SINGLE EXIT] Request to exit position: {symbol}")

        try:
            # 1) Refresh broker positions
            with self.broker_pos_lock:
                self._broker_positions_cache = self._get_broker_positions()
                broker_positions = list(self._broker_positions_cache or [])

            # 2) Find the target symbol (normalize; engine book if Angel list lags)
            target_pos = None
            for pos in broker_positions:
                if self._normalize_config_symbol(pos.get("symbol")) == symbol:
                    target_pos = pos
                    break

            if not target_pos:
                with self.positions_lock:
                    internal = dict(self.positions.get(symbol) or {})
                qty_int = int(internal.get("qty") or 0)
                side_int = internal.get("side")
                if qty_int > 0 and side_int in ("BUY", "SELL"):
                    ltp = 0.0
                    with self.live_pnl_lock:
                        tick = (self.live_pnl or {}).get(symbol) or {}
                        ltp = float(tick.get("ltp") or 0.0)
                    if ltp <= 0:
                        ltp = float((getattr(self, "_cycle_ltp_cache", {}) or {}).get(symbol) or 0.0)
                    if ltp <= 0:
                        ltp = float(internal.get("entry_price") or 0.0)
                    target_pos = {
                        "symbol": symbol,
                        "side": side_int,
                        "qty": qty_int,
                        "ltp": ltp,
                        "avg_price": float(internal.get("entry_price") or 0.0),
                    }
                    print(
                        f"[SINGLE EXIT] {symbol}: broker list miss  using engine "
                        f"{side_int} {qty_int}"
                    )

            if not target_pos:
                msg = f"No open position found for {symbol}"
                print(f"[SINGLE EXIT] {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

            side = target_pos["side"]
            qty = self._exit_qty_for(symbol, target_pos["qty"])
            curr_price = target_pos.get("ltp", 0.0)
            exit_side = "SELL" if side == "BUY" else "BUY"
            exit_reason = exit_reason or ("STOP_LOCK_EXIT" if log_signal == "STOP_LOCK" else "MANUAL_EXIT")
            pre_exit_position = self._snapshot_position_for_exit(symbol, target_pos)

            print(f"[SINGLE EXIT] {symbol}: Closing {side} position (qty={qty}) @ {curr_price:.2f}")

            # 3) Place exit order
            exit_order = OrderRequest(
                symbol=symbol,
                side=exit_side,
                qty=qty,
                metadata={
                    "signal": "SINGLE_EXIT",
                    "action_type": "EXIT_LONG" if side == "BUY" else "COVER_SHORT",
                    "curr_price": curr_price,
                    "side": exit_side,
                    "qty": qty,
                    "order_value": curr_price * qty,
                    "original_side": side,
                    "position_side": side,
                    "pre_exit_position": pre_exit_position,
                    "exit_reason": exit_reason,
                }
            )

            self._ensure_parallel_executor()
            results = self.parallel_executor.submit_orders([exit_order])

            if not results:
                msg = f"No result from order executor for {symbol}"
                print(f"[SINGLE EXIT] {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

            result = results[0]
            metadata = result.metadata or {}

            if result.success:
                exit_ctx = {
                    "order_id": result.order_id,
                    "action_type": metadata.get("action_type", "EXIT_LONG"),
                    "side": metadata.get("side"),
                    "qty": metadata.get("qty"),
                    "order_value": metadata.get("order_value", 0.0),
                    "position_side": metadata.get("position_side"),
                    "pre_exit_position": metadata.get("pre_exit_position") or pre_exit_position,
                    "exit_reason": metadata.get("exit_reason") or exit_reason,
                    "placed_at": time.time(),
                }

                if result.filled and float(result.avg_price or 0.0) > 0:
                    self._finalize_symbol_exit_fill(
                        symbol,
                        {
                            "side": metadata.get("side"),
                            "qty": int(result.filled_qty or metadata.get("qty") or 0),
                            "avg_price": float(result.avg_price or 0.0),
                        },
                        exit_ctx,
                    )
                    self._exited_symbols.add(symbol)
                    msg = f"Exit order filled for {symbol} (closed {side} position, qty={qty})"
                    print(f"[SINGLE EXIT] {msg}")
                    return {"success": True, "symbol": symbol, "message": msg, "order_id": result.order_id}

                # Add to pending for reconciliation
                with self.pending_lock:
                    self.pending_orders[symbol].append(exit_ctx)

                self._persist_exit_pending(
                    symbol,
                    exit_reason=exit_ctx["exit_reason"],
                    exit_side=exit_ctx["side"],
                    qty=exit_ctx["qty"],
                    entry_price=pre_exit_position.get("entry_price", 0.0),
                    exit_order_id=result.order_id,
                )

                # Mark symbol as exited - will be excluded from next trading cycle
                self._exited_symbols.add(symbol)
                print(f"[SINGLE EXIT] {symbol} added to _exited_symbols - will be skipped in future cycles")

                if False and log_signal:
                    session_id = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
                    if session_id:
                        symbol_unrealized_pnl = round(self._get_symbol_unrealized_pnl(symbol), 2)
                        symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(symbol, 0.0)), 2)
                        symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)
                        self.current_cycle_ts_str = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
                        self._update_ui_snapshot(
                            session_id,
                            getattr(self, "_cycle_count", 0) + 1,
                            [{
                                "symbol": symbol,
                                "curr_price": round(float(curr_price), 2),
                                "return_pct": 0.0,
                                "side": side,
                                "signal": log_signal,
                                "action": f"{log_signal} EXIT SENT",
                                "qty": qty,
                                "unrealized_pnl": symbol_unrealized_pnl,
                                "symbol_unrealized_pnl": symbol_unrealized_pnl,
                                "symbol_realized_pnl": symbol_realized_pnl,
                                "symbol_pnl": symbol_pnl,
                                "pnl": symbol_pnl,
                            }],
                        )

                msg = f"Exit order sent for {symbol} (closing {side} position, qty={qty})"
                print(f"[SINGLE EXIT] [OK] {msg}")
                
                return {"success": True, "symbol": symbol, "message": msg, "order_id": result.order_id}
            else:
                msg = f"Exit order failed for {symbol}: {result.error}"
                print(f"[SINGLE EXIT]  {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

        except Exception as e:
            msg = f"Exception during single exit for {symbol}: {e}"
            print(f"[SINGLE EXIT]  {msg}")
            return {"success": False, "symbol": symbol, "message": msg}

    def _persist_eod_exit_trading_logs(self, exit_records: list) -> None:
        """Save 15:00 / shutdown square-off rows to trading_logs for the frontend."""
        if not exit_records:
            return
        session_id = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        if not session_id:
            print("[EOD-LOG] No session_id  skipping trading_logs insert")
            return

        rows = []
        for rec in exit_records:
            sym = rec["symbol"]
            exit_side = rec["exit_side"]  # actual order placed: BUY or SELL
            position_side = rec["position_side"]  # position we had: BUY (long) or SELL (short)
            qty = int(rec.get("qty") or 0)
            price = float(rec.get("price") or 0.0)
            symbol_realized = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
            rows.append({
                "symbol": sym,
                "curr_price": round(price, 2),
                "return_pct": 0.0,
                "side": position_side,
                "signal": "",
                "action": exit_side,
                "qty": qty,
                "unrealized_pnl": 0.0,
                "symbol_unrealized_pnl": 0.0,
                "symbol_realized_pnl": symbol_realized,
                "symbol_pnl": symbol_realized,
                "pnl": symbol_realized,
            })

        self.current_cycle_ts_str = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
        self.unrealized_pnl = 0.0
        cycle = getattr(self, "_cycle_count", 0) + 1
        print(f"[EOD-LOG] Persisting {len(rows)} square-off row(s) to trading_logs (cycle={cycle})")
        self._update_ui_snapshot(session_id, cycle, rows)
