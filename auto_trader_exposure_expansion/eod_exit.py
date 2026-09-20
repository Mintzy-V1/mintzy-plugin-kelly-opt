"""AutoTrader mixin: end-of-day / single-symbol exit flow and pending-order
reconciliation. Moved verbatim from auto_trader_exposure_expansion.py during the
package split; no logic changes.
"""
import time
from datetime import datetime

from utils.eod_exit import (
    exit_plan_to_broker_positions,
    finalize_eod_shutdown,
    mark_eod_exit_done,
    prepare_eod_exit_plan,
    release_eod_exit_in_progress,
    try_begin_eod_exit,
)
from utils.session_ledger import record_engine_order_for_trader

from .order_execution import OrderRequest


class EodExitMixin:

    def _exit_all_positions_and_stop(self):
        begin = try_begin_eod_exit(self)
        if begin == "done":
            return True
        if begin == "busy":
            return False

        try:
            paper_orders = self._get_paper_orders_for_tradebook()
            self._generate_final_merged_tradebook(angel_orders=paper_orders)
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
        print("  MARKET CLOSE APPROACHING - EXITING SESSION POSITIONS")
        print("=" * 80)

        self.alerts.notify(" 1:30 PM - Initiating exit of session positions")

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

    def exit_single_position(self, symbol: str, exit_reason: str = "MANUAL_EXIT") -> dict:
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

            # 2) Find the target symbol
            target_pos = None
            for pos in broker_positions:
                if pos["symbol"] == symbol:
                    target_pos = pos
                    break

            if not target_pos:
                msg = f"No open position found for {symbol}"
                print(f"[SINGLE EXIT] {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

            side = target_pos["side"]
            qty = target_pos["qty"]
            curr_price = target_pos.get("ltp", 0.0)
            exit_side = "SELL" if side == "BUY" else "BUY"
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

    def _reconcile_pending_orders(self):
        while not self.stop_event.is_set():
            t_reconcile_start = time.time()
            pending_snapshot = []                    #... initialise here so always defined


            with self.pending_lock:
                if not self.pending_orders:
                    pass # Handled below
                else:
                    # pending_orders: symbol -> list[ctx]
                    pending_snapshot = list(self.pending_orders.items())

            if not pending_snapshot:
                time.sleep(1)
                continue

            new_positions = self._get_broker_positions()
            with self.broker_pos_lock:
                self._broker_positions_cache = new_positions
                broker_positions = list(self._broker_positions_cache or [])

            for sym, ctx_list in pending_snapshot:
                # iterate over a COPY so we can safely remove
                for ctx in list(ctx_list):

                    expected_side = ctx.get("side")
                    expected_qty = ctx.get("qty", 0)
                    action_type = ctx.get("action_type", "")
                    order_id       = ctx.get("order_id")

                    # Defensive: phantom entry with no order_id (order was rejected
                    # by Angel before an order_id was issued). Release its exposure
                    # and drop it so the symbol isn't locked out of future cycles.
                    if not order_id:
                        try:
                            self._release_exposure(sym, ctx.get("order_value", 0.0))
                        except Exception as _re:
                            print(f"[RECONCILE] {sym}: release_exposure failed: {_re}")
                        with self.pending_lock:
                            try:
                                ctx_list.remove(ctx)
                            except ValueError:
                                pass
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)
                        print(f"[RECONCILE] dropped phantom pending entry for {sym} ({action_type})")
                        continue

                    is_exit = action_type in {
                        "EXIT_LONG",
                        "COVER_SHORT",
                        "STOP_LOSS",
                        "MARKET_CLOSE_EXIT"
                    }
                    
                    is_flip = action_type in {
                        "FLIP_TO_LONG",
                        "FLIP_TO_SHORT"
                    }
                    
                    # ----------------------------------------
                    # FLIP HANDLING (NETTED POSITIONS CHANGE)
                    # ----------------------------------------
                    if is_flip:
                        exit_price = 0.0
                        if order_id:
                            check_ts = datetime.now()
                            print(f"[DEBUG-RECONCILE] [{check_ts.strftime('%H:%M:%S.%f')[:-3]}] Checking flip pending order_id {order_id} for {sym}")
                            exit_price = self._get_fill_price_from_orderbook(order_id, sym)
                        
                        if exit_price <= 0:
                            print(f"[DEBUG-RECONCILE] Flip Order {order_id} not executed yet. Retaining in pending list.")
                            continue
                            
                        # If executed, definitively call handle_filled
                        self._handle_filled(
                            sym,
                            {
                                "side": "BUY" if "LONG" in action_type else "SELL",
                                "qty": expected_qty,
                                "avg_price": exit_price,
                            },
                            ctx,
                        )

                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)

                        continue
                    
                    # -----------------------
                    # ENTRY / SAME-SIDE MATCH
                    # -----------------------
                    pos = None
                    broker_qty = 0
                    for p in broker_positions:
                        if p.get("symbol") != sym:
                            continue
                        if p.get("side") != expected_side:
                            continue

                        broker_qty = p.get("qty", 0)
                        if broker_qty <= 0:
                            continue

                        pos = p
                        break

                    if pos:
                        # ---- FILLED (or partially filled) ----
                        filled_qty = min(broker_qty, expected_qty)
                        avg_price = float(pos.get("avg_price") or 0.0)

                        if avg_price <= 0 and order_id:
                            avg_price = self._get_fill_price_from_orderbook(order_id, sym)
                            if avg_price > 0:
                                print(f"[RECONCILE] {sym}: avg_price order book se mili Rs {avg_price:.2f}")
                            else:
                                print(f"[RECONCILE] {sym}: avg_price abhi bhi 0  next cycle mein retry hoga")

                        self._handle_filled(
                            sym,
                            {
                                "side": pos["side"],
                                "qty": filled_qty,
                                "avg_price": avg_price
                            },
                            ctx
                        )
                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)
                        continue
                    
                    # -----------------------
                    # EXIT HANDLING (POSITION GONE)
                    # -----------------------
                    if is_exit:
                        still_exists = False
                        for p in broker_positions:
                            if p.get("symbol") != sym:
                                continue
                            position_side = ctx.get("position_side", expected_side)
                            if p.get("side") == position_side:
                                still_exists = True
                                break

                        if not still_exists:
                            exit_price = 0.0
                            if order_id:
                                check_ts = datetime.now()
                                print(f"[DEBUG-RECONCILE] [{check_ts.strftime('%H:%M:%S.%f')[:-3]}] Checking pending order_id {order_id} for {sym}")
                                exit_price = self._get_fill_price_from_orderbook(order_id, sym)
                                ret_ts = datetime.now()
                                print(f"[DEBUG-RECONCILE] [{ret_ts.strftime('%H:%M:%S.%f')[:-3]}] Orderbook API returned exit_price: {exit_price} for {sym} (order_id {order_id})")

                            # Fallback removed - we MUST wait for the true execution price
                            if exit_price <= 0:
                                print(f"[DEBUG-RECONCILE] Order {order_id} not executed yet. Retaining in pending list.")
                                continue

                            self._handle_filled(
                                sym,
                                {
                                    "side": expected_side,
                                    "qty": expected_qty,
                                    "avg_price": exit_price  # price already realized
                                },
                                ctx
                            )
                            order_value = ctx.get("order_value", 0.0)
                            self._release_exposure(sym, order_value)
                            with self.pending_lock:
                                ctx_list.remove(ctx)
                                if not ctx_list:
                                    self.pending_orders.pop(sym, None)

                            continue
                        
                    # ---- RECOIL / TIMEOUT ----
                    if time.time() - ctx.get("placed_at", 0) > 120:
                        self._handle_rejected(sym, ctx)

                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)

            time.sleep(2) 
            elapsed_rec = round(time.time() - t_reconcile_start, 3)
            print(f"[TIMING] RECONCILE_CYCLE_TOTAL              {elapsed_rec:>7.3f}s  pending_syms={len(pending_snapshot)}")
            try:
                self.tlog.record("RECONCILE_CYCLE_TOTAL", t_reconcile_start, note=f"pending_syms={len(pending_snapshot)}")
            except Exception:
                pass 
    
