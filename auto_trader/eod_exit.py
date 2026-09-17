import time
import os
import pandas as pd

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


class EODExitMixin:
    def _exit_all_positions_and_stop(self):
        begin = try_begin_eod_exit(self)
        if begin == "done":
            return True
        if begin == "busy":
            return False

        try:
            print("exiting all postiions from market")
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
                    live_price = getattr(self, "_cycle_ltp_cache", {}).get(sym) or curr_price
                    self._log_trade(
                        sym,
                        "MARKET_CLOSE_EXIT",   
                        0.0,                   
                        "pending",
                        live_price,
                        metadata.get("qty", 0),
                        0.0
                    )
                    print(f"  {sym}: MARKET_CLOSE_EXIT PENDING (order sent) @ {live_price:.2f}")
                    
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

    def _generate_final_merged_tradebook(self ,angel_orders=None) -> pd.DataFrame:
        internal_df = pd.DataFrame(self.trade_history)
        if not internal_df.empty:
            internal_df["source"] = "internal"

        try:
            # resp = self.broker.get_order_book(self.session)
            # if resp.get("status") == "success":
            #     angel_df = pd.DataFrame(resp["raw"].get("data", []))
            # else:
            #     angel_df = pd.DataFrame()

            # angel_df = pd.DataFrame(self.broker.orderBook())

              angel_df = pd.DataFrame(angel_orders or [])
        except Exception as e:
            print(f"[ORDERBOOK] Fetch failed: {e}")
            angel_df = pd.DataFrame()

        if not angel_df.empty:
            angel_df["source"] = "angel"

        final_df = pd.concat(
            [internal_df, angel_df],
            ignore_index=True,
            sort=False
        )

        date_str = self._now_market_time().strftime("%Y-%m-%d")
        output_path = os.path.join(
            self.log_dir,
            f"final_tradebook_{date_str}.csv"
        )

        final_df.to_csv(output_path, index=False)

        print(f"[EOD] Final tradebook saved: {output_path}")
        self.alerts.notify(f"Final tradebook generated: {output_path}")

        return final_df
