import time

from utils.session_ledger import (
    apply_fill_to_session_ledger,
    record_engine_order_for_trader,
)


class FillsReconciliationMixin:
    def _get_fill_price_from_orderbook(self, order_id, symbol):
        try:
            ob = self.session["obj"].orderBook()

            if not isinstance(ob, dict):
                return 0.0
            if not ob.get("status"):
                return 0.0

            orders = ob.get("data", [])
            if not isinstance(orders, list):
                return 0.0

            for order in orders:
                if str(order.get("orderid")) != str(order_id):
                    continue

                # âœ… Pehle orderstatus check karo
                order_status = order.get("orderstatus", "").lower()

                if order_status == "cancelled":
                    print(f"[FILL PRICE] {symbol}: order {order_id} CANCELLED hai â€” fill price nahi milegi")
                    return 0.0

                if order_status == "rejected":
                    print(f"[FILL PRICE] {symbol}: order {order_id} REJECTED hai â€” fill price nahi milegi")
                    return 0.0

                if order_status not in ("complete", "filled"):
                    # open, pending, trigger pending etc.
                    print(f"[FILL PRICE] {symbol}: order {order_id} abhi {order_status} hai â€” wait karo")
                    return 0.0

                # âœ… Order complete hai â€” ab averageprice lo
                # fill_price = float(order.get("price") or 0.0)
                fill_price = float(order.get("price") or 0.0)
                avg_price_field = float(order.get("averageprice") or 0.0)
                print(
                    f"[FILL PRICE] {symbol}: order {order_id} | "
                    f"price={fill_price:.2f} | averageprice={avg_price_field:.2f}"
                )

                # âœ… filledshares bhi check karo
                filled_shares = int(order.get("filledshares") or 0)

                # if fill_price > 0 and filled_shares > 0:
                #     print(f"[FILL PRICE] {symbol}: order {order_id} complete @ â‚¹{fill_price:.2f} ({filled_shares} shares)")
                #     return fill_price
                # else:
                #     print(f"[FILL PRICE] {symbol}: order complete but averageprice=0 ya filledshares=0")
                #     return 0.0


                if fill_price > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: order {order_id} complete @ â‚¹{fill_price:.2f} (price field) | averageprice=â‚¹{avg_price_field:.2f} ({filled_shares} shares)")
                    return fill_price
                elif avg_price_field > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: price=0 fallback to averageprice=â‚¹{avg_price_field:.2f}")
                    return avg_price_field
                else:
                    print(f"[FILL PRICE] {symbol}: order complete but both price=0 and averageprice=0 or filledshares=0")
                    return 0.0
                
            print(f"[FILL PRICE] {symbol}: order {order_id} order book mein nahi mila")
            return 0.0

        except Exception as e:
            print(f"[FILL PRICE ERROR] {symbol}: {e}")
            return 0.0

    # ------- HANDLE FILLED --------

    # def _handle_filled(self, symbol, broker_pos, ctx):
    #     action_type = ctx.get("action_type", "")
    #     if broker_pos["qty"] <= 0:
    #         return
    #     if action_type in {
    #         "EXIT_LONG",
    #         "COVER_SHORT",
    #         "TREND_VETO_EXIT_LONG",
    #         "TREND_VETO_EXIT_SHORT",
    #         "STOP_LOSS",
    #         "MARKET_CLOSE_EXIT"
    #     }:
    #         self.positions.pop(symbol, None)
    #         return
    #     self.positions[symbol] = {
    #         "side": broker_pos["side"],
    #         "qty": broker_pos["qty"],
    #         "entry_price": broker_pos["avg_price"]
    #     }
    #     print(
    #         f"{symbol}: POSITION SET"
    #         f"{broker_pos['side']} {broker_pos['qty']} @ {broker_pos['avg_price']}"
    #     )




# ----------------------------------------------------------------
# FUNCTION 1 - _handle_filled (FIXED)
# ----------------------------------------------------------------

    def _track_engine_fill(self, symbol, broker_pos, ctx) -> None:
        record_engine_order_for_trader(self, ctx.get("order_id"))
        fill_qty = int(ctx.get("qty") or broker_pos.get("qty", 0) or 0)
        apply_fill_to_session_ledger(
            self,
            symbol,
            fill_qty,
            ctx.get("action_type", ""),
            side=ctx.get("side") or broker_pos.get("side") or "",
        )

    def _handle_filled(self, symbol, broker_pos, ctx):
        """
        Jab order fill confirm ho jaaye tab ye function call hota hai.

        Entry pe  â†’ self.positions mein position daalo (sahi entry price ke saath)
        Exit pe   â†’ P&L calculate karo, phir position hatao
        """
        action_type = ctx.get("action_type", "")

        # Agar qty hi nahi hai toh kuch mat karo
        if broker_pos["qty"] <= 0:
            return

        # ================================================================
        # EXIT ACTIONS â€” Pehle P&L calculate karo, phir position hatao
        # ================================================================
        if action_type in {
            "EXIT_LONG",
            "COVER_SHORT",
            "TREND_VETO_EXIT_LONG",
            "TREND_VETO_EXIT_SHORT",
            "STOP_LOSS",
            "MARKET_CLOSE_EXIT"
        }:
            # âœ… Step 1: Exit price lo â€” broker_pos se aayegi (reconciliation ne set ki hogi)
            exit_price = float(broker_pos.get("avg_price") or 0.0)

            # âœ… Step 2: Agar exit price nahi mili toh LTP cache fallback
            # (LTP bilkul sahi nahi hoti exit price ke liye, but better than 0)
            if exit_price <= 0:
                exit_price = getattr(self, "_cycle_ltp_cache", {}).get(symbol, 0.0)
                if exit_price > 0:
                    print(f"[WARN] {symbol}: exit price broker se nahi mili, LTP use kar rahe hain: â‚¹{exit_price:.2f}")

            # âœ… Step 3: P&L calculate karo (self.positions abhi bhi exist karti hai)
            if exit_price > 0 and symbol in self.positions:
                pnl = self._close_position(self.session, symbol, exit_price)
                # _close_position andar se self.positions.pop() bhi karta hai
                print(f"[P&L REALIZED] {symbol} | Action: {action_type} | Realized: â‚¹{pnl:.2f}")
                self._log_trade(
                    symbol,
                    action_type,
                    0.0,
                    "closed",
                    exit_price,
                    broker_pos["qty"],
                    pnl
                )
                self._track_engine_fill(symbol, broker_pos, ctx)
            else:
                # Exit price nahi mili ya position nahi thi â€” bas hatao
                print(f"[WARN] {symbol}: exit price nahi mili ya position exist nahi karti â€” sirf pop kar rahe hain")
                self.positions.pop(symbol, None)

            self._track_engine_fill(symbol, broker_pos, ctx)
            return

        # ================================================================
        # ENTRY ACTIONS â€” Position save karo sahi entry price ke saath
        # ================================================================

        # âœ… Step 1: Broker se jo avg_price aaya wo lo
        entry_price = float(broker_pos.get("avg_price") or 0.0)

        # âœ… Step 2: Agar broker ne 0 diya (same candle issue) toh
        #    order book se actual fill price nikalo
        if entry_price <= 0:
            order_id = ctx.get("order_id")
            if order_id:
                entry_price = self._get_fill_price_from_orderbook(order_id, symbol)
                if entry_price > 0:
                    print(f"[ENTRY PRICE] {symbol}: order book se mili â‚¹{entry_price:.2f}")

        # âœ… Step 3: Order book se bhi nahi mili toh position save mat karo
        #    Next reconciliation cycle mein phir try hoga
        if entry_price <= 0:
            print(f"[WARN] {symbol}: entry price nahi mili â€” position set nahi hua, next cycle mein retry hoga")
            return

        # âœ… Step 4: Sahi entry price ke saath position save karo
        self.positions[symbol] = {
            "side": broker_pos["side"],
            "qty": broker_pos["qty"],
            "entry_price": entry_price        # â† actual fill price âœ…
        }

        print(
            f"[POSITION SET] {symbol}: "
            f"{broker_pos['side']} {broker_pos['qty']} @ â‚¹{entry_price:.2f}"
        )
        self._track_engine_fill(symbol, broker_pos, ctx)

    # -------- HANDLE REJECTED ---------

    def _handle_rejected(self, symbol, ctx):
        print(f" {symbol}: ORDER REJECTED / CANCELLED ({ctx.get('action_type')})")

    def _close_position(self, session, symbol, exit_price):
        if symbol not in self.positions:
            return 0.0

        pos = self.positions[symbol]
        qty = pos["qty"]
        entry = pos["entry_price"]
        side = pos["side"]

        # Realized PnL
        if side == "BUY":
            profit = (exit_price - entry) * qty
        else:
            profit = (entry - exit_price) * qty

        if side == "BUY":
            self.cash_balance += qty * exit_price
        else:
            pass

        self.realized_pnl += profit
        self.positions.pop(symbol, None)

        return profit

    def _is_position_settled(self, broker_pos):
        return (
            broker_pos is not None
            and broker_pos.get("qty", 0) > 0
            and broker_pos.get("avg_price", 0) > 0
            and broker_pos.get("side") in ("BUY", "SELL")
        )

    def _reconcile_pending_orders(self):
        while not self.stop_event.is_set():
            with self.pending_lock:
                if not self.pending_orders:
                    time.sleep(1)
                    continue    

                # pending_orders: symbol -> list[ctx]
                pending_snapshot = list(self.pending_orders.items())

            t_reconcile_start = time.time()

           
            with self.broker_pos_lock:

                self._broker_positions_cache = self._get_broker_positions()

                broker_positions = list(self._broker_positions_cache or [])

            for sym, ctx_list in pending_snapshot:
                # iterate over a COPY so we can safely remove
                for ctx in list(ctx_list):

                    expected_side = ctx.get("side")
                    expected_qty = ctx.get("qty", 0)
                    action_type = ctx.get("action_type", "")
                    order_id       = ctx.get("order_id")

                    is_exit = action_type in {
                        "EXIT_LONG",
                        "COVER_SHORT",
                        "TREND_VETO_EXIT_LONG",
                        "TREND_VETO_EXIT_SHORT",
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
                        flipped_pos = None
                        for p in broker_positions:
                            if p.get("symbol") != sym:
                                continue
                            if p.get("side") != expected_side:
                                continue
                            if p.get("qty", 0) <= 0:
                                continue
                            flipped_pos = p
                            break

                        if flipped_pos:
                            fill_price = float(flipped_pos.get("avg_price") or 0.0)
                            if fill_price <= 0 and order_id:
                                fill_price = self._get_fill_price_from_orderbook(order_id, sym)
                            self._handle_filled(
                                sym,
                                {
                                    "side": flipped_pos["side"],
                                    "qty": flipped_pos["qty"],       
                                    "avg_price": flipped_pos["avg_price"],
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
                                print(f"[RECONCILE] {sym}: avg_price order book se mili â‚¹{avg_price:.2f}")
                            else:
                                print(f"[RECONCILE] {sym}: avg_price abhi bhi 0 â€” next cycle mein retry hoga")

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
                                exit_price = self._get_fill_price_from_orderbook(order_id, sym)

                            # âœ… Fallback â€” LTP cache se lo (better than 0)
                            if exit_price <= 0:
                                exit_price = getattr(self, "_cycle_ltp_cache", {}).get(sym, 0.0)
                                if exit_price > 0:
                                    print(f"[WARN] {sym}: exit price order book se nahi mili, LTP use kar rahe hain â‚¹{exit_price:.2f}")

                            self._handle_filled(
                                sym,
                                {
                                    "side": expected_side,
                                    "qty": expected_qty,
                                    "avg_price": None  # price already realized
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
