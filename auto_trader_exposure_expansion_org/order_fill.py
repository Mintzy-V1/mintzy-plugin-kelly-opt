import time
from datetime import datetime

from utils.session_ledger import (
    apply_fill_to_session_ledger,
    record_engine_order_for_trader,
)


class OrderFillMixin:
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

                #  Pehle orderstatus check karo
                order_status = order.get("orderstatus", "").lower()

                if order_status == "cancelled":
                    print(f"[FILL PRICE] {symbol}: order {order_id} CANCELLED hai  fill price nahi milegi")
                    return 0.0

                if order_status == "rejected":
                    print(f"[FILL PRICE] {symbol}: order {order_id} REJECTED hai  fill price nahi milegi")
                    return 0.0

                if order_status not in ("complete", "filled"):
                    # open, pending, trigger pending etc.
                    print(f"[FILL PRICE] {symbol}: order {order_id} abhi {order_status} hai  wait karo")
                    return 0.0

                #  Order complete hai  ab averageprice lo
                # fill_price = float(order.get("averageprice") or 0.0)

                #  Order complete hai  ab averageprice lo
                # fill_price = float(order.get("price") or 0.0)
                # avg_price_field = float(order.get("averageprice") or 0.0)

                #  averageprice = actual execution price (AngelOne dashboard bhi yahi use karta hai)
                avg_price_field = float(order.get("averageprice") or 0.0)
                fill_price = float(order.get("price") or 0.0)
            
                print(
                    f"[FILL PRICE] {symbol}: order {order_id} | "
                    f"price={fill_price:.2f} | averageprice={avg_price_field:.2f}"
                )

                #  filledshares bhi check karo
                filled_shares = int(order.get("filledshares") or 0)

                # if fill_price > 0 and filled_shares > 0:
                #         print(f"[FILL PRICE] {symbol}: order {order_id} complete @ Rs {fill_price:.2f} (price field) | averageprice=Rs {avg_price_field:.2f} ({filled_shares} shares)")
                #         return fill_price
                # elif avg_price_field > 0 and filled_shares > 0:
                #         print(f"[FILL PRICE] {symbol}: price=0 fallback to averageprice=Rs {avg_price_field:.2f}")
                #         return avg_price_field
                # else:
                #         print(f"[FILL PRICE] {symbol}: order complete but both price=0 and averageprice=0 or filledshares=0")
                #         return 0.0
                if avg_price_field > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: averageprice=Rs {avg_price_field:.2f} ({filled_shares} shares)")
                    return avg_price_field
                elif fill_price > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: averageprice=0, fallback to price=Rs {fill_price:.2f}")
                    return fill_price
                else:
                    print(f"[FILL PRICE] {symbol}: both 0 or filledshares=0")
                    return 0.0

            print(f"[FILL PRICE] {symbol}: order {order_id} order book mein nahi mila")
            return 0.0

        except Exception as e:
            print(f"[FILL PRICE ERROR] {symbol}: {e}")
            return 0.0   

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

    # ------- HANDLE FILLED -------- 

    def _handle_filled(self, symbol, broker_pos, ctx):
        """
        Jab order fill confirm ho jaaye tab ye function call hota hai.

        Entry pe   self.positions mein position daalo (sahi entry price ke saath)
        Exit pe    P&L calculate karo, phir position hatao
        """
        action_type = ctx.get("action_type", "")

        # Agar qty hi nahi hai toh kuch mat karo
        if broker_pos["qty"] <= 0:
            return

        # ================================================================
        # EXIT ACTIONS Pehle P&L calculate karo, phir position hatao
        # ================================================================
        if action_type in {
            "EXIT_LONG",
            "COVER_SHORT",
            "STOP_LOSS",
            "MARKET_CLOSE_EXIT",
            "FLIP_TO_LONG",
            "FLIP_TO_SHORT"
        }:
            # Step 1: Exit price lo broker_pos se aayegi (reconciliation ne set ki hogi)
            exit_price = float(broker_pos.get("avg_price") or 0.0)

            # Step 2: P&L calculate karo (self.positions abhi bhi exist karti hai)
            if (
                action_type in {"EXIT_LONG", "COVER_SHORT", "STOP_LOSS"}
                and exit_price > 0
                and (symbol in self.positions or ctx.get("pre_exit_position"))
            ):
                exit_qty = broker_pos.get("qty", 0)
                self._finalize_symbol_exit_fill(
                    symbol,
                    {
                        "side": ctx.get("side") or broker_pos.get("side"),
                        "qty": exit_qty,
                        "avg_price": exit_price,
                    },
                    ctx,
                )
                return

            if exit_price > 0 and symbol in self.positions:
                exit_qty = broker_pos.get("qty", 0)
                pnl = self._close_position(self.session, symbol, exit_price, exit_qty)
                print(f"[P&L REALIZED] {symbol} | Action: {action_type} | Realized: {pnl:.2f}")
                self._log_trade(
                    symbol,
                    action_type,
                    0.0,
                    "closed",
                    exit_price,
                    exit_qty,
                    pnl
                )
                self._track_engine_fill(symbol, broker_pos, ctx)
                return
            elif action_type in ("EXIT_LONG", "COVER_SHORT", "STOP_LOSS", "MARKET_CLOSE_EXIT"):
                # Missing price/position for normal exit
                print(f"[WARN] {symbol}: exit price nahi mili ya position exist nahi karti sirf pop kar rahe hain")
                self.positions.pop(symbol, None)
                self._track_engine_fill(symbol, broker_pos, ctx)
                return

            return

        # ================================================================
        # ENTRY ACTIONS  Position save karo sahi entry price ke saath
        # ================================================================

        #  Step 1: Broker se jo avg_price aaya wo lo
        entry_price = float(broker_pos.get("avg_price") or 0.0)

        #  Step 2: Agar broker ne 0 diya (same candle issue) toh
        #    order book se actual fill price nikalo
        if entry_price <= 0:
            order_id = ctx.get("order_id")
            if order_id:
                entry_price = self._get_fill_price_from_orderbook(order_id, symbol)
                if entry_price > 0:
                    print(f"[ENTRY PRICE] {symbol}: order book se mili Rs {entry_price:.2f}")

        #  Step 3: Order book se bhi nahi mili toh position save mat karo
        #    Next reconciliation cycle mein phir try hoga
        if entry_price <= 0:
            print(f"[WARN] {symbol}: entry price nahi mili  position set nahi hua, next cycle mein retry hoga")
            return

        #  Step 4: Sahi entry price ke saath position save karo
        # self.positions[symbol] = {
        #     "side": broker_pos["side"],
        #     "qty": broker_pos["qty"],
        #     "entry_price": entry_price        #  actual fill price 
        # }
        with self.positions_lock:
            if symbol in self.positions:
                old_qty = self.positions[symbol]["qty"]
                old_avg = self.positions[symbol]["entry_price"]
                new_qty = broker_pos["qty"]
                
                blended_avg = ((old_qty * old_avg) + (new_qty * entry_price)) / (old_qty + new_qty)
                self.positions[symbol]["entry_price"] = blended_avg
                self.positions[symbol]["qty"] += new_qty
            else:
                self.positions[symbol] = {
                    "side": broker_pos["side"],
                    "qty": broker_pos["qty"],
                    "entry_price": entry_price
                }

        print(
            f"[POSITION SET] {symbol}: "
            f"{broker_pos['side']} {broker_pos['qty']} @ Rs {entry_price:.2f}"
        )
        self._track_engine_fill(symbol, broker_pos, ctx)

    # -------- HANDLE REJECTED ---------

    def _handle_rejected(self, symbol, ctx):
        print(f" {symbol}: ORDER REJECTED / CANCELLED ({ctx.get('action_type')})")

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
                        # "TREND_VETO_EXIT_LONG",
                        # "TREND_VETO_EXIT_SHORT",
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
