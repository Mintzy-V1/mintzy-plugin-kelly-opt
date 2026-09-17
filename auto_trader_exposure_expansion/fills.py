"""AutoTrader mixin: order-fill handling (paper trading engine), rejection
handling, and paper-position bookkeeping. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
from datetime import datetime
from typing import Optional

from utils.session_ledger import (
    apply_fill_to_session_ledger,
    record_engine_order_for_trader,
)

from .order_execution import OrderRequest


class FillsMixin:

    def _get_fill_price_from_orderbook(self, order_id, symbol):
        try:
            with self._paper_lock:
                order = self._paper_orders.get(str(order_id))
            if not order:
                print(f"[FILL PRICE] {symbol}: order {order_id} not found in paper book")
                return 0.0

            order_status = str(order.get("status") or order.get("orderstatus") or "").lower()
            if order_status == "cancelled":
                print(f"[FILL PRICE] {symbol}: order {order_id} CANCELLED")
                return 0.0
            if order_status == "rejected":
                print(f"[FILL PRICE] {symbol}: order {order_id} REJECTED")
                return 0.0
            if order_status not in ("complete", "filled"):
                print(f"[FILL PRICE] {symbol}: order {order_id} still {order_status}")
                return 0.0

            avg_price_field = float(order.get("avg_fill_price") or order.get("averageprice") or 0.0)
            filled_shares = int(order.get("filled_qty") or order.get("filledshares") or 0)
            if avg_price_field > 0 and filled_shares > 0:
                print(f"[FILL PRICE] {symbol}: paper fill @ {avg_price_field:.2f} ({filled_shares} shares)")
                return avg_price_field
            print(f"[FILL PRICE] {symbol}: paper order complete but price/qty missing")
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
                self._track_engine_fill(symbol, broker_pos, ctx)
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
                self._persist_paper_state_snapshot(event=f"exit:{symbol}")
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
        self._persist_paper_state_snapshot(event=f"entry:{symbol}")
        self._track_engine_fill(symbol, broker_pos, ctx)

    # -------- HANDLE REJECTED ---------

    def _handle_rejected(self, symbol, ctx):
        print(f" {symbol}: ORDER REJECTED / CANCELLED ({ctx.get('action_type')})")

    # ---------- PAPER TRADING ENGINE ----------

    def _apply_paper_slippage(self, side: str, ltp: float) -> float:
        slip = float(getattr(self, "paper_slippage_pct", 0.0) or 0.0)
        if slip <= 0 or ltp <= 0:
            return ltp
        side = (side or "").upper()
        if side == "BUY":
            return ltp * (1.0 + slip)
        if side == "SELL":
            return ltp * (1.0 - slip)
        return ltp

    def _resolve_paper_ltp(self, symbol: str, order_req: Optional[OrderRequest] = None) -> float:
        symbol = symbol.upper().replace("-EQ", "")
        with self.live_pnl_lock:
            tick = self.live_pnl.get(symbol)
            if tick and float(tick.get("ltp") or 0) > 0:
                return float(tick["ltp"])

        cache = getattr(self, "_cycle_ltp_cache", {}) or {}
        cached = cache.get(symbol)
        if cached is not None and float(cached) > 0:
            return float(cached)

        candle = getattr(self, "_current_candle", "5m")
        live = self._get_live_price_redis(symbol, candle)
        if live is not None and float(live) > 0:
            return float(live)

        if order_req and order_req.metadata:
            curr_price = order_req.metadata.get("curr_price")
            if curr_price is not None and float(curr_price) > 0:
                return float(curr_price)

        with self._paper_lock:
            pos = self._paper_positions.get(symbol)
            if pos and float(pos.get("ltp") or 0) > 0:
                return float(pos["ltp"])

        return 0.0

    def _update_paper_positions_on_fill(self, order_req: OrderRequest, fill_price: float) -> None:
        symbol = order_req.symbol.upper().replace("-EQ", "")
        side = order_req.side.upper()
        qty = int(order_req.qty or 0)
        meta = order_req.metadata or {}
        action = meta.get("action_type", "")
        ltp = float(fill_price or 0.0)

        exit_actions = {
            "EXIT_LONG", "COVER_SHORT", "STOP_LOSS", "MARKET_CLOSE_EXIT",
            "SINGLE_EXIT",
        }
        flip_actions = {"FLIP_TO_LONG", "FLIP_TO_SHORT"}

        pos = self._paper_positions.get(symbol)

        if action in flip_actions:
            new_qty = max(qty // 2, 0)
            if new_qty <= 0:
                self._paper_positions.pop(symbol, None)
                return
            new_side = "BUY" if action == "FLIP_TO_LONG" else "SELL"
            self._paper_positions[symbol] = {
                "symbol": symbol,
                "side": new_side,
                "qty": new_qty,
                "avg_price": ltp,
                "ltp": ltp,
            }
            return

        if action in exit_actions or (
            pos and (
                (pos["side"] == "BUY" and side == "SELL") or
                (pos["side"] == "SELL" and side == "BUY")
            )
        ):
            if not pos:
                return
            remaining = int(pos["qty"]) - qty
            if remaining <= 0:
                self._paper_positions.pop(symbol, None)
            else:
                pos["qty"] = remaining
                pos["ltp"] = ltp
            return

        if pos and pos["side"] == side:
            old_qty = int(pos["qty"])
            old_avg = float(pos["avg_price"])
            new_qty = old_qty + qty
            pos["avg_price"] = ((old_qty * old_avg) + (qty * ltp)) / new_qty
            pos["qty"] = new_qty
            pos["ltp"] = ltp
            return

        self._paper_positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "avg_price": ltp,
            "ltp": ltp,
        }

    def _create_paper_order(self, order_req: OrderRequest) -> dict:
        symbol = order_req.symbol.upper().replace("-EQ", "")
        raw_ltp = self._resolve_paper_ltp(symbol, order_req)
        fill_price = self._apply_paper_slippage(order_req.side, raw_ltp)

        if fill_price <= 0:
            return {
                "status": "error",
                "error": f"No market LTP available for {symbol}",
            }

        placed_at = datetime.now()
        with self._paper_lock:
            self._paper_order_counter += 1
            order_id = f"PAPER-{self._paper_order_counter:08d}"
            order_record = {
                "order_id": order_id,
                "orderid": order_id,
                "symbol": symbol,
                "side": order_req.side.upper(),
                "qty": int(order_req.qty),
                "order_type": order_req.order_type,
                "product_type": order_req.product_type,
                "price": fill_price,
                "stop_loss": order_req.stop_loss,
                "trigger_price": order_req.trigger_price,
                "status": "complete",
                "orderstatus": "complete",
                "filled_qty": int(order_req.qty),
                "filledshares": int(order_req.qty),
                "avg_fill_price": fill_price,
                "averageprice": fill_price,
                "placed_at": placed_at,
                "filled_at": placed_at,
                "metadata": order_req.metadata,
            }
            self._paper_orders[order_id] = order_record
            self._update_paper_positions_on_fill(order_req, fill_price)

        # Keep self.positions in sync (PnL / tick handler use this dict).
        with self.positions_lock:
            paper_pos = self._paper_positions.get(symbol)
            if paper_pos and int(paper_pos.get("qty") or 0) > 0:
                self.positions[symbol] = {
                    "side": paper_pos["side"],
                    "qty": int(paper_pos["qty"]),
                    "entry_price": float(paper_pos.get("avg_price") or fill_price),
                }
            else:
                self.positions.pop(symbol, None)

        print(
            f"[PAPER-ORDER] {order_id} {order_req.side} {order_req.qty} {symbol} "
            f"@ {fill_price:.2f} (LTP={raw_ltp:.2f})"
        )
        self._persist_paper_state_snapshot(event=f"order_fill:{order_id}")
        return {
            "order_id": order_id,
            "status": "success",
            "avg_fill_price": fill_price,
            "filled_qty": int(order_req.qty),
        }

    def _get_paper_orders_for_tradebook(self) -> list:
        with self._paper_lock:
            orders = list(self._paper_orders.values())
        rows = []
        for o in orders:
            rows.append({
                "orderid": o.get("order_id"),
                "tradingsymbol": o.get("symbol"),
                "transactiontype": o.get("side"),
                "quantity": o.get("qty"),
                "averageprice": o.get("avg_fill_price"),
                "orderstatus": o.get("status"),
                "filledshares": o.get("filled_qty"),
                "updatetime": (
                    o.get("filled_at").strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(o.get("filled_at"), datetime) else ""
                ),
            })
        return rows

    def _build_current_ui_rows(self) -> list:
        rows = []
        symbols = set(self.positions.keys()) | set(self._paper_positions.keys())
        for sym in sorted(symbols):
            paper_pos = self._paper_positions.get(sym)
            internal_pos = self.positions.get(sym)
            pos = paper_pos or internal_pos
            if not pos:
                continue
            side = pos.get("side", "NONE")
            qty = int(pos.get("qty") or 0)
            if qty <= 0:
                continue
            # Prefer paper position for live LTP; fall back to entry_price on internal dict.
            price_src = paper_pos or internal_pos
            curr_price = float(
                price_src.get("ltp")
                or price_src.get("avg_price")
                or price_src.get("entry_price")
                or 0.0
            )
            cache = getattr(self, "_cycle_ltp_cache", {}) or {}
            if sym in cache and float(cache[sym]) > 0:
                curr_price = float(cache[sym])
            live_pnl = self._calculate_pnl(sym, curr_price) if curr_price > 0 else 0.0
            symbol_realized = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
            symbol_unrealized = round(live_pnl, 2)
            symbol_pnl = round(symbol_realized + symbol_unrealized, 2)
            rows.append({
                "symbol": sym,
                "curr_price": round(curr_price, 2),
                "return_pct": 0.0,
                "side": side,
                "signal": "PAPER",
                "action": "HOLD (Paper)",
                "qty": qty,
                "unrealized_pnl": symbol_unrealized,
                "symbol_unrealized_pnl": symbol_unrealized,
                "symbol_realized_pnl": symbol_realized,
                "symbol_pnl": symbol_pnl,
                "pnl": symbol_pnl,
            })
        return rows

    def _get_symbol_position_qty(self, symbol: str) -> int:
        symbol = symbol.upper().replace("-EQ", "")
        with self.positions_lock:
            pos = self.positions.get(symbol)
            if pos:
                return int(pos.get("qty") or 0)
        with self._paper_lock:
            pos = self._paper_positions.get(symbol)
            if pos:
                return int(pos.get("qty") or 0)
        return 0

    def _get_symbol_unrealized_pnl(self, symbol: str) -> float:
        symbol = symbol.upper().replace("-EQ", "")
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

        with self._paper_lock:
            pos = self._paper_positions.get(symbol)
            if pos:
                ltp = float(pos.get("ltp") or pos.get("avg_price") or 0.0)
                if ltp > 0:
                    return float(self._calculate_pnl(symbol, ltp))

        with self.positions_lock:
            pos = self.positions.get(symbol)
            if pos:
                ltp = float(pos.get("ltp") or pos.get("entry_price") or 0.0)
                if ltp > 0:
                    return float(self._calculate_pnl(symbol, ltp))
        return 0.0

