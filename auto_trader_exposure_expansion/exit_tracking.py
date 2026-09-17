"""AutoTrader mixin: exited-symbols persistence (Mongo), exit reason/label
resolution, and post-fill exit finalization. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
import time
from datetime import datetime, timezone
from typing import Optional


class ExitTrackingMixin:

    def _get_session_id_for_db(self) -> str:
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        return str(sid or "").strip()

    def _get_source_mode(self) -> str:
        session = getattr(self, "session", None)
        if isinstance(session, dict) and session.get("paper"):
            return "simulation"
        return "live"

    def _get_exit_cycle_numbers(self) -> tuple:
        actual_cycle = int(
            getattr(self, "_current_cycle_count", 0)
            or getattr(self, "_restored_cycle_count", 0)
            or 0
        )
        return actual_cycle, max(actual_cycle + 1, 1)

    def _get_exited_symbols_collection(self):
        base = self.trading_logs_collection
        if base is None:
            return None
        db_name = self._resolve_config_db_name()
        coll = (
            base.database.client[db_name]["exited_symbols"]
            if db_name != base.database.name
            else base.database["exited_symbols"]
        )
        if not self._exited_symbols_index_ready:
            try:
                coll.create_index(
                    [("session_id", 1), ("symbol", 1)],
                    unique=True,
                    name="uniq_session_symbol_exit",
                )
                coll.create_index(
                    [("session_id", 1), ("status", 1)],
                    name="idx_session_exit_status",
                )
            except Exception as exc:
                print(f"[EXITED-SYMBOLS] index ensure failed: {exc}")
            self._exited_symbols_index_ready = True
        return coll

    def _snapshot_position_for_exit(self, symbol: str, fallback: Optional[dict] = None) -> dict:
        sym = self._normalize_config_symbol(symbol)
        with self.positions_lock:
            pos = self.positions.get(sym)
            if pos:
                return {
                    "symbol": sym,
                    "side": pos.get("side"),
                    "qty": int(pos.get("qty") or 0),
                    "entry_price": float(pos.get("entry_price") or 0.0),
                    "ltp": float(pos.get("ltp") or 0.0),
                }

        with self._paper_lock:
            pos = self._paper_positions.get(sym)
            if pos:
                return {
                    "symbol": sym,
                    "side": pos.get("side"),
                    "qty": int(pos.get("qty") or 0),
                    "entry_price": float(pos.get("avg_price") or pos.get("entry_price") or 0.0),
                    "ltp": float(pos.get("ltp") or 0.0),
                }

        fallback = fallback or {}
        return {
            "symbol": sym,
            "side": fallback.get("side"),
            "qty": int(fallback.get("qty") or 0),
            "entry_price": float(fallback.get("entry_price") or fallback.get("avg_price") or 0.0),
            "ltp": float(fallback.get("ltp") or 0.0),
        }

    def _exit_action_label(self, exit_reason: str) -> str:
        labels = {
            "RMS_TICKER_EXIT": "RMS EXITED",
            "STOP_LOSS_EXIT": "STOP LOSS EXITED",
            "MANUAL_EXIT": "MANUAL EXITED",
            "PORTFOLIO_RMS_EXIT": "PORTFOLIO RMS EXITED",
        }
        return labels.get(exit_reason or "", "EXITED")

    def _resolve_exit_reason(self, action_type: str, ctx: Optional[dict] = None) -> str:
        ctx = ctx or {}
        explicit = ctx.get("exit_reason")
        if explicit:
            return str(explicit).upper()
        if action_type == "STOP_LOSS":
            return "STOP_LOSS_EXIT"
        if action_type in ("EXIT_LONG", "COVER_SHORT"):
            return "MANUAL_EXIT"
        return "EXIT"

    def _clear_symbol_live_pnl_after_exit(
        self,
        symbol: str,
        *,
        exit_price: float = 0.0,
        entry_price: float = 0.0,
        side: str = "",
        exit_reason: str = "",
    ) -> None:
        sym = self._normalize_config_symbol(symbol)
        self._exited_symbols.add(sym)
        with self.live_pnl_lock:
            old = self.live_pnl.get(sym, {})
            self.live_pnl[sym] = {
                "ltp": float(exit_price or old.get("ltp") or 0.0),
                "pnl": 0.0,
                "qty": 0,
                "entry": float(entry_price or old.get("entry") or 0.0),
                "side": side or old.get("side", ""),
                "ts": time.time(),
                "status": "EXITED",
                "exit_reason": exit_reason,
            }
        with self._paper_lock:
            self._paper_positions.pop(sym, None)
        print(
            f"[EXITED-SYMBOLS] live pnl cleared session={self._get_session_id_for_db()} "
            f"symbol={sym} reason={exit_reason} exit_price={float(exit_price or 0.0):.2f}"
        )

    def _persist_exit_pending(
        self,
        symbol: str,
        *,
        exit_reason: str,
        exit_side: str,
        qty: int,
        entry_price: float,
        exit_order_id: Optional[str],
    ) -> None:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        if not sid or coll is None:
            print(
                f"[EXITED-SYMBOLS] pending persist skipped symbol={symbol} "
                f"session_present={bool(sid)} collection_present={coll is not None}"
            )
            return

        actual_cycle, display_cycle = self._get_exit_cycle_numbers()
        now_market = self._now_market_time()
        now_utc = datetime.now(timezone.utc)
        sym = self._normalize_config_symbol(symbol)
        try:
            existing = coll.find_one({"session_id": sid, "symbol": sym}, {"status": 1})
            if existing and str(existing.get("status") or "").upper() in {"EXITED", "RMS_EXITED", "CLOSED"}:
                return
        except Exception:
            pass

        pending_unrealized = self._get_symbol_unrealized_pnl(sym)
        doc = {
            "session_id": sid,
            "configuration_id": getattr(self, "configuration_id", None),
            "symbol": sym,
            "status": "EXIT_PENDING",
            "position_status": "EXIT_PENDING",
            "exit_reason": exit_reason,
            "entry_side": "BUY" if exit_side == "SELL" else "SELL",
            "exit_side": exit_side,
            "qty": int(qty or 0),
            "entry_price": round(float(entry_price or 0.0), 2),
            "exit_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": pending_unrealized,
            "total_pnl": pending_unrealized,
            "exit_actual_cycle": actual_cycle,
            "display_cycle": display_cycle,
            "exit_order_id": exit_order_id,
            "exit_order_status": "PENDING",
            "source": self._get_source_mode(),
            "exit_time": None,
            "exit_time_utc": None,
            "updated_at": now_utc.isoformat(),
        }
        try:
            coll.update_one(
                {"session_id": sid, "symbol": sym},
                {"$set": doc, "$setOnInsert": {"created_at": now_utc.isoformat(), "requested_at": now_market.strftime("%Y-%m-%d %H:%M:%S")}},
                upsert=True,
            )
            print(
                f"[EXITED-SYMBOLS] pending saved session={sid} symbol={sym} "
                f"reason={exit_reason} order_id={exit_order_id} "
                f"actual_cycle={actual_cycle} display_cycle={display_cycle} "
                f"unrealized={pending_unrealized:.2f}"
            )
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] pending persist failed for {sym}: {exc}")

    def _persist_exit_trading_log(self, exit_doc: dict) -> None:
        coll = self.trading_logs_collection
        if coll is None:
            print(f"[TRADING-LOGS] exit row skipped symbol={exit_doc.get('symbol')} collection_present=False")
            return

        sid = exit_doc.get("session_id")
        sym = exit_doc.get("symbol")
        if not sid or not sym:
            print(f"[TRADING-LOGS] exit row skipped missing session/symbol session={sid} symbol={sym}")
            return

        display_cycle = int(exit_doc.get("display_cycle") or 1)
        realized = round(float(exit_doc.get("realized_pnl") or 0.0), 2)
        total = round(float(exit_doc.get("total_pnl") or realized), 2)
        timestamp = exit_doc.get("exit_time") or self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
        portfolio_unrealized = round(float(getattr(self, "unrealized_pnl", 0.0) or 0.0), 2)
        portfolio_realized = round(float(getattr(self, "realized_pnl", 0.0) or 0.0), 2)
        portfolio_pnl = round(portfolio_realized + portfolio_unrealized, 2)
        total_equity = round(float(self.cash_balance or 0.0) + portfolio_realized + portfolio_unrealized, 2)

        row = {
            "session_id": sid,
            "configuration_id": exit_doc.get("configuration_id"),
            "cycle": display_cycle,
            "timestamp": timestamp,
            "simulation_logs": bool(getattr(self, "simulation_logs", True)),
            "symbol": sym,
            "curr_price": exit_doc.get("exit_price"),
            "return_pct": 0.0,
            "trajectory_pct": None,
            "side": exit_doc.get("entry_side"),
            "signal": None,
            "action": self._exit_action_label(exit_doc.get("exit_reason")),
            "qty": int(exit_doc.get("qty") or 0),
            "unrealized_pnl": 0.0,
            "symbol_unrealized_pnl": 0.0,
            "symbol_realized_pnl": realized,
            "exit_realized_pnl": exit_doc.get("exit_realized_pnl", realized),
            "symbol_pnl": total,
            "cash_balance": round(float(self.cash_balance or 0.0), 2),
            "realized_pnl": portfolio_realized,
            "pnl": total,
            "total_equity": total_equity,
            "portfolio_cash_balance": round(float(self.cash_balance or 0.0), 2),
            "portfolio_realized_pnl": portfolio_realized,
            "portfolio_unrealized_pnl": portfolio_unrealized,
            "portfolio_pnl": portfolio_pnl,
            "portfolio_total_equity": total_equity,
            "is_exit_row": True,
            "source": "exited_symbols",
            "status": exit_doc.get("status"),
            "position_status": exit_doc.get("position_status") or exit_doc.get("status"),
            "exit_reason": exit_doc.get("exit_reason"),
            "exit_side": exit_doc.get("exit_side"),
            "entry_price": exit_doc.get("entry_price"),
            "exit_price": exit_doc.get("exit_price"),
            "exit_order_id": exit_doc.get("exit_order_id"),
            "exit_order_status": exit_doc.get("exit_order_status"),
            "exit_actual_cycle": exit_doc.get("exit_actual_cycle"),
            "display_cycle": display_cycle,
            "exit_time": exit_doc.get("exit_time"),
            "exit_time_utc": exit_doc.get("exit_time_utc"),
        }
        try:
            coll.update_one(
                {"session_id": sid, "symbol": sym, "is_exit_row": True},
                {"$set": row, "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            )
            print(
                f"[TRADING-LOGS] exit row upserted session={sid} symbol={sym} "
                f"cycle={display_cycle} actual_cycle={exit_doc.get('exit_actual_cycle')} "
                f"reason={exit_doc.get('exit_reason')} realized={realized:.2f} "
                f"unrealized=0.00 order_id={exit_doc.get('exit_order_id')}"
            )
        except Exception as exc:
            print(f"[TRADING-LOGS] exit row persist failed for {sym}: {exc}")

    def _persist_symbol_exit(
        self,
        symbol: str,
        *,
        exit_reason: str,
        action_type: str,
        exit_side: str,
        qty: int,
        entry_price: float,
        exit_price: float,
        realized_pnl: float,
        exit_order_id: Optional[str],
        exit_order_status: str = "FILLED",
    ) -> dict:
        sid = self._get_session_id_for_db()
        actual_cycle, display_cycle = self._get_exit_cycle_numbers()
        sym = self._normalize_config_symbol(symbol)
        now_market = self._now_market_time()
        now_utc = datetime.now(timezone.utc)
        exit_realized_pnl = round(float(realized_pnl or 0.0), 2)
        cumulative_realized_pnl = round(
            float(self.realized_pnl_by_symbol.get(sym, exit_realized_pnl) or exit_realized_pnl),
            2,
        )
        total_pnl = cumulative_realized_pnl
        entry_side = "BUY" if exit_side == "SELL" else "SELL"
        doc = {
            "session_id": sid,
            "configuration_id": getattr(self, "configuration_id", None),
            "symbol": sym,
            "status": "EXITED",
            "position_status": "EXITED",
            "exit_reason": exit_reason,
            "action_type": action_type,
            "entry_side": entry_side,
            "exit_side": exit_side,
            "qty": int(qty or 0),
            "entry_price": round(float(entry_price or 0.0), 2),
            "exit_price": round(float(exit_price or 0.0), 2),
            "exit_realized_pnl": exit_realized_pnl,
            "realized_pnl": cumulative_realized_pnl,
            "unrealized_pnl": 0.0,
            "total_pnl": total_pnl,
            "exit_actual_cycle": actual_cycle,
            "display_cycle": display_cycle,
            "exit_time": now_market.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time_utc": now_utc.isoformat(),
            "source": self._get_source_mode(),
            "exit_order_id": exit_order_id,
            "exit_order_status": exit_order_status,
            "updated_at": now_utc.isoformat(),
        }

        coll = self._get_exited_symbols_collection()
        if sid and coll is not None:
            try:
                coll.update_one(
                    {"session_id": sid, "symbol": sym},
                    {"$set": doc, "$setOnInsert": {"created_at": now_utc.isoformat()}},
                    upsert=True,
                )
                print(
                    f"[EXITED-SYMBOLS] final saved session={sid} symbol={sym} "
                    f"reason={exit_reason} status=EXITED order_id={exit_order_id} "
                    f"entry={entry_price:.2f} exit={exit_price:.2f} qty={int(qty or 0)} "
                    f"exit_realized={exit_realized_pnl:.2f} cumulative_realized={cumulative_realized_pnl:.2f} "
                    f"display_cycle={display_cycle}"
                )
            except Exception as exc:
                print(f"[EXITED-SYMBOLS] persist failed for {sym}: {exc}")
        else:
            print(
                f"[EXITED-SYMBOLS] final persist skipped symbol={sym} "
                f"session_present={bool(sid)} collection_present={coll is not None}"
            )

        self._persist_exit_trading_log(doc)
        return doc

    def _get_exited_symbol_doc(self, symbol: str) -> Optional[dict]:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        sym = self._normalize_config_symbol(symbol)
        if not sid or coll is None or not sym:
            return None
        try:
            doc = coll.find_one({"session_id": sid, "symbol": sym})
            if doc and str(doc.get("status") or "").upper() in {"EXITED", "RMS_EXITED", "CLOSED"}:
                print(
                    f"[EXITED-SYMBOLS] found closed symbol session={sid} symbol={sym} "
                    f"status={doc.get('status')} reason={doc.get('exit_reason')} "
                    f"realized={doc.get('realized_pnl')} unrealized={doc.get('unrealized_pnl')}"
                )
                return doc
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] lookup failed for {sym}: {exc}")
        return None

    def _sync_exited_symbols_from_db(self) -> None:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        if not sid or coll is None:
            return
        try:
            cursor = coll.find(
                {"session_id": sid, "status": {"$in": ["EXIT_PENDING", "EXITED", "RMS_EXITED", "CLOSED"]}},
                {"symbol": 1},
            )
            synced = set()
            for doc in cursor:
                sym = self._normalize_config_symbol(doc.get("symbol"))
                if sym:
                    synced.add(sym)
            if synced:
                before = len(self._exited_symbols)
                self._exited_symbols.update(synced)
                if len(self._exited_symbols) > before:
                    print(f"[EXITED-SYMBOLS] Synced from DB: {sorted(synced)}")
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] sync failed: {exc}")

    def _finalize_symbol_exit_fill(self, symbol: str, broker_pos: dict, ctx: dict) -> dict:
        sym = self._normalize_config_symbol(symbol)
        action_type = ctx.get("action_type", "")
        exit_reason = self._resolve_exit_reason(action_type, ctx)
        exit_price = float(broker_pos.get("avg_price") or 0.0)
        exit_qty = int(broker_pos.get("qty") or ctx.get("qty") or 0)
        exit_side = (ctx.get("side") or broker_pos.get("side") or "").upper()
        pre_exit = ctx.get("pre_exit_position") or self._snapshot_position_for_exit(sym)
        entry_price = float(pre_exit.get("entry_price") or 0.0)
        entry_side = pre_exit.get("side") or ("BUY" if exit_side == "SELL" else "SELL")
        print(
            f"[EXIT-FINALIZE] start symbol={sym} action={action_type} reason={exit_reason} "
            f"entry_side={entry_side} exit_side={exit_side} qty={exit_qty} "
            f"entry={entry_price:.2f} exit={exit_price:.2f} order_id={ctx.get('order_id')}"
        )

        pnl = self._close_position(
            self.session,
            sym,
            exit_price,
            exit_qty,
            position_snapshot=pre_exit,
        )
        print(f"[P&L REALIZED] {sym} | Action: {action_type} | Reason: {exit_reason} | Realized: {pnl:.2f}")

        self._log_trade(
            sym,
            action_type,
            0.0,
            "closed",
            exit_price,
            exit_qty,
            pnl,
        )

        remaining_qty = self._get_symbol_position_qty(sym)
        if remaining_qty <= 0:
            self._clear_symbol_live_pnl_after_exit(
                sym,
                exit_price=exit_price,
                entry_price=entry_price,
                side=entry_side,
                exit_reason=exit_reason,
            )

        exit_doc = self._persist_symbol_exit(
            sym,
            exit_reason=exit_reason,
            action_type=action_type,
            exit_side=exit_side,
            qty=exit_qty,
            entry_price=entry_price,
            exit_price=exit_price,
            realized_pnl=pnl,
            exit_order_id=ctx.get("order_id"),
            exit_order_status="FILLED",
        )
        self._persist_paper_state_snapshot(event=f"exit:{sym}")
        print(
            f"[EXIT-FINALIZE] done symbol={sym} reason={exit_reason} "
            f"realized={exit_doc.get('realized_pnl')} unrealized={exit_doc.get('unrealized_pnl')} "
            f"trading_log_cycle={exit_doc.get('display_cycle')}"
        )
        return exit_doc

