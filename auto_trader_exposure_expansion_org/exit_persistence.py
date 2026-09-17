import os
import time
from datetime import datetime, timezone
from typing import Optional


class ExitPersistenceMixin:
    def _get_session_id_for_db(self) -> str:
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        return str(sid or "").strip()

    def _get_source_mode(self) -> str:
        return "live"

    def _get_exit_cycle_numbers(self) -> tuple:
        actual_cycle = int(
            getattr(self, "_cycle_count", 0)
            or getattr(self, "_current_cycle_count", 0)
            or getattr(self, "_restored_cycle_count", 0)
            or 0
        )
        return actual_cycle, max(actual_cycle + 1, 1)

    def _get_exited_symbols_collection(self):
        base = self.trading_logs_collection
        if base is None:
            return None
        db_name = getattr(self, "config_db_name", None) or os.environ.get("MONGO_CONFIG_DB_NAME", "test")
        coll = (
            base.database.client[db_name]["exited_symbols"]
            if db_name != base.database.name
            else base.database["exited_symbols"]
        )
        if not getattr(self, "_exited_symbols_index_ready", False):
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

        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if self._normalize_config_symbol(pos.get("symbol")) == sym:
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
            "STOP_LOCK_EXIT": "STOP LOCK EXITED",
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
