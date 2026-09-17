"""AutoTrader mixin: broker RMS free-cash resolution and pyramid handoff/
PNL-row persistence helpers. Moved verbatim from auto_trader_exposure_expansion.py
during the package split; no logic changes.
"""
from datetime import datetime, timezone
from typing import Optional


class BrokerCashMixin:

    def _resolve_live_broker_session(self):
        """Return a SmartConnect session suitable for RMS / balance calls."""
        live = getattr(self, "broker_live_session", None)
        if isinstance(live, dict) and live.get("obj"):
            return live

        session = getattr(self, "session", None)
        if isinstance(session, dict) and session.get("obj") and not session.get("paper"):
            self.broker_live_session = session
            return session

        payload = getattr(self, "broker_session_payload", None)
        broker = getattr(self, "broker", None)
        if payload and broker:
            try:
                restored = broker.restore_session(payload)
                if isinstance(restored, dict) and restored.get("obj"):
                    self.broker_live_session = restored
                    print("[BROKER] Live session re-restored from payload for RMS")
                    return restored
                print("[BROKER] Re-restore from payload did not yield SmartConnect obj")
            except Exception as exc:
                print(f"[BROKER] Re-restore from payload failed: {type(exc).__name__}: {exc}")
        return None

    @staticmethod
    def _parse_broker_free_cash(balance_resp) -> Optional[float]:
        if not isinstance(balance_resp, dict) or balance_resp.get("status") != "success":
            return None

        if "free_cash" in balance_resp:
            try:
                free_cash = float(balance_resp["free_cash"])
                if free_cash >= 0:
                    return free_cash
            except (TypeError, ValueError):
                pass

        data = balance_resp.get("data")
        if isinstance(data, dict):
            for key in ("availablecash", "available_cash", "availableCash", "net", "cash"):
                if key in data:
                    try:
                        free_cash = float(data[key])
                        if free_cash >= 0:
                            return free_cash
                    except (TypeError, ValueError):
                        pass
        return None

    def _fetch_broker_free_cash(self, context: str = "BROKER") -> Optional[float]:
        """Fetch available cash from Angel RMS using the preserved live broker session."""
        # Resilience fix (error_fix_detail.md #5): reset the timeout flag on every
        # call so a stale True from a previous invocation can never leak forward.
        self._pyramid_cash_fetch_timed_out = False

        live = self._resolve_live_broker_session()
        broker = getattr(self, "broker", None)
        if not live:
            print(f"[{context}] No live broker session available for RMS (paper session only?)")
            return None
        if not broker:
            print(f"[{context}] Broker connector missing  cannot fetch RMS balance")
            return None

        try:
            balance_resp = broker.get_account_balance(live)
            broker_cash = self._parse_broker_free_cash(balance_resp)
            if broker_cash is not None:
                print(f"[{context}] broker free_cash (RMS): {broker_cash:,.2f}")
                return broker_cash
            error_text = (
                balance_resp.get("error") if isinstance(balance_resp, dict) else balance_resp
            )
            # get_account_balance() catches its own exceptions and returns them as
            # this error string, so a broker_angle.py timeout (error_fix_detail.md
            # #1) surfaces here as text rather than a raised TimeoutError.
            self._pyramid_cash_fetch_timed_out = "timed out" in str(error_text or "").lower()
            print(f"[{context}] broker balance unreadable: {error_text}")
        except Exception as exc:
            self._pyramid_cash_fetch_timed_out = isinstance(exc, TimeoutError)
            print(f"[{context}] broker balance fetch failed: {type(exc).__name__}: {exc}")
        return None

    def _get_pyramid_free_cash(self) -> Optional[float]:
        """Pyramid uses broker RMS cash, then session TOTP free_cash  never paper ledger."""
        broker_cash = self._fetch_broker_free_cash(context="PYRAMID")
        if broker_cash is not None:
            return broker_cash

        session_free = getattr(self, "session_free_cash", None)
        if session_free is not None:
            try:
                parsed = float(session_free)
                if parsed >= 0:
                    print(f"[PYRAMID] using session_free_cash fallback: {parsed:,.2f}")
                    return parsed
            except (TypeError, ValueError):
                pass

        print(
            "[PYRAMID] ABORT  no broker RMS cash and no session_free_cash; "
            "refusing paper cash_balance fallback"
        )
        return None

    @staticmethod
    def _build_pyramid_handoff_result(
        *,
        applied: bool,
        live_allowed: bool,
        reason: str,
        profitable_count: int = 0,
        symbols_for_live=None,
        removed_symbols=None,
    ) -> dict:
        """Build pyramid handoff payload. live_allowed=True only on successful pyramid apply."""
        return {
            "applied": applied,
            "live_allowed": live_allowed,
            "reason": reason,
            "profitable_count": profitable_count,
            "symbols_for_live": symbols_for_live or [],
            "removed_symbols": removed_symbols or [],
        }

    def _persist_pyramid_pnls(self, symbols: list, applied: bool = False, reason: str = None) -> None:
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        base = self.trading_logs_collection
        if not sid or base is None:
            return
        db_name = self._resolve_config_db_name()
        coll = (
            base.database.client[db_name]["pyramid_pnls"]
            if db_name != base.database.name
            else base.database["pyramid_pnls"]
        )
        try:
            coll.replace_one(
                {"session_id": str(sid)},
                {
                    "session_id": str(sid),
                    "configuration_id": getattr(self, "configuration_id", None),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "pyramid_applied": applied,
                    "reason": reason,
                    "symbols": symbols,
                },
                upsert=True,
            )
        except Exception as exc:
            print(f"[PYRAMID-PNL] persist failed: {exc}")

    def _finish_pyramid_handoff(self, result: dict, pnl_rows: list) -> dict:
        self._persist_pyramid_pnls(pnl_rows, applied=bool(result.get("applied")), reason=result.get("reason"))
        return result

    def _symbols_for_live_from_entries(self, updated_by_symbol: dict) -> list:
        rows = []
        allocations = getattr(self, "symbol_allocations", {}) or {}
        for sym_key, entry in updated_by_symbol.items():
            stop_loss = entry.get("stop_loss")
            if stop_loss is None:
                stop_loss = (allocations.get(sym_key) or {}).get("stop_loss", 0.05)
            rows.append(
                {
                    "symbol": sym_key,
                    "capital": float(entry.get("capital") or 0),
                    "stop_loss": float(stop_loss or 0.05),
                }
            )
        return [row for row in rows if row["symbol"] and row["capital"] > 0]

