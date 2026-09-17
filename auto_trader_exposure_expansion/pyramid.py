"""AutoTrader mixin: capital pyramid apply-on-stop logic. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
import time
from typing import Optional

from .constants import (
    PYR_PROFIT_THRESHOLD_PCT,
    pyramid_multiplier_for_rank,
    pyramid_profit_threshold_for_rank,
)


class PyramidMixin:

    # ===== PYRAMID CYCLE LOGS + AVG_PRICE PROFITABILITY (disabled) =====
#    def _resolve_pyramid_session_id(self) -> Optional[str]:
#        for attr in ("ui_session_id", "session_id"):
#            sid = getattr(self, attr, None)
#            if sid:
#                return str(sid).strip()
#        return None
    #
#    @staticmethod
#    def _normalize_trading_log_row(doc: dict) -> Optional[dict]:
#        if not isinstance(doc, dict):
#            return None
    #
#        sym = AutoTrader._normalize_config_symbol(doc.get("symbol"))
#        if not sym:
#            return None
    #
#        try:
#            cycle = int(doc.get("cycle"))
#        except (TypeError, ValueError):
#            return None
#        if cycle <= 0:
#            return None
    #
#        def _num(key, default=0.0, as_int=False):
#            raw = doc.get(key)
#            if raw is None:
#                return default
#            try:
#                return int(raw) if as_int else float(raw)
#            except (TypeError, ValueError):
#                return default
    #
#        ts = doc.get("timestamp")
#        if ts is not None and not isinstance(ts, str):
#            ts = str(ts)
    #
#        return {
#            "session_id": doc.get("session_id"),
#            "cycle": cycle,
#            "timestamp": ts or "",
#            "symbol": sym,
#            "curr_price": _num("curr_price"),
#            "return_pct": _num("return_pct"),
#            "side": (doc.get("side") or "NONE").upper(),
#            "signal": doc.get("signal") or "",
#            "action": (doc.get("action") or "").strip(),
#            "qty": _num("qty", default=0, as_int=True),
#            "unrealized_pnl": _num("unrealized_pnl"),
#            "symbol_unrealized_pnl": _num("symbol_unrealized_pnl"),
#            "symbol_realized_pnl": _num("symbol_realized_pnl"),
#            "symbol_pnl": _num("symbol_pnl"),
#            "cash_balance": _num("cash_balance"),
#            "realized_pnl": _num("realized_pnl"),
#            "total_equity": _num("total_equity"),
#            "_id": str(doc.get("_id")) if doc.get("_id") is not None else "",
#        }
    #
#    @staticmethod
#    def _trading_log_row_is_newer(candidate: dict, incumbent: dict) -> bool:
#        """True if candidate should replace incumbent for the same symbol+cycle."""
#        cand_ts = candidate.get("timestamp") or ""
#        inc_ts = incumbent.get("timestamp") or ""
#        if cand_ts != inc_ts:
#            return cand_ts > inc_ts
    #
#        cand_id = candidate.get("_id") or ""
#        inc_id = incumbent.get("_id") or ""
#        return cand_id > inc_id
    #
#    def _fetch_executed_symbol_cycle_logs(self, symbols=None) -> dict:
#        """
#        Load every trading_logs row for this session up to stop/pyramid time,
#        grouped by symbol. Includes cycle 1, 2, 3, ... however many ran.
    #
#        Returns:
#            {
#              "ok": bool,
#              "session_id": str | None,
#              "reason": str | None,
#              "cycles_executed": [1, 2, 3, ...],   # union across all symbols
#              "cycle_count": int,                    # len(cycles_executed)
#              "symbols": {
#                "AXISBANK": {
#                  "cycles": [1, 2, 3],               # cycle numbers ascending
#                  "all": [{cycle 1 row}, {cycle 2 row}, ...],  # ordered history
#                  "by_cycle": {1: {...}, 2: {...}, 3: {...}},
#                  "cycle_count": 3,
#                  "first_cycle": 1,
#                  "last_cycle": 3,
#                  "first": {...},
#                  "last": {...},
#                },
#              },
#              "row_count": int,
#            }
    #
#        Use `all` or `by_cycle` when you need every executed cycle (not just first/last).
#        If only one cycle ran, all == [first] == [last].
#        """
#        empty = {
#            "ok": False,
#            "session_id": None,
#            "reason": None,
#            "cycles_executed": [],
#            "cycle_count": 0,
#            "symbols": {},
#            "row_count": 0,
#        }
    #
#        coll = getattr(self, "trading_logs_collection", None)
#        session_id = self._resolve_pyramid_session_id()
#        if coll is None:
#            empty["reason"] = "trading_logs_unavailable"
#            print("[PYRAMID-LOGS] trading_logs_collection is None  cannot fetch cycles")
#            return empty
#        if not session_id:
#            empty["reason"] = "session_id_missing"
#            print("[PYRAMID-LOGS] session_id missing  cannot fetch cycles")
#            return empty
    #
#        requested_symbols = []
#        if symbols:
#            seen = set()
#            for sym in symbols:
#                sym_key = self._normalize_config_symbol(sym)
#                if sym_key and sym_key not in seen:
#                    seen.add(sym_key)
#                    requested_symbols.append(sym_key)
    #
#        result = {
#            "ok": True,
#            "session_id": session_id,
#            "reason": None,
#            "cycles_executed": [],
#            "cycle_count": 0,
#            "symbols": {},
#            "row_count": 0,
#        }
    #
#        if requested_symbols:
#            for sym_key in requested_symbols:
#                result["symbols"][sym_key] = {
#                    "cycles": [],
#                    "all": [],
#                    "by_cycle": {},
#                    "cycle_count": 0,
#                    "first_cycle": None,
#                    "last_cycle": None,
#                    "first": None,
#                    "last": None,
#                }
    #
#        by_symbol_cycle: dict = {}
#        global_cycles = set()
    #
#        try:
#            cursor = coll.find({"session_id": session_id}).sort([
#                ("cycle", 1),
#                ("timestamp", 1),
#                ("symbol", 1),
#            ])
#            for raw in cursor:
#                row = self._normalize_trading_log_row(raw)
#                if not row:
#                    continue
    #
#                sym_key = row["symbol"]
#                cycle = row["cycle"]
    #
#                if requested_symbols and sym_key not in result["symbols"]:
#                    continue
    #
#                result["row_count"] += 1
#                global_cycles.add(cycle)
    #
#                bucket = by_symbol_cycle.setdefault(sym_key, {})
#                existing = bucket.get(cycle)
#                if existing is None or self._trading_log_row_is_newer(row, existing):
#                    bucket[cycle] = row
    #
#        except Exception as exc:
#            print(f"[PYRAMID-LOGS] Mongo fetch failed for session={session_id}: {exc}")
#            empty["session_id"] = session_id
#            empty["reason"] = f"mongo_fetch_failed:{type(exc).__name__}"
#            return empty
    #
#        if not by_symbol_cycle and not requested_symbols:
#            print(f"[PYRAMID-LOGS] no trading_logs rows for session={session_id}")
#            return result
    #
#        symbol_keys = requested_symbols or sorted(by_symbol_cycle.keys())
#        for sym_key in symbol_keys:
#            cycle_map = by_symbol_cycle.get(sym_key, {})
#            cycles = sorted(cycle_map.keys())
#            by_cycle = {c: cycle_map[c] for c in cycles}
#            all_rows = [by_cycle[c] for c in cycles]
#            first_cycle = cycles[0] if cycles else None
#            last_cycle = cycles[-1] if cycles else None
    #
#            result["symbols"][sym_key] = {
#                "cycles": cycles,
#                "all": all_rows,
#                "by_cycle": by_cycle,
#                "cycle_count": len(cycles),
#                "first_cycle": first_cycle,
#                "last_cycle": last_cycle,
#                "first": all_rows[0] if all_rows else None,
#                "last": all_rows[-1] if all_rows else None,
#            }
    #
#        result["cycles_executed"] = sorted(global_cycles)
#        result["cycle_count"] = len(result["cycles_executed"])
    #
#        print(
#            f"[PYRAMID-LOGS] session={session_id} rows={result['row_count']} "
#            f"cycles={result['cycles_executed']} (count={result['cycle_count']}) "
#            f"symbols={list(result['symbols'].keys())}"
#        )
#        for sym_key, sym_data in result["symbols"].items():
#            cycle_summary = [
#                {
#                    "cycle": row.get("cycle"),
#                    "timestamp": row.get("timestamp"),
#                    "action": row.get("action"),
#                    "qty": row.get("qty"),
#                    "curr_price": row.get("curr_price"),
#                }
#                for row in sym_data.get("all") or []
#            ]
#            print(f"[PYRAMID-LOGS]   {sym_key} ({sym_data.get('cycle_count', 0)} cycles): {cycle_summary}")
    #
#        return result
    #
#    @staticmethod
#    def _classify_pyramid_log_action(action: str) -> str:
#        text = (action or "").strip().upper()
#        if not text:
#            return "UNKNOWN"
#        if "WAIT" in text and "NO POSITION" in text:
#            return "WAIT"
#        if "PENDING" in text and "ORDER" in text:
#            return "PENDING"
#        if "HOLD" in text and "CONTINUE" in text:
#            return "HOLD"
#        if "HOLD" in text and "FLAT" in text:
#            return "WAIT"
#        if "STOP-LOSS" in text or "CLOSED" in text or "COVERED" in text:
#            return "EXIT"
#        if "OPEN LONG" in text or "OPEN SHORT" in text:
#            return "PENDING"
#        return "OTHER"
    #
#    @staticmethod
#    def _is_pyramid_flip_between_cycles(c1: dict, c2: dict) -> bool:
#        sig2 = (c2.get("signal") or "").upper()
#        act2 = (c2.get("action") or "").upper()
#        if "FLIP" in sig2 or "FLIP" in act2:
#            return True
    #
#        s1 = (c1.get("side") or "NONE").upper()
#        s2 = (c2.get("side") or "NONE").upper()
#        if s1 in ("BUY", "SELL") and s2 in ("BUY", "SELL") and s1 != s2:
#            return True
    #
#        if s1 == "BUY" and ("FLIP TO SHORT" in sig2 or ("SELL" in sig2 and "FLIP" in sig2)):
#            return True
#        if s1 == "SELL" and ("FLIP TO LONG" in sig2 or ("BUY" in sig2 and "FLIP" in sig2)):
#            return True
    #
#        q1 = int(c1.get("qty") or 0)
#        q2 = int(c2.get("qty") or 0)
#        if q1 > 0 and q2 >= max(q1 * 2 - 1, q1 + 1):
#            sig1 = (c1.get("signal") or "").upper()
#            if s1 == "BUY" and "SELL" in sig2:
#                return True
#            if s1 == "SELL" and "BUY" in sig2:
#                return True
#            if "FLIP" in sig2:
#                return True
#            if s1 != s2:
#                return True
#            _ = sig1
#        return False
    #
#    @staticmethod
#    def _is_pyramid_expansion_between_cycles(c1: dict, c2: dict) -> bool:
#        sig2 = (c2.get("signal") or "").upper()
#        if "EXPANSION" in sig2 or "EXPAND" in sig2:
#            return True
    #
#        s1 = (c1.get("side") or "NONE").upper()
#        s2 = (c2.get("side") or "NONE").upper()
#        q1 = int(c1.get("qty") or 0)
#        q2 = int(c2.get("qty") or 0)
    #
#        if s1 in ("BUY", "SELL") and s1 == s2 and q2 > q1 > 0:
#            return True
#        if q2 > q1 > 0 and s2 in ("BUY", "SELL") and s2 == s1:
#            return True
    #
#        sig1 = (c1.get("signal") or "").upper()
#        if q2 > q1 > 0:
#            if "BUY" in sig1 and "BUY" in sig2 and ("EXPAND" in sig2 or "EXPANSION" in sig2):
#                return True
#            if "SELL" in sig1 and "SELL" in sig2 and ("EXPAND" in sig2 or "EXPANSION" in sig2):
#                return True
#        return False
    #
#    def _compute_pyramid_avg_price(self, c1: Optional[dict], c2: Optional[dict], sym_key: str) -> tuple:
#        if not c1 and not c2:
#            return 0.0, "no_logs"
#        if c1 and not c2:
#            c2 = c1
#        if not c1 and c2:
#            c1 = c2
    #
#        b1 = self._classify_pyramid_log_action(c1.get("action"))
#        b2 = self._classify_pyramid_log_action(c2.get("action"))
#        p1 = float(c1.get("curr_price") or 0)
#        p2 = float(c2.get("curr_price") or 0)
    #
#        if b1 == "WAIT" and b2 == "WAIT":
#            return 0.0, "wait+wait"
#        if b1 == "WAIT" and b2 == "PENDING":
#            return p2, "wait+pending"
#        if b1 == "WAIT" and b2 == "HOLD":
#            return p2, "wait+hold"
#        if b1 == "PENDING" and b2 == "HOLD":
#            return p2, "pending+hold"
#        if b1 == "PENDING" and b2 == "PENDING":
#            if self._is_pyramid_expansion_between_cycles(c1, c2):
#                if p1 > 0 and p2 > 0:
#                    return (p1 + p2) / 2.0, "pending+pending+expand"
#                return p2, "pending+pending+expand"
#            if self._is_pyramid_flip_between_cycles(c1, c2):
#                return p2, "pending+pending+flip"
#            return p2, "pending+pending"
#        if b2 in ("WAIT", "EXIT"):
#            return 0.0, f"{b1.lower()}+{b2.lower()}"
#        if b1 == "HOLD" and b2 == "HOLD":
#            with self.positions_lock:
#                pos = self.positions.get(sym_key)
#            if pos:
#                entry_price = float(pos.get("entry_price") or 0)
#                if entry_price > 0:
#                    return entry_price, "hold+hold+entry"
#            if p1 > 0:
#                return p1, "hold+hold+c1"
#            return p2, "hold+hold+c2"
#        if b1 == "HOLD" and b2 == "PENDING":
#            if self._is_pyramid_expansion_between_cycles(c1, c2):
#                if p1 > 0 and p2 > 0:
#                    return (p1 + p2) / 2.0, "hold+pending+expand"
#            if self._is_pyramid_flip_between_cycles(c1, c2):
#                return p2, "hold+pending+flip"
#            return p2, "hold+pending"
#        if b2 in ("HOLD", "PENDING") and p2 > 0:
#            return p2, f"{b1.lower()}+{b2.lower()}"
#        if p1 > 0:
#            return p1, "fallback_c1"
#        return 0.0, "fallback_zero"
    #
#    def _resolve_pyramid_position_qty(self, sym_key: str, last_row: Optional[dict]) -> int:
#        bucket = self._classify_pyramid_log_action((last_row or {}).get("action"))
#        if bucket in ("WAIT", "EXIT"):
#            return 0
    #
#        log_qty = int((last_row or {}).get("qty") or 0)
#        if log_qty > 0:
#            return log_qty
#        return self._get_symbol_position_qty(sym_key)
    #
#    def _resolve_pyramid_position_side(self, sym_key: str, last_row: Optional[dict]) -> str:
#        side = ((last_row or {}).get("side") or "NONE").upper()
#        if side in ("BUY", "SELL"):
#            return side
    #
#        with self.positions_lock:
#            pos = self.positions.get(sym_key)
#            if pos:
#                pos_side = (pos.get("side") or "NONE").upper()
#                if pos_side in ("BUY", "SELL"):
#                    return pos_side
    #
#        with self._paper_lock:
#            pos = self._paper_positions.get(sym_key)
#            if pos:
#                pos_side = (pos.get("side") or "NONE").upper()
#                if pos_side in ("BUY", "SELL"):
#                    return pos_side
#        return "NONE"
    #
#    @staticmethod
#    def _compute_pyramid_unrealized_pnl(avg_price: float, qty: int, side: str, mark_price: float) -> float:
#        if avg_price <= 0 or qty <= 0 or mark_price <= 0:
#            return 0.0
#        if side == "BUY":
#            return round((mark_price - avg_price) * qty, 2)
#        if side == "SELL":
#            return round((avg_price - mark_price) * qty, 2)
#        return 0.0
    #
#    def _evaluate_pyramid_symbol_profitability(self, sym_key: str, sym_log_data: dict) -> dict:
#        c_first = sym_log_data.get("first")
#        c_last = sym_log_data.get("last")
    #
#        avg_price, case = self._compute_pyramid_avg_price(c_first, c_last, sym_key)
#        qty = self._resolve_pyramid_position_qty(sym_key, c_last)
#        side = self._resolve_pyramid_position_side(sym_key, c_last)
#        mark_price = float((c_last or {}).get("curr_price") or 0)
    #
#        capital_allocated = avg_price * qty if avg_price > 0 and qty > 0 else 0.0
#        threshold = capital_allocated * PYRAMID_PROFIT_THRESHOLD_PCT
#        unrealized_pnl = self._compute_pyramid_unrealized_pnl(avg_price, qty, side, mark_price)
    #
#        if unrealized_pnl == 0 and c_last:
#            bucket = self._classify_pyramid_log_action(c_last.get("action"))
#            if bucket in ("HOLD", "PENDING"):
#                log_unrealized = float(
#                    c_last.get("symbol_unrealized_pnl")
#                    or c_last.get("unrealized_pnl")
#                    or 0
#                )
#                if log_unrealized != 0:
#                    unrealized_pnl = log_unrealized
    #
#        is_profitable = capital_allocated > 0 and unrealized_pnl > threshold
#        return {
#            "avg_price": avg_price,
#            "qty": qty,
#            "side": side,
#            "mark_price": mark_price,
#            "capital_allocated": capital_allocated,
#            "threshold": threshold,
#            "unrealized_pnl": unrealized_pnl,
#            "is_profitable": is_profitable,
#            "case": case,
#            "first_cycle": (c_first or {}).get("cycle"),
#            "last_cycle": (c_last or {}).get("cycle"),
#            "first_action": (c_first or {}).get("action"),
#            "last_action": (c_last or {}).get("action"),
#        }
    #
    # ===== END PYRAMID CYCLE LOGS (disabled) =====
    def _apply_capital_pyramid_on_stop(self) -> dict:
        if getattr(self, "_pyramid_applied", False):
            print("[PYRAMID] Already applied  skipping duplicate stop")
            cached = getattr(self, "_pyramid_handoff_result", None) or getattr(
                self, "_pyramid_handoff_result_cache", None
            )
            if cached:
                return cached
            return self._build_pyramid_handoff_result(
                applied=True,
                live_allowed=False,
                reason="already_applied_no_handoff",
            )

        configuration_id = getattr(self, "configuration_id", None)
        if not configuration_id:
            print("[PYRAMID] configuration_id missing  skipping capital pyramid update")
            return self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="configuration_id_missing",
            )

        config_doc = self._fetch_saved_trading_configuration(configuration_id)
        if not config_doc:
            return self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="configuration_not_found",
            )

        symbols_list, set_path = self._get_configuration_symbols_ref(config_doc)
        if symbols_list is None or set_path is None:
            print(f"[PYRAMID] configuration symbols not found for {configuration_id}")
            return self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="configuration_symbols_missing",
            )

        rank_map = self._build_configuration_rank_map(config_doc)
        config_entries = self._build_pyramid_config_entries(config_doc, symbols_list, rank_map)
        if not config_entries:
            print("[PYRAMID] No symbols with capital to evaluate  aborting update")
            return self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="no_symbols_to_evaluate",
            )

        # pyramid_symbol_keys = [sym_key for sym_key, _, _ in config_entries]
        # self._pyramid_cycle_logs = self._fetch_executed_symbol_cycle_logs(symbols=pyramid_symbol_keys)

        active_symbols = self._get_pyramid_active_symbol_keys()
        total_capital_allocated = sum(cap for _, _, cap in config_entries)
        free_cash = self._get_pyramid_free_cash()
        if free_cash is None:
            print("[PYRAMID] Skipping capital pyramid  real broker/session cash unavailable")
            # Resilience fix (error_fix_detail.md #5): a bounded broker-call timeout
            # (error_fix_detail.md #1) gets its own reason so it's distinguishable
            # from a genuine "no cash data" result in logs/DB - the outcome
            # (live_allowed=False) is identical either way, only the label differs.
            timed_out = getattr(self, "_pyramid_cash_fetch_timed_out", False)
            return self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="broker_cash_timeout" if timed_out else "broker_cash_unavailable",
            )

        raw_free_cash = float(free_cash)
        leverage_mult = self._resolve_leverage_multiplier(config_doc=config_doc, default=1.0)
        free_cash = raw_free_cash * leverage_mult
        # Distribute the full leveraged cash to pyramided names (do not subtract morning allocations).
        remaining_cash = free_cash
        print(
            f"[PYRAMID] raw_free_cash={raw_free_cash:.2f} "
            f"leverage=x{leverage_mult} "
            f"effective_free_cash={free_cash:.2f} "
            f"total_capital_allocated={total_capital_allocated:.2f} "
            f"remaining_cash={remaining_cash:.2f} "
            f"evaluated_symbols={[sym for sym, _, _ in config_entries]}"
        )

        profitable = []
        removed = []
        removed_keys = set()
        pnl_rows = []
        for sym_key, entry, cap in config_entries:
            exit_doc = self._get_exited_symbol_doc(sym_key)
            is_exited = bool(exit_doc)
            if exit_doc:
                realized = round(float(exit_doc.get("realized_pnl") or 0.0), 2)
            else:
                realized = round(float(self.realized_pnl_by_symbol.get(sym_key, 0.0) or 0.0), 2)
            unrealized = 0.0 if is_exited else self._get_symbol_unrealized_pnl(sym_key)
            total_pnl = round(realized + float(unrealized or 0.0), 2)
            position_status = "EXITED" if is_exited else "OPEN"
            exit_reason = (exit_doc or {}).get("exit_reason")
            rank = entry.get("rank")
            if rank is None:
                rank = rank_map.get(sym_key)
            threshold = pyramid_profit_threshold_for_rank(rank, cap)
            row = {
                "symbol": sym_key,
                "rank": rank,
                "capital_allocated": round(cap, 2),
                "position_status": position_status,
                "exit_reason": exit_reason,
                "realized_pnl": realized,
                "unrealized_pnl": round(unrealized, 2),
                "total_pnl": total_pnl,
                "profit_threshold": round(threshold, 2) if threshold is not None else None,
                "is_profitable": False,
            }
            if is_exited:
                print(
                    f"[PYRAMID-PROFIT] {sym_key}: already exited "
                    f"reason={exit_reason} realized={realized:.2f} unrealized=0.00"
                )
                removed.append(
                    f"{sym_key}(status=EXITED, reason={exit_reason}, realized={realized:.2f})"
                )
                removed_keys.add(sym_key)
                pnl_rows.append(row)
                continue

            if threshold is None:
                removed.append(f"{sym_key}(rank={rank!r}, missing threshold)")
                removed_keys.add(sym_key)
                pnl_rows.append(row)
                continue
            pct = PYR_PROFIT_THRESHOLD_PCT.get(int(rank), PYR_PROFIT_THRESHOLD_PCT[10])
            print(
                f"[PYRAMID-PROFIT] {sym_key}: rank={rank} capital={cap:.2f} "
                f"pct={pct}% threshold={threshold:.2f} unrealized={unrealized:.2f}"
            )
            row["is_profitable"] = unrealized > threshold
            pnl_rows.append(row)
            if unrealized > threshold:
                profitable.append((sym_key, entry, cap, unrealized))
            else:
                removed.append(
                    f"{sym_key}(rank={rank}, unrealized={unrealized:.2f}, threshold={threshold:.2f})"
                )
                removed_keys.add(sym_key)

        if removed:
            print(f"[PYRAMID] Removing non-profitable symbols: {', '.join(removed)}")

        if not profitable:
            print(
                "[PYRAMID] No symbols passed rank-based profit threshold  "
                "skipping Mongo update (master config preserved)"
            )
            return self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=False,
                    live_allowed=False,
                    reason="no_profitable_symbols",
                    removed_symbols=[item.split("(")[0] for item in removed],
                ),
                pnl_rows,
            )

        # --- pyramid cycle-log profitability (disabled) ---
        # profitable = []
        # removed = []
        # removed_keys = set()
        # for sym_key, entry, cap in config_entries:
        #     sym_log_data = self._pyramid_cycle_logs.get("symbols", {}).get(sym_key, {})
        #     metrics = self._evaluate_pyramid_symbol_profitability(sym_key, sym_log_data)
        #     print(
        #         f"[PYRAMID-CASE] {sym_key}: case={metrics['case']} "
        #         f"cycles={metrics['first_cycle']}->{metrics['last_cycle']} "
        #         f"actions=({metrics['first_action']!r} -> {metrics['last_action']!r}) "
        #         f"avg={metrics['avg_price']:.2f} qty={metrics['qty']} side={metrics['side']} "
        #         f"mark={metrics['mark_price']:.2f} capital={metrics['capital_allocated']:.2f} "
        #         f"unrealized={metrics['unrealized_pnl']:.2f} "
        #         f"threshold={metrics['threshold']:.4f} ({PYRAMID_PROFIT_THRESHOLD_PCT * 100:.3f}%) "
        #         f"profitable={metrics['is_profitable']}"
        #     )
        #     if metrics["is_profitable"]:
        #         profitable.append((sym_key, entry, cap, metrics["unrealized_pnl"]))
        #     else:
        #         removed.append(
        #             f"{sym_key}(case={metrics['case']}, unrealized={metrics['unrealized_pnl']:.2f}, "
        #             f"threshold={metrics['threshold']:.4f})"
        #         )
        #         removed_keys.add(sym_key)
        #
        # if removed:
        #     print(f"[PYRAMID] Removing non-profitable symbols: {', '.join(removed)}")
        #
        # if not profitable:
        #     print(
        #         f"[PYRAMID] No symbols with unrealized_pnl > "
        #         f"{PYRAMID_PROFIT_THRESHOLD_PCT * 100:.3f}% of allocated capital  "
        #         "skipping Mongo update (master config preserved)"
        #     )

        profitable_static = []
        for sym_key, entry, current_capital, unrealized in profitable:
            rank = entry.get("rank")
            if rank is None:
                rank = rank_map.get(sym_key)
            static_multiplier = pyramid_multiplier_for_rank(rank)
            if static_multiplier is None:
                print(
                    f"[PYRAMID] {sym_key}: missing/invalid rank={rank!r} "
                    f"(entry_rank={entry.get('rank')!r}, map_rank={rank_map.get(sym_key)!r})  skipped"
                )
                continue
            capital_after_static = current_capital * static_multiplier
            profitable_static.append(
                (sym_key, entry, current_capital, unrealized, static_multiplier, capital_after_static, rank)
            )

        if not profitable_static:
            print("[PYRAMID] No profitable symbols with valid rank  aborting update")
            return self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=False,
                    live_allowed=False,
                    reason="no_profitable_with_valid_rank",
                    removed_symbols=[item.split("(")[0] for item in removed],
                ),
                pnl_rows,
            )

        capital_after_static_sum = sum(item[5] for item in profitable_static)
        if capital_after_static_sum <= 0:
            print("[PYRAMID] Sum of capital after static multiplier <= 0  aborting")
            return self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=False,
                    live_allowed=False,
                    reason="capital_after_static_non_positive",
                ),
                pnl_rows,
            )

        dynamic_multiplier = remaining_cash / capital_after_static_sum
        print(
            f"[PYRAMID] capital_after_static_sum={capital_after_static_sum:.2f} "
            f"dynamic_multiplier={dynamic_multiplier:.4f}"
        )
        if dynamic_multiplier <= 0:
            print(f"[PYRAMID] dynamic_multiplier <= 0 ({dynamic_multiplier:.4f})  aborting update")
            return self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=False,
                    live_allowed=False,
                    reason="dynamic_multiplier_non_positive",
                ),
                pnl_rows,
            )

        updated_by_symbol = {}
        for (
            sym_key,
            entry,
            current_capital,
            unrealized,
            static_multiplier,
            capital_after_static,
            rank,
        ) in profitable_static:
            new_capital = round(dynamic_multiplier * capital_after_static, 2)
            new_entry = dict(entry)
            new_entry["capital"] = new_capital
            new_entry["rank"] = rank
            updated_by_symbol[sym_key] = new_entry
            print(
                f"[PYRAMID] {sym_key}: rank={rank} capital "
                f"{current_capital:.2f} -> {new_capital:.2f} "
                f"(after_static={capital_after_static:.2f}, static=x{static_multiplier}, "
                f"dynamic=x{dynamic_multiplier:.4f}, "
                f"combined=x{static_multiplier * dynamic_multiplier:.4f}, "
                f"unrealized={unrealized:.2f})"
            )

        if not updated_by_symbol:
            print("[PYRAMID] No profitable symbols with valid rank  aborting update")
            return self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=False,
                    live_allowed=False,
                    reason="no_symbols_updated",
                ),
                pnl_rows,
            )

        saved = self._persist_pyramid_merged_updates(
            config_doc=config_doc,
            updated_by_symbol=updated_by_symbol,
            removed_active_symbols=removed_keys,
            active_symbols=active_symbols,
            updated_count=len(updated_by_symbol),
            configuration_id=configuration_id,
        )
        symbols_for_live = self._symbols_for_live_from_entries(updated_by_symbol)
        if saved:
            self._pyramid_applied = True
            result = self._finish_pyramid_handoff(
                self._build_pyramid_handoff_result(
                    applied=True,
                    live_allowed=bool(symbols_for_live),
                    reason="ok" if symbols_for_live else "no_symbols_for_live",
                    profitable_count=len(symbols_for_live),
                    symbols_for_live=symbols_for_live,
                    removed_symbols=sorted(removed_keys),
                ),
                pnl_rows,
            )
            self._pyramid_handoff_result_cache = result
            return result

        return self._finish_pyramid_handoff(
            self._build_pyramid_handoff_result(
                applied=False,
                live_allowed=False,
                reason="mongo_save_failed",
                profitable_count=len(symbols_for_live),
                symbols_for_live=symbols_for_live,
            ),
            pnl_rows,
        )

