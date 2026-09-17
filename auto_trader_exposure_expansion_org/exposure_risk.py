import json
import os
import threading
import time
from datetime import datetime
from typing import Optional

from .constants import MARKET_TZ


class ExposureRiskMixin:
    # ---------- TIME HELPERS ----------
    
    def _now_market_time(self):
        return datetime.now(MARKET_TZ)

    # ---------- SYMBOL LOCK (thread-safe exposure updates) -----------

    def _get_symbol_lock(self, symbol):
        t0 = time.time()

        if symbol not in self.symbol_locks:
            self.symbol_locks.setdefault(symbol, threading.Lock())

        elapsed = time.time() - t0

        print(f"[TRACE] SYMBOL_LOCK {symbol}: {elapsed:.6f}s")
        self.tlog.record("symbol lock timing ", t0, note=symbol)
        return self.symbol_locks[symbol]

    # ---------- RESERVED (PENDING) EXPOSURE READ --------------
    
    def _reserved_exposure(self, symbol):
        return self.reserved_exposure.get(symbol, 0.0)

    def _coerce_leverage_multiplier(value, source: str = "") -> Optional[float]:
        if value is None:
            return None
        try:
            mult = float(value)
            if mult > 0:
                return mult
            print(f"[LEVERAGE] Ignoring non-positive {source}: {value!r}")
        except (TypeError, ValueError):
            print(f"[LEVERAGE] Invalid {source}: {value!r}")
        return None

    def _normalize_configuration_root(self, config_doc: dict) -> dict:
        root = config_doc.get("configuration", config_doc)
        if isinstance(root, str):
            try:
                root = json.loads(root)
            except Exception as exc:
                print(f"[LEVERAGE] configuration JSON parse failed: {exc}")
                return {}
        if not isinstance(root, dict):
            return {}
        return root

    def _leverage_from_config_doc(self, config_doc: dict) -> Optional[float]:
        if not isinstance(config_doc, dict):
            return None
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(
                config_doc.get(key), f"SavedTradingConfiguration.{key}"
            )
            if val is not None:
                return val
        root = self._normalize_configuration_root(config_doc)
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(root.get(key), f"configuration.{key}")
            if val is not None:
                return val
        return None

    def _fetch_saved_trading_configuration_for_leverage(self) -> Optional[dict]:
        configuration_id = getattr(self, "configuration_id", None)
        coll = self.trading_logs_collection
        if not configuration_id or coll is None:
            return None

        config_db_name = getattr(self, "config_db_name", None) or os.environ.get(
            "MONGO_CONFIG_DB_NAME", "test"
        )
        collection_names = (
            "savedtradingconfigurations",
            "SavedTradingConfiguration",
        )
        mongo_client = coll.database.client
        config_db = mongo_client[config_db_name]

        queries = [{"configuration_id": configuration_id}]
        try:
            from bson import ObjectId

            if ObjectId.is_valid(configuration_id):
                oid = ObjectId(configuration_id)
                queries.insert(0, {"_id": oid})
                queries.append({"configuration_id": str(oid)})
        except Exception:
            pass

        for coll_name in collection_names:
            if coll_name not in config_db.list_collection_names():
                continue
            config_coll = config_db[coll_name]
            for query in queries:
                try:
                    doc = config_coll.find_one(query)
                except Exception as exc:
                    print(f"[LEVERAGE] SavedTradingConfiguration lookup failed: {exc}")
                    continue
                if doc:
                    return doc
        return None

    def _resolve_leverage_multiplier(self, config_doc=None, default: float = 4.0) -> float:
        """
        Resolve intraday leverage: session/request -> SavedTradingConfiguration -> default.
        Live exposure cap defaults to 4.0 (legacy behavior) when unset.
        """
        val = self._coerce_leverage_multiplier(
            getattr(self, "leverage_multiplier", None), "session"
        )
        if val is not None:
            self.leverage_multiplier = val
            return val

        doc = config_doc
        if doc is None:
            doc = self._fetch_saved_trading_configuration_for_leverage()

        if doc:
            val = self._leverage_from_config_doc(doc)
            if val is not None:
                self.leverage_multiplier = val
                print(f"[LEVERAGE] Using leverage_multiplier={val} from SavedTradingConfiguration")
                return val

        self.leverage_multiplier = float(default)
        print(f"[LEVERAGE] Falling back to leverage_multiplier={default}")
        return float(default)

    # ---------- TOTAL SYMBOL EXPOSURE (FILLED + RESERVED) -------------
    
    def _total_symbol_exposure(self, symbol):
        return self._stock_exposure(symbol) + self._reserved_exposure(symbol)

    # ---------- EXPOSURE CAP CHECK (ATOMIC) ----------------

    def _can_reserve_exposure(self, symbol, order_value):
        t0 = time.time()
        leverage_mult = self._resolve_leverage_multiplier(default=4.0)
        leveraged_capital = leverage_mult * self.initial_capital

        result = (self._total_symbol_exposure(symbol) + order_value) <= (self.max_exposure_pct * leveraged_capital)

        elapsed = time.time() - t0

        print(f"[TRACE] CAN_RESERVE {symbol}: {elapsed:.6f}s")

        self.tlog.record("CAN_RESERVE_EXPOSURE", t0, note=symbol)

        return result

    # ---------- RESERVE EXPOSURE (BEFORE ORDER SUBMIT) -------------

    def _reserve_exposure(self, symbol, order_value):
        t0 = time.time()

        self.reserved_exposure[symbol] = (
            self.reserved_exposure.get(symbol, 0.0) + order_value
        )

        elapsed = time.time() - t0

        print(f"[TRACE] RESERVE_EXPOSURE {symbol}: {elapsed:.6f}s")

        self.tlog.record("RESERVE_EXPOSURE", t0, note=symbol)

    # ---------- RELEASE RESERVED EXPOSURE (AFTER RESULT) --------------
    
    def _release_exposure(self, symbol, order_value):
        if symbol in self.reserved_exposure:
            self.reserved_exposure[symbol] -= order_value
            if self.reserved_exposure[symbol] <= 0:
                self.reserved_exposure.pop(symbol, None)

    # ---------- STOCK EXPOSURE ------------
    
    def _stock_exposure(self, symbol):
        with self.broker_pos_lock:
            broker_positions = list(self._broker_positions_cache or [])

        for p in broker_positions:
            if p["symbol"] == symbol:
                qty = abs(p.get("qty", 0))
                avg = p.get("avg_price", 0.0)

                if qty <= 0 or avg <= 0:
                    return 0.0

                return qty * avg

        return 0.0

    # ---------- CASH / BALANCE ----------

    def _get_free_cash(self):
        try:
            bal = self.broker.get_account_balance(self.session)
        except Exception as e:
            self.alerts.notify(f"Failed to fetch account balance: {e}")
            return None

        if not isinstance(bal, dict) or bal.get("status") != "success":
            self.alerts.notify(
                f"Could not read account balance: {bal.get('error') if isinstance(bal, dict) else bal}"
            )
            return None

        if "free_cash" in bal:
            try:
                free_cash = float(bal["free_cash"])
                if free_cash >= 0:
                    print(f"[INFO] Free cash detected: {free_cash:,.2f}")
                    return free_cash
            except Exception:
                pass

        if "data" in bal:
            try:
                data = bal["data"]
                if isinstance(data, dict):
                    for key in ["availablecash", "available_cash", "availableCash", "net", "cash"]:
                        if key in data:
                            try:
                                free_cash = float(data[key])
                                if free_cash >= 0:
                                    print(f"[INFO] Free cash from data.{key}: {free_cash:,.2f}")
                                    return free_cash
                            except Exception:
                                pass
            except Exception:
                pass

        raw = bal.get("raw")
        free_cash = None

        try:
            if isinstance(raw, dict):
                candidate = raw.get("data") if "data" in raw else raw
                for key in (
                    "available_cash", "availableCash", "available_balance", "availableBalance",
                    "cash", "equity", "netEquity", "availableMargin", "available_margin",
                    "availablecash", "net"
                ):
                    if isinstance(candidate, dict) and key in candidate:
                        try:
                            free_cash = float(candidate[key])
                            if free_cash >= 0:
                                print(f"[INFO] Free cash from raw.{key}: {free_cash:,.2f}")
                                return free_cash
                        except Exception:
                            try:
                                free_cash = float(str(candidate[key]).replace(",", ""))
                                if free_cash >= 0:
                                    print(f"[INFO] Free cash from raw.{key} (parsed): {free_cash:,.2f}")
                                    return free_cash
                            except Exception:
                                pass

                if free_cash is None:
                    for k, v in (candidate.items() if isinstance(candidate, dict) else []):
                        try:
                            if isinstance(v, (int, float)) and v >= 0:
                                if (
                                    "available" in k.lower() or
                                    "free" in k.lower() or
                                    "cash" in k.lower() or
                                    "net" in k.lower()
                                ):
                                    free_cash = float(v)
                                    print(f"[INFO] Free cash from scanning {k}: {free_cash:,.2f}")
                                    return free_cash
                        except Exception:
                            continue
        except Exception as e:
            print(f"[WARN] Error parsing raw response: {e}")
            free_cash = None

        print(f"[ERROR] Could not extract free cash from response. Available keys: {list(bal.keys())}")
        return free_cash

    def _sync_cash_with_broker(self):
        print("[SYNC] Syncing cash balance with broker...")
        free_cash = self._get_free_cash()
        if free_cash is not None:
            old_balance = self.cash_balance
            self.cash_balance = free_cash
            print(f"[SYNC]  Cash balance updated: {old_balance:,.2f} -> {free_cash:,.2f}")
            self.alerts.notify(f"Cash synced with broker: {free_cash:,.2f}")
            return True
        else:
            print("[SYNC] Failed to sync cash balance")
            self.alerts.notify("Warning: Could not sync cash balance with broker")
            return False
