from broker_angle import BrokerConnector

from .order_execution import ParallelOrderExecutor


class SessionBrokerMixin:
    # ---------- BROKER / SESSION ----------

    def _link_broker(self, force_relink=False):
        if not force_relink and getattr(self, "broker", None) and getattr(self, "session", None):
            return

        if getattr(self, "broker", None) is None or force_relink:
            self.broker = BrokerConnector()

        try:
            self.session = self.broker.get_session()
        except Exception as e:
            self.session = None
            raise

    def _ensure_session(self):
        """Ensure self.session is present and valid; attempt relink if missing."""
        if getattr(self, "session", None) and isinstance(self.session, dict) and "obj" in self.session:
            return True
        try:
            self._link_broker(force_relink=True)
            return True
        except Exception as e:
            self.alerts.notify(f"Failed to (re)link broker session: {e}")
            return False

    # ==================== PARALLEL EXECUTOR HELPER ====================
    def _ensure_parallel_executor(self):
        """Ensure parallel executor is initialized with current session"""
        if self.parallel_executor is None and hasattr(self, 'session') and self.session:
            self.parallel_executor = ParallelOrderExecutor(
                broker=self.broker,
                session=self.session,
                max_workers=5,
                order_rate_limit=10,
                order_rate_window=1.0,
                status_rate_limit=20,
                status_rate_window=1.0
            )
            self.parallel_executor.start()
            print("[INFO] Parallel order executor initialized with 4 workers")
            print("[INFO]  Rate limits: 8 orders/sec, 18 status checks/sec")
    # ==================================================================

    def _get_broker_positions(self):
        try:
            resp = self.broker.get_positions(self.session)

            if not isinstance(resp, dict) or not resp.get("status"):
                return []

            raw = resp.get("raw")
            if not isinstance(raw, dict):
                return []

            data = raw.get("data")
            if not isinstance(data, list):
                return []

            positions = []

            for p in data:
                try:
                    # Exit path must never square off CNC/DELIVERY/MIS/carry positions
                    product = str(p.get("producttype") or p.get("productType") or "").upper()
                    if product != "INTRADAY":
                        continue

                    net_qty = int(p.get("netqty", 0))
                    if net_qty == 0:
                        continue

                    symbol = (
                        p.get("tradingsymbol", "")
                        .replace("-EQ", "")
                        .upper()
                    )

                    side = "BUY" if net_qty > 0 else "SELL"

                    positions.append({
                        "symbol": symbol,
                        "side": side,
                        "qty": abs(net_qty),
                        "product_type": product or "INTRADAY",
                        "avg_price": float(
                            p.get("averageprice")
                            or p.get("avg_price")
                            or 0.0
                        ),
                        "ltp": float(
                            p.get("ltp")
                            or p.get("lastprice")
                            or p.get("last_price")
                            or 0.0
                        )
                    })

                except Exception:
                    continue

            return positions

        except Exception as e:
            self.alerts.notify(f"Broker positions fetch failed: {e}")
            return []

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
