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
    
    def _seed_realized_pnl_from_broker(self):
        """
        On a fresh session start, pull today's already-realized PnL from the
        broker so the RMS layers don't believe the day starts at zero.

        Why: self.realized_pnl and self.realized_pnl_by_symbol are in-memory
        only  every new AutoTrader instance resets them. If a user stops and
        restarts mid-day, the previous day's closed-trade losses/profits are
        invisible to RMS unless we re-seed from the broker's position book.

        Source of truth:
          - Per-symbol: position book entries (open AND closed positions both
            carry a 'realised' field with today's realized PnL for that name).
          - Total cross-check: rmsLimit -> data.m2mrealized.
        """
        seeded_total = 0.0
        seeded_by_symbol: dict = {}

        try:
            resp = self.broker.get_positions(self.session)
            if (isinstance(resp, dict) and resp.get("status")
                    and isinstance(resp.get("raw"), dict)):
                data = resp["raw"].get("data") or []
                if isinstance(data, list):
                    for p in data:
                        try:
                            sym = (p.get("tradingsymbol") or "").replace("-EQ", "").upper()
                            if not sym:
                                continue
                            # Angel returns 'realised' (sometimes 'realized' or 'pnl')
                            r = (p.get("realised")
                                 or p.get("realized")
                                 or p.get("pnl")
                                 or 0.0)
                            r = float(r or 0.0)
                            if r != 0.0:
                                seeded_by_symbol[sym] = seeded_by_symbol.get(sym, 0.0) + r
                                seeded_total += r
                        except Exception as inner:
                            print(f"[SEED-REALIZED] skip row: {inner}")
                            continue
        except Exception as e:
            print(f"[SEED-REALIZED] get_positions failed: {e}")

        # Cross-check vs rmsLimit (broker's own aggregate)
        broker_total = None
        try:
            ab = self.broker.get_account_balance(self.session)
            if isinstance(ab, dict) and ab.get("status") == "success":
                broker_total = float(ab.get("m2m_realized") or 0.0)
        except Exception as e:
            print(f"[SEED-REALIZED] rmsLimit cross-check failed: {e}")

        # Apply
        for sym, val in seeded_by_symbol.items():
            self.realized_pnl_by_symbol[sym] = val
        self.realized_pnl = seeded_total

        print(
            f"[SEED-REALIZED] today's realized PnL loaded from broker: "
            f"total=Rs{seeded_total:.2f} per_symbol={dict(seeded_by_symbol)} "
            f"broker_m2m_realized=Rs{broker_total if broker_total is not None else 'N/A'}"
        )
        if broker_total is not None and abs(broker_total - seeded_total) > 1.0:
            print(
                f"[SEED-REALIZED] WARNING: per-symbol sum (Rs{seeded_total:.2f}) "
                f"differs from broker total (Rs{broker_total:.2f}) by "
                f"Rs{broker_total - seeded_total:.2f}  using per-symbol sum"
            )

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
