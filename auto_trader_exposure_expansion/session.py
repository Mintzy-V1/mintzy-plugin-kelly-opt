"""AutoTrader mixin: paper/broker session linking and parallel-executor setup.
Moved verbatim from auto_trader_exposure_expansion.py during the package split;
no logic changes.
"""
from .order_execution import ParallelOrderExecutor


class SessionMixin:

    # ---------- BROKER / SESSION (PAPER) ----------

    def _link_broker(self, force_relink=False):
        live = getattr(self, "session", None)
        if isinstance(live, dict) and live.get("obj") and not live.get("paper"):
            self.broker_live_session = live

        if not force_relink and isinstance(getattr(self, "session", None), dict) and self.session.get("paper"):
            return

        if not getattr(self, "session", None) or not self.session.get("paper"):
            self._paper_orders = {}
            self._paper_positions = {}
            self._paper_order_counter = 0

        self.session = {"obj": None, "paper": True}

        if self.cash_balance <= 0:
            broker_cash = self._fetch_broker_free_cash(context="PAPER-LINK")
            seed_cash = broker_cash if broker_cash is not None else float(self.initial_capital)
            self.cash_balance = seed_cash
            self.current_capital = seed_cash

        if not getattr(self, "_restored_cycle_count", 0):
            restored = self._restore_paper_state_from_mongodb()
            if restored:
                print("[PAPER] Session restored from MongoDB")
            else:
                print("[PAPER] Fresh paper session initialized")
        else:
            print("[PAPER] Paper session re-linked (in-memory state preserved)")

    def _ensure_session(self):
        """Ensure self.session is present and valid."""
        if getattr(self, "session", None) and isinstance(self.session, dict) and "obj" in self.session:
            return True
        try:
            self._link_broker(force_relink=False)
            return True
        except Exception as e:
            self.alerts.notify(f"Failed to initialize paper session: {e}")
            return False
    
    # ==================== PARALLEL EXECUTOR HELPER ====================
    def _ensure_parallel_executor(self):
        """Ensure parallel executor is initialized with current session"""
        if self.parallel_executor is None and hasattr(self, 'session') and self.session:
            self.parallel_executor = ParallelOrderExecutor(
                trader=self,
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
        Paper mode: realized PnL is seeded from MongoDB restore (if any) or starts at 0.
        """
        if self.realized_pnl != 0.0 or self.realized_pnl_by_symbol:
            print(
                f"[SEED-REALIZED] paper session restored: "
                f"total=Rs{self.realized_pnl:.2f} per_symbol={dict(self.realized_pnl_by_symbol)}"
            )
            return

        print("[SEED-REALIZED] fresh paper session  realized PnL starts at 0")

    def _get_broker_positions(self):
        try:
            with self._paper_lock:
                positions = [
                    {
                        "symbol": p["symbol"],
                        "side": p["side"],
                        "qty": int(p["qty"]),
                        "avg_price": float(p.get("avg_price") or 0.0),
                        "ltp": float(p.get("ltp") or p.get("avg_price") or 0.0),
                    }
                    for p in self._paper_positions.values()
                    if int(p.get("qty") or 0) > 0
                ]
            return positions
        except Exception as e:
            self.alerts.notify(f"Paper positions fetch failed: {e}")
            return []

