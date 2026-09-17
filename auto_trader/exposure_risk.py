import threading


class ExposureRiskMixin:
    # ---------- SYMBOL LOCK (thread-safe exposure updates) -----------

    def _get_symbol_lock(self, symbol):
        if symbol not in self.symbol_locks:
            self.symbol_locks.setdefault(symbol, threading.Lock())
        return self.symbol_locks[symbol]

    # ---------- RESERVED (PENDING) EXPOSURE READ --------------

    def _reserved_exposure(self, symbol):
        return self.reserved_exposure.get(symbol, 0.0)


    # ---------- TOTAL SYMBOL EXPOSURE (FILLED + RESERVED) -------------

    def _total_symbol_exposure(self, symbol):
        return self._stock_exposure(symbol) + self._reserved_exposure(symbol)


    # ---------- EXPOSURE CAP CHECK (ATOMIC) ----------------

    def _can_reserve_exposure(self, symbol, order_value):
        max_allowed = self.max_exposure_pct * self.initial_capital
        return (self._total_symbol_exposure(symbol) + order_value) <= max_allowed


    # ---------- RESERVE EXPOSURE (BEFORE ORDER SUBMIT) -------------

    def _reserve_exposure(self, symbol, order_value):
        self.reserved_exposure[symbol] = (
            self.reserved_exposure.get(symbol, 0.0) + order_value
        )


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
