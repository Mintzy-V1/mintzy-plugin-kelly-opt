"""AutoTrader mixin: market-time helper, per-symbol locks, and exposure
reservation/release bookkeeping. Moved verbatim from
auto_trader_exposure_expansion.py during the package split; no logic changes.
"""
import threading
import time
from datetime import datetime

from .constants import MARKET_TZ


class ExposureMixin:

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

    # ---------- TOTAL SYMBOL EXPOSURE (FILLED + RESERVED) -------------
    
    def _total_symbol_exposure(self, symbol):
        return self._stock_exposure(symbol) + self._reserved_exposure(symbol)

    # ---------- EXPOSURE CAP CHECK (ATOMIC) ----------------

    def _can_reserve_exposure(self, symbol, order_value):
        t0 = time.time()

        result = (self._total_symbol_exposure(symbol) + order_value) <= (self.max_exposure_pct * self.initial_capital)

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

