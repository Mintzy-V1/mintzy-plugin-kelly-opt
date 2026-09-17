"""AutoTrader mixin: live LTP tick handling, live/realized portfolio RMS checks,
and per-ticker RMS exit worker. Moved verbatim from auto_trader_exposure_expansion.py
during the package split; no logic changes.
"""
import json
import threading
import time
from datetime import datetime

from utils.redis_keys import live_pnl_key, rms_exited_key


class LivePnlMixin:

    # ---------- LIVE LTP TICK (background WS thread) ----------
    def on_ltp_tick(self, symbol: str, ltp: float, ts_epoch: float) -> None:
        """
        Called from LiveLTPStream's WS thread on every tick.
        Reads from self.positions (snapshot), writes to self.live_pnl.
        Does NOT mutate any field used by the existing PnL pipeline.
        """
        try:
            pos = self.positions.get(symbol)
            if not pos:
                if symbol not in self._tick_first_seen_in_trader:
                    self._tick_first_seen_in_trader.add(symbol)
                    print(f"[TRADER-TICK] {symbol} tick received but no position yet (ltp={ltp:.2f})")
                return
            qty = int(pos.get("qty") or 0)
            entry = float(pos.get("entry_price") or 0.0)
            side = (pos.get("side") or "BUY").upper()
            if qty <= 0 or entry <= 0:
                return

            pnl = (ltp - entry) * qty if side == "BUY" else (entry - ltp) * qty

            if symbol + ":pos" not in self._tick_first_seen_in_trader:
                self._tick_first_seen_in_trader.add(symbol + ":pos")
                print(
                    f"[TRADER-TICK] first tick-with-position for {symbol}: "
                    f"side={side} qty={qty} entry={entry:.2f} ltp={ltp:.2f} pnl={pnl:.2f}"
                )

            with self.live_pnl_lock:
                self.live_pnl[symbol] = {
                    "ltp": ltp,
                    "pnl": pnl,
                    "qty": qty,
                    "entry": entry,
                    "side": side,
                    "ts": ts_epoch,
                }
            with self._paper_lock:
                if symbol in self._paper_positions:
                    self._paper_positions[symbol]["ltp"] = ltp
                last = self._live_pnl_last_write.get(symbol, 0.0)
                if ts_epoch - last >= 1.0:
                    self._live_pnl_last_write[symbol] = ts_epoch
                    write_row = True
                else:
                    write_row = False

            if write_row:
                self._csv_logger.write(
                    self.live_pnl_log,
                    [
                        datetime.fromtimestamp(ts_epoch).strftime("%Y-%m-%d %H:%M:%S"),
                        symbol, side, qty, f"{entry:.2f}",
                        f"{ltp:.2f}", f"{pnl:.2f}",
                    ],
                )

            # ----- Per-ticker RMS halt (independent of portfolio RMS) -----
            # Uses realized (closed trades for this symbol) + unrealized (open
            # position live PnL). Threshold = 1% of entry_price * qty (dynamic per open).
            realized_for_sym = float(self.realized_pnl_by_symbol.get(symbol, 0.0))
            total_for_sym = realized_for_sym + pnl
            # loss_threshold = -self.rms_per_ticker_loss_per_share * qty
            loss_threshold = -(self.rms_per_ticker_loss_pct * entry * qty)

            # Throttled per-ticker PnL log (every ~30s per symbol)  mirrors
            # the [RMS-LIVE-PORTFOLIO] line so you can watch each symbol's
            # headroom against its own threshold.
            now = time.time()
            last_print = self._last_per_ticker_print_ts.get(symbol, 0.0)
            if now - last_print >= self._per_ticker_print_interval_sec:
                self._last_per_ticker_print_ts[symbol] = now
                usage_pct = (total_for_sym / loss_threshold * 100.0) if loss_threshold else 0.0
                print(
                    f"[RMS-TICKER] {symbol} total={total_for_sym:.2f} "
                    f"(realized={realized_for_sym:.2f} + unrealized={pnl:.2f}) "
                    f"threshold={loss_threshold:.2f} usage={usage_pct:.1f}% "
                    f"qty={qty} side={side} ltp={ltp:.2f}"
                )

            if (total_for_sym <= loss_threshold
                    and symbol not in self._exited_symbols
                    and symbol not in self._rms_exit_inflight):
                self._rms_exit_inflight.add(symbol)
                print(
                    f"[RMS-TICKER] {symbol} breached: total={total_for_sym:.2f} "
                    f"(realized={realized_for_sym:.2f} + unrealized={pnl:.2f}) "
                    f"<= threshold={loss_threshold:.2f} (qty={qty}). Exiting."
                )
                self._csv_logger.write(
                    self.rms_events_log,
                    [
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "PER_TICKER_BREACH", symbol,
                        f"{total_for_sym:.2f}", f"{realized_for_sym:.2f}",
                        f"{pnl:.2f}", f"{loss_threshold:.2f}",
                        f"EXIT_SINGLE qty={qty}",
                    ],
                )
                self._notify_rms_exit_to_api(symbol, total_for_sym)
                threading.Thread(
                    target=self._rms_exit_worker,
                    args=(symbol,),
                    name=f"RMSExit-{symbol}",
                    daemon=True,
                ).start()

            # ----- Portfolio RMS halt disabled -----
            # Keep the layer available, but do not let portfolio-level loss
            # exit all stocks. Per-ticker RMS above remains active.
            # self._check_live_portfolio_rms()

            # ----- Push live PnL snapshot to Redis (max 1 write/sec) -----
            now = time.time()
            if now - self._live_pnl_last_redis_write >= 1.0:
                self._live_pnl_last_redis_write = now
                self._push_live_pnl_to_redis()

        except Exception as e:
            print(f"[LIVE-PNL] tick handler error for {symbol}: {e}")

    def _push_live_pnl_to_redis(self) -> None:
        """
        Builds a per-symbol + portfolio PnL snapshot and writes it to Redis
        so the API server (separate process) can serve it tick-by-tick.
        Key: live_pnl:{session_id}   TTL: 5s (auto-expires if trader dies)
        """
        try:
            sid = getattr(self, "session_id", None) or getattr(self, "ui_session_id", None)
            rc = getattr(self.market_client, "redis_client", None)
            if not sid or rc is None:
                return

            # Snapshot live_pnl and realized_pnl_by_symbol safely
            with self.live_pnl_lock:
                live_snapshot = dict(self.live_pnl)

            realized_by_sym = dict(self.realized_pnl_by_symbol)

            # Build per-symbol response  merge open positions + closed-only symbols
            all_symbols = set(live_snapshot.keys()) | set(realized_by_sym.keys())
            symbols_out = {}
            live_unrealized_total = 0.0
            exited_symbols = set(getattr(self, "_exited_symbols", set()) or set())

            for sym in all_symbols:
                live = live_snapshot.get(sym, {})
                is_exited = sym in exited_symbols or str(live.get("status") or "").upper() == "EXITED"
                unrealized = 0.0 if is_exited else round(float(live.get("pnl", 0.0)), 2)
                realized = round(float(realized_by_sym.get(sym, 0.0)), 2)
                live_unrealized_total += unrealized
                symbols_out[sym] = {
                    "ltp": round(float(live.get("ltp", 0.0)), 2),
                    "unrealized_pnl": unrealized,
                    "realized_pnl": realized,
                    "total_pnl": round(unrealized + realized, 2),
                    "qty": 0 if is_exited else int(live.get("qty", 0)),
                    "entry": round(float(live.get("entry", 0.0)), 2),
                    "side": live.get("side", ""),
                    "position_status": "EXITED" if is_exited else "OPEN",
                    "exit_reason": live.get("exit_reason") if is_exited else None,
                }

            realized_total = round(float(self.realized_pnl), 2)
            live_unrealized_total = round(live_unrealized_total, 2)

            payload = {
                "realized_pnl": realized_total,
                "live_unrealized_pnl": live_unrealized_total,
                "total_pnl": round(realized_total + live_unrealized_total, 2),
                "symbols": symbols_out,
                "ts": time.time(),
            }

            rc.setex(live_pnl_key(sid), 5, json.dumps(payload))

        except Exception as e:
            print(f"[LIVE-PNL] Redis push error: {e}")

    def _check_realized_portfolio_rms(self) -> None:
        """
        Called after every trade close (_close_position).
        Checks realized PnL alone  no live unrealized needed.
        Catches the case where all positions close in one batch and no
        further WS ticks arrive to trigger _check_live_portfolio_rms.
        Mirror of break_1's register_pnl() halt check.
        """
        # Portfolio RMS layer is intentionally disabled.
        # Keep this function available, but never let portfolio loss exit all stocks.
        return

        if self._live_portfolio_rms_inflight or self.rms_triggered:
            return
        if not self.rms_loss_limit or self.rms_loss_limit >= 0:
            return

        total_realized = float(self.realized_pnl or 0.0)
        if total_realized > self.rms_loss_limit:
            return

        self._live_portfolio_rms_inflight = True
        self.rms_triggered = True
        print(
            f"\n[RMS-REALIZED-PORTFOLIO] BREACH: realized PnL Rs{total_realized:.2f} "
            f"<= limit Rs{self.rms_loss_limit:.2f}. Exiting all positions."
        )
        self._csv_logger.write(
            self.rms_events_log,
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "REALIZED_PORTFOLIO_BREACH", "ALL",
                f"{total_realized:.2f}", f"{total_realized:.2f}",
                "0.00", f"{self.rms_loss_limit:.2f}",
                "EXIT_ALL realized_only",
            ],
        )
        try:
            self.alerts.notify(
                f"RMS REALIZED PORTFOLIO HALT: realized PnL Rs{total_realized:.2f} "
                f"<= Rs{self.rms_loss_limit:.2f}  exiting all positions"
            )
        except Exception:
            pass

        try:
            with self.positions_lock:
                for sym in list(self.positions.keys()):
                    self._exited_symbols.add(sym)
        except Exception as e:
            print(f"[RMS-REALIZED-PORTFOLIO] mark exited failed: {e}")

        all_syms = set(self.positions.keys())
        for sym in all_syms:
            self._notify_rms_exit_to_api(sym, self.live_pnl.get(sym, {}).get("pnl", 0.0))

        def _do_exit():
            try:
                self._exit_all_positions_and_stop()
                print("Done for the day all positions exitted")
            finally:
                self.stop_event.set()

        threading.Thread(
            target=_do_exit,
            name="RMSRealizedPortfolioExit",
            daemon=True,
        ).start()

    def _check_live_portfolio_rms(self) -> None:
        """
        Sum live_pnl across all symbols. If the running total breaches
        self.rms_loss_limit (already set by the cycle-level RMS init),
        exit every open position. Dedup'd via _live_portfolio_rms_inflight.
        """
        # Portfolio RMS layer is intentionally disabled.
        # Keep this function available, but never let portfolio loss exit all stocks.
        return

        if self._live_portfolio_rms_inflight or self.rms_triggered:
            return
        if not self.rms_loss_limit or self.rms_loss_limit >= 0:
            return  # limit not initialised yet (set in trader.start)

        with self.live_pnl_lock:
            total_live_pnl = sum(float(v.get("pnl") or 0.0) for v in self.live_pnl.values())
            symbols_snapshot = list(self.live_pnl.keys())

        # Realized + currently-unrealized across the portfolio.
        total_realized = float(self.realized_pnl or 0.0)
        total_pnl = total_realized + total_live_pnl

        # Throttled portfolio-PnL log (every ~30s) so we can see RMS headroom.
        now = time.time()
        usage_pct = (total_pnl / self.rms_loss_limit * 100.0) if self.rms_loss_limit else 0.0
        if now - self._last_portfolio_print_ts >= self._portfolio_print_interval_sec:
            self._last_portfolio_print_ts = now
            print(
                f"[RMS-LIVE-PORTFOLIO] total={total_pnl:.2f} "
                f"(realized={total_realized:.2f} + live_unrealized={total_live_pnl:.2f}) "
                f"limit={self.rms_loss_limit:.2f} usage={usage_pct:.1f}% "
                f"open_syms={len(symbols_snapshot)}"
            )
            self._csv_logger.write(
                self.portfolio_pnl_log,
                [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    f"{total_pnl:.2f}", f"{total_realized:.2f}",
                    f"{total_live_pnl:.2f}", f"{self.rms_loss_limit:.2f}",
                    f"{usage_pct:.2f}", len(symbols_snapshot),
                ],
            )

        if total_pnl > self.rms_loss_limit:
            return

        self._live_portfolio_rms_inflight = True
        self.rms_triggered = True  # gate the cycle-level path so it doesn't double-fire
        print(
            f"\n[RMS-LIVE-PORTFOLIO] BREACH: total PnL Rs{total_pnl:.2f} "
            f"(realized={total_realized:.2f} + unrealized={total_live_pnl:.2f}) "
            f"<= limit Rs{self.rms_loss_limit:.2f}. Exiting all positions."
        )
        self._csv_logger.write(
            self.rms_events_log,
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "PORTFOLIO_BREACH", "ALL",
                f"{total_pnl:.2f}", f"{total_realized:.2f}",
                f"{total_live_pnl:.2f}", f"{self.rms_loss_limit:.2f}",
                f"EXIT_ALL syms={len(symbols_snapshot)}",
            ],
        )
        try:
            self.alerts.notify(
                f"RMS LIVE PORTFOLIO HALT: PnL Rs{total_pnl:.2f} "
                f"<= Rs{self.rms_loss_limit:.2f}  exiting all positions"
            )
        except Exception:
            pass

        # Mark every open position as permanently exited so any in-flight
        # cycle skips them on its next iteration.
        try:
            with self.positions_lock:
                for sym in list(self.positions.keys()):
                    self._exited_symbols.add(sym)
        except Exception as e:
            print(f"[RMS-LIVE-PORTFOLIO] mark exited failed: {e}")

        # Notify api_server to clear the symbols payload by reusing the
        # per-ticker drain channel  push every live symbol onto it.
        all_syms = set(symbols_snapshot) | set(self.positions.keys())
        for sym in all_syms:
            self._notify_rms_exit_to_api(sym, self.live_pnl.get(sym, {}).get("pnl", 0.0))

        def _do_exit():
            try:
                self._exit_all_positions_and_stop()
                print("Done for the day all positions exitted")
            finally:
                self.stop_event.set()

        threading.Thread(
            target=_do_exit,
            name="RMSLivePortfolioExit",
            daemon=True,
        ).start()

    def _rms_exit_worker(self, symbol: str) -> None:
        """Run exit_single_position off the WS thread."""
        sym = self._normalize_config_symbol(symbol)
        print(f"[RMS-TICKER] worker start symbol={sym} reason=RMS_TICKER_EXIT")
        try:
            result = self.exit_single_position(sym, exit_reason="RMS_TICKER_EXIT")
            print(
                f"[RMS-TICKER] worker done symbol={sym} "
                f"success={result.get('success')} order_id={result.get('order_id')} "
                f"message={result.get('message')}"
            )
        except Exception as e:
            print(f"[RMS-TICKER] exit failed for {sym}: {e}")
        finally:
            # exit_single_position adds to _exited_symbols on success;
            # drop the inflight marker either way so a retry is possible if it failed.
            self._rms_exit_inflight.discard(sym)

    def _notify_rms_exit_to_api(self, symbol: str, pnl: float) -> None:
        """
        Push the RMS-exited symbol to a Redis list so api_server (different
        process) can prune it from sessions_store[sid]["symbols"].
        """
        try:
            sid = getattr(self, "session_id", None) or getattr(self, "ui_session_id", None)
            rc = getattr(self.market_client, "redis_client", None)
            if not sid or rc is None:
                return
            rc.rpush(
                rms_exited_key(sid),
                json.dumps({"symbol": symbol, "pnl": pnl, "ts": time.time()}),
            )
            rc.expire(rms_exited_key(sid), 86400)
            print(f"[RMS-TICKER] redis notify queued session={sid} symbol={symbol} pnl={float(pnl or 0.0):.2f}")
        except Exception as e:
            print(f"[RMS-TICKER] redis notify failed for {symbol}: {e}")

