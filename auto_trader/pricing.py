import time
import json
from datetime import datetime
from typing import Optional


class PricingMixin:
    def _get_bulk_broker_ltp(self, symbols):
        """
        Read closed candle price from Redis â€” the exact same price
        prediction service wrote after fetching the closed candle.
        Redis key: price:live:{ticker}.NS  (set by prediction_service.py)
        Falls back to broker LTP if Redis has nothing.
        """
        ltp_map = {}
        t_func_start = time.time()

        # âœ… Get Redis client from market_client (same Redis prediction service uses)
        redis_client = getattr(self.market_client, "redis_client", None)

        for sym in symbols:
            try:
                ticker = f"{sym}.NS"
                redis_key = f"price:live:{ticker}"   # matches redis_key_live_price(ticker) in prediction_service.py

                # âœ… Step 1: Try Redis first â€” this is what prediction service wrote
                if redis_client is not None:
                    cached = redis_client.get(redis_key)
                    if cached:
                        try:
                            data = json.loads(cached)
                            price = float(data["price"])
                            ts    = data.get("ts", "unknown")
                            if price > 0:
                                ltp_map[sym] = price
                                print(f"[LTP REDIS] {sym} = {price:.2f} (candle close @ {ts})")
                                continue
                        except Exception as e:
                            print(f"[LTP REDIS PARSE ERROR] {sym}: {e}")

                # âœ… Step 2: Fallback â€” broker LTP if Redis miss
                print(f"[LTP REDIS] {sym}: cache miss, falling back to broker LTP")
                if not self._ensure_session():
                    continue

                instruments = [f"NSE|{sym}-EQ"]
                resp = self.broker.get_bulk_ltp(self.session, instruments)

                if not isinstance(resp, dict):
                    continue

                data = resp.get("data") or resp.get("raw", {}).get("data")
                if not isinstance(data, list):
                    continue

                for row in data:
                    ltp = row.get("ltp")
                    if ltp is not None and float(ltp) > 0:
                        ltp_map[sym] = float(ltp)
                        print(f"[LTP BROKER FALLBACK] {sym} = {float(ltp):.2f}")
                        break

            except Exception as e:
                print(f"[LTP ERROR] {sym}: {e}")
                continue

        print(f"[LTP FINAL] {ltp_map}")
        self.tlog.record("LTP_BULK_FETCH_TOTAL", t_func_start, note=f"symbols={len(symbols)} hits={len(ltp_map)}")

        return ltp_map

    def _get_candle_key(self, now, candle):
        candle = str(candle or "5m").lower().strip()

        if candle.endswith("m"):
            step = int(candle[:-1])
        else:
            step = 5  # safe default

        # Round DOWN to nearest candle boundary
        minute_bucket = (now.minute // step) * step

        return now.replace(
            minute=minute_bucket,
            second=0,
            microsecond=0
        ).strftime("%Y-%m-%d %H:%M")

    def _get_live_price_redis(self, symbol: str, candle: str) -> Optional[float]:
        """
        Worker-independent LIVE price cache using Redis.
        Keyed by candle boundary so it auto-refreshes every candle.
        """
        
        print(
            f"[LTP FUNC] symbol={symbol} candle={candle} "
            f"redis={'YES' if getattr(self.market_client, 'redis_client', None) else 'NO'} "
            f"fetch_price={'YES' if hasattr(self.market_client, 'fetch_price') else 'NO'}",
            flush=True
        )

        try:
            # Ensure redis exists
            redis_client = getattr(self.market_client, "redis_client", None)
            if redis_client is None:
                return None

            now = self._now_market_time()
            candle_key = self._get_candle_key(now, candle)

            # Unique key per candle = no stale bleed into next candle
            # redis_key = f"LTP:{symbol}"
            
            ticker_ns = f"{symbol}.NS"
            redis_key = f"price:live:{ticker_ns}" 


            # 1) Try redis cache first
            cached = redis_client.get(redis_key)
            # if cached:
            #     try:
            #         return float(cached)
            #     except Exception:
            #         pass
            if cached:
                try:
                    return float(cached)
                except (ValueError, TypeError):
                    pass
                try:
                    data = json.loads(cached)
                    return float(data["price"])
                except Exception:
                    pass


            # 2) Fetch live price using client.fetch_price (your existing Upstox function)
            if not hasattr(self.market_client, "fetch_price"):
                return None

            ticker = f"{symbol}.NS"
            px = self.market_client.fetch_price(
                ticker=ticker,
                target_datetime=now,
                candle=candle
            )

            if not px:
                return None

            live = px.get("Close")
            if live is None:
                return None

            live = float(live)
            if live <= 0:
                return None

            # 3) Cache in Redis: short TTL (safe)
            # Since key is candle-specific, TTL is just to clean up memory.
            
            # redis_client.setex(redis_key, 5, str(live))
            
            candle_ttl = 60 if candle == "1m" else 310   # was: hardcoded 5
            redis_client.setex(redis_key, candle_ttl, str(live))

            return live

        except Exception as e:
            print(f"[LIVE PRICE REDIS ERROR] {symbol}: {e}")
            return None

    # ---------- P&L / POSITIONS ----------
    def _calculate_pnl(self, symbol, ltp): 
        if ltp is None or ltp <= 0:
            return 0.0
        entry_price = 0.0
        qty = 0
        side = None

        if symbol in self.positions:
            pos = self.positions[symbol]
            entry_price = float(pos.get("entry_price", 0.0))
            qty = int(pos.get("qty", 0))
            side = pos.get("side")

             # âœ… Agar entry_price galat save hua tha (0), broker cache se update karne ki koshish
            if entry_price <= 0:
                with self.broker_pos_lock:
                    broker_positions = list(self._broker_positions_cache or [])

                for p in broker_positions:
                    if p.get("symbol") != symbol:
                        continue
                    fresh_price = float(p.get("avg_price", 0.0))
                    if fresh_price > 0:
                        # âœ… Broker ne ab sahi price de di â€” update karo
                        self.positions[symbol]["entry_price"] = fresh_price
                        entry_price = fresh_price
                        print(f"[ENTRY PRICE UPDATE] {symbol}: â‚¹{fresh_price:.2f} (broker cache se fix hua)")
                    break


        
        # ================================================================
        # Priority 2 â€” broker cache fallback (agar self.positions mein nahi)
        # ================================================================
        else:
            with self.broker_pos_lock:
                broker_positions = list(self._broker_positions_cache or [])

            for p in broker_positions:
                if p.get("symbol") != symbol:
                    continue

                qty = int(p.get("qty", 0))
                if qty <= 0:
                    return 0.0

                side = p.get("side")
                entry_price = float(p.get("avg_price", 0.0))
                break
            # ================================================================
        # Validation
        # ================================================================
        if entry_price <= 0:
            print(f"[PNL WARN] {symbol}: entry_price nahi mili â€” P&L = 0")
            return 0.0

        if qty <= 0:
            return 0.0

        if side not in ("BUY", "SELL"):
            return 0.0

            # ================================================================
        # P&L Formula
        # ================================================================
        if side == "BUY":
            pnl = (ltp - entry_price) * qty
        else:
            pnl = (entry_price - ltp) * qty

        # ================================================================
        # Sanity Check â€” P&L kabhi total investment se zyada nahi honi chahiye
        # ================================================================


        if abs(pnl) > (entry_price * qty):
            print(
                f"[P&L SANITY BREACH] {symbol} | "
                f"pnl={pnl:.2f}, entry={entry_price:.2f}, qty={qty}, ltp={ltp:.2f}"
            )
            if not hasattr(self, "pnl_sanity_errors"):
                self.pnl_sanity_errors = []
            self.pnl_sanity_errors.append({
                "symbol": symbol,
                "pnl": pnl,
                "entry_price": entry_price,
                "qty": qty,
                "ltp": ltp,
                "timestamp": datetime.now().isoformat()
            })
            return 0.0

        return round(pnl, 2)
