import json
import time
from datetime import datetime, time as dt_time, timedelta
from typing import Optional

from .constants import MARKET_TZ, MARKET_EXIT_TIME


class PriceFeedMixin:
    def _get_candle_key(self, now, candle):
        candle = str(candle or "5m").lower().strip()
        step = 5  # Hardcoded to 5m boundaries to prevent gap issues with larger timeframes

        # e.g. now=09:21:05  floor to 09:20  closed candle key
        elapsed = (now.hour * 60 + now.minute) - (9 * 60 + 15)

        if elapsed < 0:
            # Before market open  use 09:15 as key
            return now.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

        floored_offset = (elapsed // step) * step
        closed_candle_dt = now.replace(
            hour=9, minute=15, second=0, microsecond=0
        ) + timedelta(minutes=floored_offset)

        return closed_candle_dt.strftime("%Y-%m-%d %H:%M")

    def _shared_ltp_bar(self, now):
        """15m slot from 09:30 IST (same grid as csv15m / plugin cycles)."""
        local = now
        if getattr(local, "tzinfo", None) is not None:
            local = local.replace(tzinfo=None)
        start = datetime.combine(local.date(), dt_time(9, 30))
        end = datetime.combine(local.date(), dt_time(15, 0))
        if local < start:
            return start
        asof = local if local <= end else end
        slot = (int((asof - start).total_seconds() // 60) // 15) * 15
        bar = start + timedelta(minutes=slot)
        if bar > end:
            bar = end
        return bar

    def _parse_shared_ltp(self, cached):
        if cached is None:
            return None
        try:
            val = float(cached)
            return val if val > 0 else None
        except (ValueError, TypeError):
            pass
        try:
            data = json.loads(cached)
            val = float(data["price"])
            return val if val > 0 else None
        except Exception:
            return None

    def _get_live_price_redis(self, symbol: str, candle: str) -> Optional[float]:
        """
        Shared LTP for all accounts: GET price:ltp:{SYM}.NS:{date}:{HHMM}.
        Written by csv15m (09:3010:15, ) and /predict append (10:30, 11:45, ) via SET NX.
        First trader caller after a miss also SET NX; losers must not use their own fetch.
        """
        t_ltp_sym = time.time()
        redis_client = getattr(self.market_client, "redis_client", None)
        now = self._now_market_time()
        bar = self._shared_ltp_bar(now)
        redis_key = (
            f"price:ltp:{symbol}.NS:{bar.strftime('%Y-%m-%d')}:{bar.strftime('%H%M')}"
        )
        print(
            f"[LTP FUNC] symbol={symbol} candle={candle} key={redis_key} "
            f"redis={'YES' if redis_client else 'NO'}",
            flush=True
        )
        if redis_client is None:
            return None
        try:
            cached = redis_client.get(redis_key)
            val = self._parse_shared_ltp(cached)
            if val is not None:
                print(f"[LTP SHARED] GET hit {redis_key} price={val:.4f}")
                return val

            for _ in range(8):
                time.sleep(0.15)
                cached = redis_client.get(redis_key)
                val = self._parse_shared_ltp(cached)
                if val is not None:
                    print(f"[LTP SHARED] GET wait-hit {redis_key} price={val:.4f}")
                    return val

            if not hasattr(self.market_client, "fetch_price"):
                return None
            ticker = f"{symbol}.NS"
            bar_dt = bar
            if now.tzinfo is not None and bar_dt.tzinfo is None:
                bar_dt = bar_dt.replace(tzinfo=now.tzinfo)
            tlog_fetch_price_start = time.time()
            print("calling market_client.fetch_price inside the get_live_price_redis", ticker, bar_dt, candle)
            px = self.market_client.fetch_price(
                ticker=ticker,
                target_datetime=bar_dt,
                candle=candle,
            )
            self.tlog.record(
                "fetch_Price_total_time",
                tlog_fetch_price_start,
                note=f"fetch_price_total_time={time.time() - tlog_fetch_price_start}",
            )
            if not px or float(px.get("Close") or 0) <= 0:
                cached = redis_client.get(redis_key)
                return self._parse_shared_ltp(cached)
            live = float(px["Close"])
            payload = json.dumps({
                "price": live,
                "bar": bar.strftime("%H:%M"),
                "source": "trader_setnx",
            })
            created = redis_client.set(redis_key, payload, nx=True, ex=5400)
            print(
                f"[LTP SHARED] SET NX {redis_key} price={live:.4f} created={bool(created)}"
            )
            if not created:
                for _ in range(12):
                    cached = redis_client.get(redis_key)
                    val = self._parse_shared_ltp(cached)
                    if val is not None:
                        elapsed_ltp = round(time.time() - t_ltp_sym, 3)
                        print(
                            f"[LTP SHARED] winner price after SET NX lost "
                            f"{redis_key} price={val:.4f}"
                        )
                        print(f"[TIMING] LTP_PER_SYMBOL {symbol:<15} {elapsed_ltp:>7.3f}s  source=SHARED_LTP")
                        return val
                    time.sleep(0.15)
                print(
                    f"[LTP SHARED] SET NX lost and GET empty  refusing local fetch "
                    f"for {redis_key}"
                )
                return None
            elapsed_ltp = round(time.time() - t_ltp_sym, 3)
            print(f"[TIMING] LTP_PER_SYMBOL {symbol:<15} {elapsed_ltp:>7.3f}s  source=SHARED_LTP")
            return live
        except Exception as e:
            print(f"[LIVE PRICE REDIS ERROR] {symbol}: {e}")
            return None

    def _sleep_until_next_candle(self, candle):
      
        candle = str(candle or "5m").lower().strip()
        step = int(candle[:-1]) if candle.endswith("m") else 5

        now = self._now_market_time()

        # Get the current locked candle base calculation
        candle_key_str = self._get_candle_key(now, candle)
        current_boundary = datetime.strptime(candle_key_str, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)

        # Target: NEXT candle boundary (base + step) + 60 seconds
        next_candle_close = current_boundary + timedelta(minutes=step)
        next_run = next_candle_close + timedelta(seconds=60)

        # If we're already past next_run, advance to the next cycle
        if next_run <= now:
            missed = int((now - next_run).total_seconds() // (step * 60)) + 1
            next_run += timedelta(minutes=missed * step)

        # Cap sleep so we wake at/before 15:00 IST for forced square-off
        today_exit = now.replace(
            hour=MARKET_EXIT_TIME.hour,
            minute=MARKET_EXIT_TIME.minute,
            second=0,
            microsecond=0,
        )
        if now < today_exit < next_run:
            next_run = today_exit
            print(
                f"[SCHEDULER] capping sleep to MARKET_EXIT "
                f"{today_exit.strftime('%H:%M:%S')} IST"
            )

        sleep_seconds = max(1, (next_run - now).total_seconds())
        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
    )


        deadline = time.time() + sleep_seconds
        while time.time() < deadline:
            if self.stop_event.is_set():
                print("[SCHEDULER] Stop signal mila neend mein  uth raha hoon!")
                return   #  neend se uthta hai, loop pe wapas jaata hai
            if self._now_market_time().time() >= MARKET_EXIT_TIME:
                print("[SCHEDULER] 15:00 IST reached during sleep  waking for square-off")
                return
            time.sleep(1)


        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next_candle_close={next_candle_close.strftime('%H:%M:%S')} "
            f"firing_at={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
        )

    def convert_candle_to_seconds(self, c):
        c = str(c).lower().strip()

        if c.endswith("m"):
            return int(c[:-1]) * 60

        return 300
