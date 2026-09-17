import json
from datetime import time as dt_time, timedelta, timezone


# Market timezone: IST (UTC+5:30)
MARKET_TZ = timezone(timedelta(hours=5, minutes=30))

# Hard square-off window (IST)
MARKET_EXIT_TIME = dt_time(15, 0)       # 3:00 PM IST  exit all open positions
MARKET_EXIT_WARN_TIME = dt_time(14, 55) # 2:55 PM IST  warning before auto exit

# 14:15 IST stop-lock  exit losers, continue with green symbols
STOP_LOCK_TIME = dt_time(14, 15)


def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}
