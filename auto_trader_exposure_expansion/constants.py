"""Shared constants and small standalone helpers used across the AutoTrader mixins.

Moved verbatim out of the monolithic auto_trader_exposure_expansion.py during the
package split; no logic changes.
"""
import json
import os
from datetime import timedelta, timezone
from typing import Optional



# ==================== CAPITAL PYRAMID (STOP TRADING) ====================
# Must match Mongoose model SavedTradingConfiguration  savedtradingconfigurations
SAVED_TRADING_CONFIGURATION_COLLECTION = "savedtradingconfigurations"
SAVED_TRADING_CONFIGURATION_COLLECTION_CANDIDATES = (
    "savedtradingconfigurations",  # Mongoose default (gateway)
    "SavedTradingConfiguration",     # legacy manual name
)
DEFAULT_MONGO_CONFIG_DB_NAME = "test"

# Rank-based pyramid multipliers (rank 1 = index 0, highest rank gets largest mult)
PYR_MULTS = [1.40, 1.30, 1.20, 1.00, 1.00, 0.75, 0.75, 0.75, 0.75, 0.75]

# Nifty intraday leverage: use 4x of account free cash for pyramid allocation headroom
PYRAMID_LEVERAGE_MULTIPLIER = float(os.environ.get("PYRAMID_LEVERAGE_MULTIPLIER", "4"))

# # Min unrealized PnL vs allocated capital to count as profitable at pyramid stop (0.05%)
# PYRAMID_PROFIT_THRESHOLD_PCT = float(os.environ.get("PYRAMID_PROFIT_THRESHOLD_PCT", "0.0005"))


# Rank  min unrealized PnL as % of allocated capital (from rank table; values are % e.g. 0.0576 = 0.0576%)
PYR_PROFIT_THRESHOLD_PCT = {
    1: 0.0576,
    2: 0.0638,
    3: 0.0744,
    4: 0.0893,
    5: 0.0893,
    6: 0.1190,
    7: 0.1374,
    8: 0.1374,
    9: 0.1374,
    10: 0.1374,
}


def pyramid_multiplier_for_rank(rank) -> Optional[float]:
    try:
        rank_key = int(rank)
    except (TypeError, ValueError):
        return None
    if rank_key < 1:
        return None
    idx = rank_key - 1
    if idx >= len(PYR_MULTS):
        return PYR_MULTS[-1]
    return PYR_MULTS[idx]


def pyramid_profit_threshold_for_rank(rank, capital_allocated: float) -> Optional[float]:
    """Min unrealized PnL (Rs) to count as profitable: capital  rank % of allocated."""
    try:
        rank_key = int(rank)
    except (TypeError, ValueError):
        return None
    if rank_key < 1 or capital_allocated <= 0:
        return None
    pct = PYR_PROFIT_THRESHOLD_PCT.get(rank_key)
    if pct is None:
        pct = PYR_PROFIT_THRESHOLD_PCT[10]  # ranks 11+ use rank 710 bucket
    return float(capital_allocated) * (pct / 100.0)



# Market timezone: IST (UTC+5:30)
MARKET_TZ = timezone(timedelta(hours=5, minutes=30))


def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


