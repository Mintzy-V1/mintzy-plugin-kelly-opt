"""Pydantic request models and simulation-timing helpers for the plugin API."""
import time
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from core.logging import get_logger

_sim_log = get_logger("SIMULATION")


class BrokerCredentials(BaseModel):
    api_key: str = Field(..., min_length=1, description="Angel One API Key")
    client_code: str = Field(..., min_length=1, description="Angel One Client Code")
    password: str = Field(..., min_length=1, description="Angel One Password")


class TOTPRequest(BaseModel):
    session_id: str = Field(..., description="Session ID from initial auth")
    totp: str = Field(default="", description="TOTP code (optional)")


class StockAllocation(BaseModel):
    symbol: str = Field(..., description="Stock symbol (e.g., RELIANCE)")
    capital: float = Field(..., gt=0, description="Capital to allocate")
    stop_loss: float = Field(default=0.05, ge=0, le=1, description="Stop loss percentage (0-1)")


class TradingConfig(BaseModel):
    session_id: str = Field(..., description="Active session ID")
    strategy: str = "A"
    symbols: list[StockAllocation] = Field(..., min_length=1, max_length=25)
    time_frame: str = Field(default="3 hours", description="Prediction time frame")
    use_broker_cash: bool = Field(default=True, description="Use actual broker cash as capital")
    candle: Optional[str] = Field(default=None, description="Optional candle interval (e.g. '1','5','1m','5m')")
    configuration_id: Optional[str] = Field(
        default=None,
        description="SavedTradingConfiguration id for capital pyramid on stop",
    )
    leverage_multiplier: Optional[float] = Field(
        default=None,
        gt=0,
        description="Intraday leverage multiplier for exposure/pyramid (e.g. 4.0). Falls back to SavedTradingConfiguration then default.",
    )


def _sim_plugin_now_ms() -> int:
    return int(time.time() * 1000)


def _sim_plugin_elapsed_ms(start_perf: float) -> float:
    return round((time.perf_counter() - start_perf) * 1000, 2)


def _sim_plugin_payload_summary(config: "TradingConfig") -> Dict[str, Any]:
    return {
        "session_id": config.session_id,
        "strategy": config.strategy,
        "configuration_id": config.configuration_id,
        "leverage_multiplier": config.leverage_multiplier,
        "time_frame": config.time_frame,
        "use_broker_cash": config.use_broker_cash,
        "candle": config.candle,
        "symbol_count": len(config.symbols),
        "symbols": [
            {
                "symbol": s.symbol,
                "capital": float(s.capital),
                "stop_loss": float(s.stop_loss),
            }
            for s in config.symbols
        ],
    }


def _sim_plugin_log(event: str, **fields):
    _sim_log.info(
        "SIM-PLUGIN-TIMING %s | %s",
        event,
        " | ".join(f"{key}={value}" for key, value in fields.items()),
    )
