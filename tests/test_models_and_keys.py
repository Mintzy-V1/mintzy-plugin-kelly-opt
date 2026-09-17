"""Regression tests for Pydantic request models + Redis key formats."""
import pytest

from models import StockAllocation, TradingConfig, _sim_plugin_payload_summary
from utils.redis_keys import (
    exit_result_key,
    live_pnl_key,
    pyramid_result_key,
    session_meta_key,
    session_pid_key,
    simulation_stop_key,
    stop_job_key,
)

SID = "session_20260910123456_abc123"


def test_trading_config_minimal():
    cfg = TradingConfig(session_id=SID, symbols=[StockAllocation(symbol="RELIANCE", capital=1000)])
    assert cfg.strategy == "A"
    assert cfg.use_broker_cash is True


def test_trading_config_strategy_default():
    cfg = TradingConfig(session_id=SID, symbols=[StockAllocation(symbol="TCS", capital=500)])
    assert cfg.strategy == "A"


def test_stock_allocation_stop_loss_default():
    a = StockAllocation(symbol="INFY", capital=100)
    assert a.stop_loss == 0.05


def test_stock_allocation_rejects_negative_capital():
    with pytest.raises(Exception):
        StockAllocation(symbol="INFY", capital=-1)


def test_sim_plugin_payload_summary():
    cfg = TradingConfig(session_id=SID, symbols=[StockAllocation(symbol="ITC", capital=2000, stop_loss=0.1)])
    s = _sim_plugin_payload_summary(cfg)
    assert s["session_id"] == SID
    assert s["symbol_count"] == 1
    assert s["symbols"][0]["symbol"] == "ITC"
    assert s["symbols"][0]["capital"] == 2000.0


def test_redis_key_formats():
    # Must match production Redis layout (see scripts/verify_redis_keys.py).
    assert session_pid_key(SID) == f"autotrader:session:{SID}"
    assert session_meta_key(SID) == f"autotrader:session:{SID}:meta"
    assert simulation_stop_key(SID) == f"autotrader:simulation_stop:{SID}"
    assert pyramid_result_key(SID) == f"autotrader:pyramid_result:{SID}"
    assert stop_job_key(SID) == f"autotrader:stop_job:{SID}"
    assert exit_result_key(SID, "ITC-EQ") == f"autotrader:exit_result:{SID}:ITC"
    assert live_pnl_key(SID) == f"live_pnl:{SID}"
