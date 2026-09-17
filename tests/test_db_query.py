"""Regression tests for the trading_logs Mongo query builder (pure)."""
import pytest

from repositories.db import _trading_logs_mongo_query


def test_query_by_session_id():
    q = _trading_logs_mongo_query(session_id="s1")
    assert q == {"session_id": "s1"}


def test_query_by_session_ids():
    q = _trading_logs_mongo_query(session_ids=["s1", "s2"])
    assert q == {"session_id": {"$in": ["s1", "s2"]}}


def test_query_simulation_live():
    q = _trading_logs_mongo_query(session_id="s1", simulation_logs=True)
    assert q == {"session_id": "s1", "simulation_logs": True}


def test_query_simulation_live_excludes_legacy():
    q = _trading_logs_mongo_query(session_id="s1", simulation_logs=False)
    assert q == {"session_id": "s1", "simulation_logs": {"$ne": True}}


def test_query_requires_session():
    with pytest.raises(ValueError):
        _trading_logs_mongo_query()
