"""Regression tests for pure helper/config/logging functions (no broker, no DB)."""
import pytest

from core.config import _decrypt_val, _encrypt_val
from core.helpers import _limit_rows, flatten_numeric_values, guess_free_cash_from_resp
from core.logging import redact


def test_encrypt_decrypt_roundtrip():
    v = "s3cret-value"
    assert _decrypt_val(_encrypt_val(v)) == v


def test_encrypt_none():
    assert _encrypt_val(None) is None
    assert _decrypt_val(None) is None


def test_limit_rows():
    rows = [{"a": i} for i in range(10)]
    assert len(_limit_rows(rows, 3)) == 3
    assert _limit_rows(rows, 3) == rows[-3:]
    assert _limit_rows(rows, 100) == rows
    assert _limit_rows(rows, 0) == rows
    assert _limit_rows(rows, -5) == rows


def test_flatten_numeric_values():
    nums = []
    flatten_numeric_values({"data": {"a": 100, "b": "2,500", "c": [3.5, "40"]}}, nums)
    assert sorted(nums) == [3.5, 40.0, 100.0, 2500.0]


def test_guess_free_cash_from_resp_nested():
    assert guess_free_cash_from_resp({"data": {"availablecash": 50000}}) == 50000.0


def test_guess_free_cash_from_resp_none():
    assert guess_free_cash_from_resp(None) is None
    assert guess_free_cash_from_resp({"data": {}}) is None


def test_redact_masks_secrets():
    d = {"token": "abc", "password": "pw", "user": "alice", "nested": {"refresh_token": "rt"}}
    r = redact(d)
    assert r["token"] == "***"
    assert r["password"] == "***"
    assert r["user"] == "alice"
    assert r["nested"]["refresh_token"] == "***"
    # input must not be mutated
    assert d["token"] == "abc"


def test_redact_leaves_non_dict():
    assert redact("plain") == "plain"
    assert redact(42) == 42
