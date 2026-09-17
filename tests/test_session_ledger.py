"""Regression tests for the session ledger fill math (pure, no broker)."""
from utils.session_ledger import _compute_ledger_qty_after_fill, _signed_fill_delta


def test_signed_fill_delta_buy():
    assert _signed_fill_delta(10, "BUY", "OPEN_LONG") == 10
    assert _signed_fill_delta(10, "SELL", "EXIT_LONG") == -10


def test_signed_fill_delta_from_action():
    # side missing -> infer from action
    assert _signed_fill_delta(10, "", "OPEN_LONG") == 10
    assert _signed_fill_delta(10, "", "OPEN_SHORT") == -10
    assert _signed_fill_delta(10, "", "UNKNOWN") is None


def test_compute_ledger_qty_after_fill():
    # long open
    assert _compute_ledger_qty_after_fill(0, 10, "OPEN_LONG", "BUY") == 10
    # long exit -> flat
    assert _compute_ledger_qty_after_fill(10, 10, "EXIT_LONG", "SELL") == 0
    # short open (negative)
    assert _compute_ledger_qty_after_fill(0, 10, "OPEN_SHORT", "SELL") == -10
    # zero fill is a no-op
    assert _compute_ledger_qty_after_fill(10, 0, "OPEN_LONG", "BUY") == 10
    # unknown side -> None (caller skips)
    assert _compute_ledger_qty_after_fill(10, 5, "UNKNOWN", "") is None


def test_compute_ledger_flip_short_to_long():
    # flipping short -10 by buying 20 -> +10
    assert _compute_ledger_qty_after_fill(-10, 20, "FLIP_TO_LONG", "BUY") == 10
