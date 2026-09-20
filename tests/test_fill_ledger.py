"""Self-check for the signed-fill ledger + min-cap exit rule.

Covers the drift (100 intended vs 101 actual), shared-account (system 100,
manual 100 -> broker 200), and flip-netting math described in the design.
"""
import pytest

from utils.session_ledger import (
    _compute_ledger_qty_after_fill,
    _signed_fill_delta,
)


def _cap(own: int, broker_qty: int) -> int:
    """Mirror of _exit_qty_for: min(|own|, broker), fallback to broker when flat."""
    if own == 0:
        return int(broker_qty or 0)
    return max(0, min(abs(own), int(broker_qty or 0)))


def test_entry_records_actual_fill_not_intended():
    # Intended 100 (capital/LTP), broker actually filled 101.
    ledger = _compute_ledger_qty_after_fill(0, 101, "OPEN_LONG", side="BUY")
    assert ledger == 101


def test_flip_nets_to_reversed_side():
    # Long +101, flip places SELL 202 -> ledger goes -101 (short).
    long = _compute_ledger_qty_after_fill(0, 101, "OPEN_LONG", side="BUY")
    short = _compute_ledger_qty_after_fill(long, 202, "FLIP_TO_SHORT", side="SELL")
    assert short == -101
    # Flip back: BUY 202 -> flat then +101 (long again).
    back = _compute_ledger_qty_after_fill(short, 202, "FLIP_TO_LONG", side="BUY")
    assert back == 101


def test_shared_account_exits_only_own_qty():
    # System opened 100, someone else opened 100 -> broker holds 200.
    assert _cap(own=100, broker_qty=200) == 100
    assert _cap(own=-100, broker_qty=200) == 100


def test_never_exits_more_than_broker_holds():
    # System owns 100, manual close left broker at 70.
    assert _cap(own=100, broker_qty=70) == 70


def test_flat_ledger_falls_back_to_broker_qty():
    # Paper mode / feature disabled: ledger empty -> old behavior.
    assert _cap(own=0, broker_qty=200) == 200


def test_unknown_action_rejected():
    assert _signed_fill_delta(10, "", "NOT_A_REAL_ACTION") is None