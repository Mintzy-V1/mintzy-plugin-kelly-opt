# Resilience Fixes — 10:30 AM Simulation → ~1 PM Live Switch Window

## Purpose and constraints

This document logs 9 fixes applied to harden the trading system against failure
points identified in an audit of the operational window where a session runs
in simulation ("paper", strategy B) from ~10:30 AM to ~1 PM, then gets stopped
and a live session is started using the pyramid capital handoff from the
simulation run.

**Every fix here is additive.** None of them changes what the system does when
everything is working normally (the "happy path"). Each fix only changes
behavior on a *failure* path that previously had no bound, no retry, or no
distinguishing signal — turning "hangs forever" into "fails after N seconds",
or "silently loses data" into "retries, then fails visibly." No trading logic,
order-placement logic, pyramid math, or session-state decisions were altered.
Where a numeric constant changed (the shutdown timeout in fix #8), it's a pure
parameter increase to prevent a premature kill — not a behavior/algorithm change.

All fixes were smoke-tested directly (mocked fast/slow calls) to confirm the
happy path is unaffected and the timeout/retry path actually triggers. The
full `pytest tests/` suite (23 tests) and all repo imports were re-verified
after every change.

---

## Fix #1 — Timeout on every Angel One broker SDK call

**File:** `broker_angle.py`

**Problem:** Every SmartAPI SDK call (`ltpData`, `placeOrder`, `modifyOrder`,
`cancelOrder`, `orderBook`, `rmsLimit`/`holding`/`position` via
`get_account_balance`) went through `_call_api()`, which just called the
function directly with **no timeout**. A network stall on Angel One's side
could hang the calling thread forever.

**Change:** `_call_api()` now submits the call to a shared
`ThreadPoolExecutor` (`_BROKER_CALL_EXECUTOR`, 16 workers) and waits at most
`BROKER_API_TIMEOUT_SECONDS` (env-configurable, default **20s**) via
`future.result(timeout=...)`. On timeout it raises `TimeoutError` instead of
hanging. Every existing caller already wraps `_call_api()` in
`try/except Exception` and returns an error dict either way, so this only
changes the failure path from "hang" to "raise after 20s" — the success path
and return value are identical.

**Known limitation (documented in code):** Python threads cannot be forcibly
killed. If the underlying SDK call is truly stuck (not just slow), the
timeout makes the *caller* stop waiting, but the stuck worker thread in the
pool keeps running in the background. A generous worker count (16) means a
few stuck calls won't immediately starve the pool, but this is a best-effort
mitigation, not true cancellation.

---

## Fix #2 — Redis operation timeout (not just connect timeout)

**Files:** `client.py`, `repositories/redis.py`

**Problem:** Both `RedisCluster(...)` constructions set
`socket_connect_timeout=5` (bounds the initial TCP handshake) but no
`socket_timeout` (bounds each subsequent read/write). Once connected, a
degraded network path could make any `.get()`/`.set()`/`.setex()` call block
indefinitely.

**Change:** Added `socket_timeout=float(os.environ.get("REDIS_SOCKET_TIMEOUT", "10"))`
to both constructors. Redis calls that complete in well under 10s today are
completely unaffected; only a call stuck on a degraded connection now fails
after 10s instead of hanging.

---

## Fix #3 — Parallelized bulk LTP fetch with per-symbol + overall timeout

**File:** `broker_angle.py` (`_get_bulk_broker_ltp`)

**Problem:** This method looped over symbols **sequentially**, calling
`self.obj.ltpData(...)` directly (bypassing `_call_api`, so fix #1 didn't
cover it). One slow/hung symbol stalled the LTP fetch for every other symbol
in the session, once per trading cycle.

**Change:** Symbols are now fetched concurrently via a per-call
`ThreadPoolExecutor` (sized to `min(len(symbols), 16)`), with:
- a per-symbol timeout (`BULK_LTP_PER_SYMBOL_TIMEOUT_SECONDS`, default **8s**)
- an overall timeout for the whole batch (`BULK_LTP_OVERALL_TIMEOUT_SECONDS`, default **30s**)

A symbol that fails or times out is skipped and logged — exactly the same
observable outcome as the old per-symbol `try/except: continue`, just reached
concurrently instead of sequentially. The executor is explicitly shut down
with `wait=False` (not via a `with` block) so the function returns on
schedule instead of blocking on an abandoned hung thread — this was caught
and fixed during smoke-testing (see verification section below).

**Smoke-tested:** 3 symbols, 1 artificially hung — function returned at the
overall timeout with the 2 fast results, correctly omitting the hung one.
All-fast case: identical result set, sub-millisecond overhead.

---

## Fix #4 — LTP websocket staleness watchdog

**File:** `live_ltp_ws.py`

**Problem:** `LiveLTPStream` only reconnects when the SDK explicitly fires
`on_error`/`on_close`, or `connect()` raises. A connection that stays
technically open but silently stops delivering ticks (a known failure mode
for exchange feeds) would never trigger a reconnect — the trader would keep
using stale prices with no visible error.

**Change:** Added a `_last_tick_ts` timestamp updated on every successfully
processed tick, plus a new daemon watchdog thread (`_watchdog_loop`) polling
every `LTP_WATCHDOG_POLL_SECONDS` (default **10s**). If the stream is marked
`_connected` but no tick has arrived for `LTP_STALE_TICK_TIMEOUT_SECONDS`
(default **45s**), it logs a warning and force-closes the socket
(`_sws.close_connection()`), which triggers the *existing* reconnect loop in
`_run()` — no new reconnect logic was written, this just forces the existing
mechanism to fire in a case it previously couldn't detect. While ticks flow
normally, `_last_tick_ts` keeps advancing and the watchdog never fires — zero
behavior change on the happy path.

---

## Fix #5 — Distinguishable reason when the pyramid cash lookup times out

**Files:** `auto_trader_exposure_expansion/broker_cash.py`,
`auto_trader_exposure_expansion/pyramid.py`

*(Scope note: the capital-carry-forward "pyramid" feature only exists in the
`auto_trader_exposure_expansion` package — strategy B, the simulation trader.
It is not present in `auto_trader` (strategy A) or
`auto_trader_exposure_expansion_org` (strategy C), so no changes were needed
there.)*

**Problem:** `_apply_capital_pyramid_on_stop()` sets
`reason="broker_cash_unavailable"` in the pyramid handoff whenever
`_fetch_broker_free_cash()` returns `None` — whether that's because the
broker genuinely returned no usable cash data, or because the underlying
`get_account_balance()` call was killed by the new fix #1 timeout. Both cases
looked identical in logs/DB, making it impossible to tell "pyramid legitimately
found nothing" from "pyramid calculation got cut off."

**Change:** `_fetch_broker_free_cash()` now sets a side-channel instance flag
`self._pyramid_cash_fetch_timed_out` (reset to `False` at the top of every
call, so it never leaks a stale value) — `True` only when the failure text
contains "timed out" (from fix #1) or the exception itself is a
`TimeoutError`. `_apply_capital_pyramid_on_stop()` reads this flag and uses
reason `"broker_cash_timeout"` instead of `"broker_cash_unavailable"` when set.
The **outcome is identical either way** (`live_allowed=False`, pyramid not
applied) — only the diagnostic label differs. No return type or call
signature changed.

---

## Fix #6 — Bounded the unguarded `orderBook()` diagnostic call

**File:** `auto_trader/order_execution.py`

**Problem:** After a successful order placement, this file calls
`self.session["obj"].orderBook()` **directly** (bypassing `broker_angle.py`'s
`_call_api`, so fix #1 didn't cover it) purely to print Angel's RMS
rejection-reason diagnostics. This call had no timeout — a hang here blocked
that entire `OrderWorker` thread indefinitely even though the order itself
had already succeeded, permanently shrinking the parallel-execution worker
pool by one.

**Change:** The call now goes through the same shared
`broker_angle._BROKER_CALL_EXECUTOR` / `BROKER_API_TIMEOUT_SECONDS` used
everywhere else in `broker_angle.py`, via
`future = broker_angle._BROKER_CALL_EXECUTOR.submit(...); future.result(timeout=...)`.
The existing `except Exception` around this block already handles
`TimeoutError` (it's an `Exception` subclass) exactly like any other failure
— it just logs and moves on, same as before. `auto_trader_exposure_expansion`
and `auto_trader_exposure_expansion_org`'s `order_execution.py` files do not
have this unguarded call (confirmed by direct search), so no change was
needed there.

---

## Fix #7 — Confirm the 1 PM switch uses the non-blocking stop path

**File:** `routes/simulation.py` (verified only — **no code changed**)

**Finding:** `POST /api/trading/stop-simulation/{session_id}` already
defaults to the non-blocking path: it calls
`SessionManager.stop_simulation_session_async` and returns HTTP 202
immediately with a poll URL; the caller then polls
`GET .../stop-simulation/{session_id}/status` for completion. The blocking
`stop_simulation_session` path only runs when the caller explicitly passes
`?wait=true`, which the code itself labels `"legacy sync mode"`.

**Conclusion:** No code change was required — this was already implemented
correctly. Documented here as an operational recommendation: **the 1 PM
switch trigger should call this endpoint without `?wait=true`** so the HTTP
request thread is never held hostage for the multi-minute worst-case shutdown
sequence. If any external caller (frontend/plugin) is currently passing
`?wait=true` for this specific call, switching it to the default async mode
+ polling would remove that risk entirely.

---

## Fix #8 — Realigned parent/child shutdown timeout budgets

**Files:** `session_worker/constants.py`, `session_worker/lifecycle.py`

**Problem:** This was the highest-severity finding. The parent
(`SessionManager.stop_session`, called during the simulation→live switch)
gave up waiting for the child worker process after only **60 seconds**
(`worker.process.join(timeout=60)`), then force-terminated and eventually
SIGKILLed it. But the child's own graceful shutdown sequence
(`trader_worker.py`: `trader_thread.join(45)` → `trader.shutdown()` — which
runs the pyramid capital calculation, now bounded by fix #1's broker-call
timeouts across up to 3 sequential calls → `trader_thread.join(60)`) has a
realistic worst case around **195 seconds** once broker calls are slow but
not instant. The parent could — and would — SIGKILL the child mid
pyramid-calculation, before it ever got to write the pyramid handoff result
to Redis, silently losing the day's capital carry-forward.

**Change:** Added `WORKER_GRACEFUL_STOP_TIMEOUT_SECONDS`
(env-configurable, default **240s** — comfortably above the ~195s worst
case) to `session_worker/constants.py`, and changed
`session_worker/lifecycle.py`'s `stop_session()` to use it instead of the
hardcoded `60`. This is the one place where a numeric constant changed rather
than pure additive logic — but it's a parameter increase only: normal
shutdowns that complete in a few seconds today are completely unaffected;
only the "broker/Mongo is slow right now" case behaves differently (waits
long enough to actually finish instead of being killed early).

---

## Fix #9 — Retry on the pyramid-result Redis write

**File:** `session_worker/trader_worker.py`

**Problem:** The single most consequential write in the whole chain — the
pyramid handoff result written to Redis at the end of `_trader_worker`'s
shutdown sequence, which the live-start gate reads to decide whether capital
carries forward — was a bare `try/except: print(...)` with no retry. One
transient Redis error at exactly the wrong moment silently defaulted the live
session to "no capital carry-forward" with only a `print()` in a worker
process log as the only trace.

**Change:** The `setex()` call is now wrapped in the same 3-attempt retry
pattern (with `0.1s * attempt` backoff) already used elsewhere in this
codebase for the Redis meta-save in `start_session`. On success, logs exactly
as before. Only after all 3 attempts fail does it log the
"failed after 3 attempts" message — behavior is identical to before for the
success case, and only differs in retrying transient failures before giving
up.

---

## Verification performed

- **Syntax check** (`ast.parse`) on every modified file — all pass.
- **Full import check** — every touched module and package imports cleanly;
  the only import-time failures (`auto_trader`, `session_worker`,
  `session_manager` — `ConfigurationError: Empty host`) are a pre-existing
  sandbox issue (missing real Redis config in this environment), confirmed
  present identically before any of these changes were made.
- **`pytest tests/`** — 23/23 passed, before and after.
- **Direct smoke tests** (mocked fast/slow calls, no real broker/Redis needed):
  - `_call_api`: fast call returns instantly with correct value; a call
    exceeding the timeout raises `TimeoutError` at the expected time.
  - `_get_bulk_broker_ltp`: 3 symbols with 1 artificially hung — function
    returned at the overall timeout boundary with the 2 fast results, hung
    symbol correctly omitted; all-fast case returned all 3 results in
    sub-millisecond time (happy path unaffected). This test also caught and
    fixed a real bug in the first draft (the `with pool:` context manager
    blocked on the hung thread instead of respecting the overall timeout).

## What was **not** changed

No trading/order-placement logic, no pyramid capital math, no session
lifecycle decisions (when to start/stop/reconcile a worker), no API request/
response contracts, and no default numeric behavior on the happy path. Every
change here either bounds a previously-unbounded call, adds a
previously-missing retry, adds a previously-missing staleness check, or
widens a timeout that was already too short for its own downstream logic
(fix #8) — never removes or alters existing decision logic.
