import os
import time
import json
import threading
import traceback
import signal
from multiprocessing import Queue, Event
from typing import Dict, Any, Optional
from datetime import datetime

from auto_trader import AutoTrader as AutoTraderA
from auto_trader_exposure_expansion import AutoTrader as AutoTraderB
from alerts import AlertManager
from client import PredictionClient, MarketClient
from broker_angle import BrokerConnector
from live_ltp_ws import LiveLTPStream

from utils.redis_keys import (
    exit_request_key,
    exit_result_key,
    pyramid_result_key,
    simulation_stop_key,
)
from utils.eod_exit import log_eod_config

from .constants import PYRAMID_RESULT_TTL, _PYRAMID_HANDOFF_BLOCKED


def _signal_trader_stop(trader, stop_event, session_id: str, reason: str) -> None:
    """
    First action on any stop path: halt the trader loop before pyramid/shutdown.
    Sets trader threading.Event first, then the worker multiprocessing.Event.
    Both .set() calls are idempotent.
    """
    if trader is not None and hasattr(trader, "stop_event"):
        trader.stop_event.set()
        print(f"[Worker-{session_id}] trader.stop_event set ({reason})")
    stop_event.set()


def _trader_worker(
    session_id: str,
    strategy: str,
    symbols: list,
    allocations: dict,
    time_frame: str,
    candle: str,
    broker_config: dict,
    trading_logs_collection_name: str,
    stop_event: Event,
    health_queue: Queue,
    mongo_uri: str,
    mongo_db_name: str,
    configuration_id: Optional[str] = None,
    mongo_config_db_name: Optional[str] = None,
    leverage_multiplier: Optional[float] = None,
):
    """
    Worker process that runs a single AutoTrader instance.
    Isolated from other traders - has its own Python interpreter and memory space.
    """
    import signal

    from .manager import SessionManager

    trader_holder: Dict[str, Any] = {"trader": None}

    def handle_sigterm(signum, frame):
        print(f"[Worker-{session_id}] SIGTERM mila — graceful shutdown...")
        _signal_trader_stop(trader_holder["trader"], stop_event, session_id, "SIGTERM")

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        # Set up environment for this worker
        os.environ["ANGEL_API_KEY"] = broker_config["api_key"]
        os.environ["ANGEL_CLIENT_CODE"] = broker_config["client_code"]
        os.environ["ANGEL_PASSWORD"] = broker_config["password"]

        # Initialize broker
        broker = BrokerConnector(require_totp=False)
        restored = broker.restore_session(broker_config["broker_session"])

        # Clean up env immediately
        for k in ["ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PASSWORD"]:
            os.environ.pop(k, None)

        if not restored or not restored.get("token"):
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": "Failed to restore broker session"
            })
            return

        # Set up MongoDB connection (each process gets its own connection)
        from pymongo import MongoClient
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_db = mongo_client[mongo_db_name]
        trading_logs_collection = mongo_db[trading_logs_collection_name]
        config_db_name = mongo_config_db_name or os.environ.get("MONGO_CONFIG_DB_NAME") or "test"
        os.environ["MONGO_CONFIG_DB_NAME"] = config_db_name
        print(
            f"[Worker-{session_id}] Mongo DBs — sessions/logs: {mongo_db_name}, "
            f"SavedTradingConfiguration: {config_db_name}"
        )

        # Initialize clients
        prediction_client = PredictionClient(
            api_key="XeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            base_url=os.environ.get("PREDICTION_BASE_URL", "http://54.204.215.28:8000/predict")
        )
        market_client = MarketClient()

        # Redis client for inter-process messaging (single-symbol exit requests)
        exit_redis_client = getattr(market_client, 'redis_client', None)

        # Select trader class (C is lazy-imported so api_server boots even if org module differs on disk)
        TRADER_MAP = {"A": AutoTraderA, "B": AutoTraderB}
        if strategy == "C":
            try:
                from auto_trader_exposure_expansion_org import AutoTrader as AutoTraderC
                TRADER_MAP["C"] = AutoTraderC
            except ImportError as e:
                health_queue.put({
                    "session_id": session_id,
                    "status": "error",
                    "error": (
                        f"Strategy C (auto_trader_exposure_expansion_org) unavailable: {e}. "
                        "Ensure auto_trader_exposure_expansion_org.py defines class AutoTrader."
                    ),
                })
                return
        TraderClass = TRADER_MAP.get(strategy)

        if not TraderClass:
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": f"Invalid strategy: {strategy}"
            })
            return

        # Create trader instance
        trader = TraderClass(
            prediction_client=prediction_client,
            market_client=market_client,
            broker=broker,
            alerts=AlertManager(),
            trading_logs_collection=trading_logs_collection
        )
        trader.config_db_name = config_db_name

        # Configure trader
        trader.session_id = session_id
        trader.broker_live_session = restored
        trader.broker_session_payload = broker_config.get("broker_session")
        session_free_cash = broker_config.get("free_cash")
        if session_free_cash is not None:
            try:
                trader.session_free_cash = float(session_free_cash)
            except (TypeError, ValueError):
                pass
        trader.session = restored
        trader.ui_session_id = session_id
        trader.symbol_allocations = {k: v["capital"] for k, v in allocations.items()}
        trader.initial_allocations = allocations
        trader.configuration_id = configuration_id
        trader.simulation_logs = strategy == "B"

        if strategy != "B" and not allocations:
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": "Live start without symbol allocations",
            })
            return

        log_eod_config()
        if leverage_multiplier is not None:
            try:
                trader.leverage_multiplier = float(leverage_multiplier)
            except (TypeError, ValueError):
                trader.leverage_multiplier = None

        trader_holder["trader"] = trader

        # Signal that we're healthy and starting
        health_queue.put({
            "session_id": session_id,
            "status": "starting",
            "timestamp": time.time()
        })

        # Start heartbeat thread
        def heartbeat_loop():
            while not stop_event.is_set():
                try:
                    health_queue.put({
                        "session_id": session_id,
                        "status": "running",
                        "timestamp": time.time()
                    }, timeout=1)
                except:
                    pass
                time.sleep(10)  # Heartbeat every 10 seconds

        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        # Run the trader with monitoring
        print(f"[Worker-{session_id}] Starting trader with {len(symbols)} symbols")

        # Wrap trader.start in a monitoring loop
        trader_thread = threading.Thread(
            target=trader.start,
            args=(symbols, time_frame, candle),
            kwargs={
                "initial_allocations": allocations,
                "leverage_multiplier": leverage_multiplier,
            },
            daemon=False  # Don't make daemon - we want proper cleanup
        )
        trader_thread.start()

        # ---------- Background LTP stream (independent of PnL flow) ----------
        ltp_stream = None
        try:
            print(
                f"[Worker-{session_id}] Wiring LiveLTPStream -> trader.on_ltp_tick "
                f"(callable={callable(getattr(trader, 'on_ltp_tick', None))}) "
                f"symbols={list(allocations.keys())}"
            )
            ltp_stream = LiveLTPStream(broker, trader.on_ltp_tick)
            ltp_stream.start(list(allocations.keys()))
            print(f"[Worker-{session_id}] LiveLTPStream started for {len(allocations)} symbols")
        except Exception as e:
            print(f"[Worker-{session_id}] LiveLTPStream failed to start: {e}")

        # Monitor for stop signal AND single-symbol exit requests
        exit_queue_key = exit_request_key(session_id)
        sim_stop_key = simulation_stop_key(session_id)
        # Latched at detection time so a slow shutdown can never lose the fact
        # that this was a simulation stop rather than a plain stop.
        simulation_stop_latched = False
        while trader_thread.is_alive() and not stop_event.is_set():
            if exit_redis_client:
                try:
                    if exit_redis_client.get(sim_stop_key):
                        simulation_stop_latched = True
                        _signal_trader_stop(
                            trader, stop_event, session_id, "redis simulation_stop"
                        )
                        break
                except Exception as e:
                    print(f"[Worker-{session_id}] Simulation stop flag check error: {e}")

            # Check for single-symbol exit requests from Redis
            if exit_redis_client:
                try:
                    exit_req_raw = exit_redis_client.lpop(exit_queue_key)
                    if exit_req_raw:
                        data = json.loads(exit_req_raw)
                        exit_symbol = data.get("symbol", "")
                        print(f"[Worker-{session_id}] Exit request received for symbol: {exit_symbol}")
                        result = trader.exit_single_position(exit_symbol)
                        # Push result back to Redis for the API to read
                        result_key = exit_result_key(session_id, exit_symbol)
                        exit_redis_client.setex(result_key, 60, json.dumps(result))
                        print(f"[Worker-{session_id}] Exit result for {exit_symbol}: {result}")
                except Exception as e:
                    print(f"[Worker-{session_id}] Exit queue check error: {e}")

            trader_thread.join(timeout=1)

        handoff = None
        # If stop was requested, shutdown trader
        if stop_event.is_set():
            print(f"[Worker-{session_id}] Stop requested, shutting down...")
            _signal_trader_stop(trader, stop_event, session_id, "worker stop monitor")
            trader_thread.join(timeout=45)
            if trader_thread.is_alive():
                print(
                    f"[Worker-{session_id}] trader_thread still alive after 45s "
                    "— proceeding with shutdown"
                )
            trader.shutdown()
            print("shutdown called successfully")
            trader_thread.join(timeout=60)

            handoff = getattr(trader, "_pyramid_handoff_result", None)
            if handoff is None:
                handoff = dict(_PYRAMID_HANDOFF_BLOCKED)
            if exit_redis_client:
                # Resilience fix (error_fix_detail.md #9): this is the single most
                # consequential write in the whole shutdown chain - the live-start
                # gate reads it, and it previously had no retry (one transient
                # Redis error silently blocked live trading with only a print()
                # as a trace). Retries 3x with the same backoff pattern already
                # used for the Redis meta-save retry in lifecycle.py start_session.
                pyramid_write_ok = False
                last_pyramid_write_err = None
                for attempt in range(3):
                    try:
                        exit_redis_client.setex(
                            pyramid_result_key(session_id),
                            PYRAMID_RESULT_TTL,
                            json.dumps(handoff),
                        )
                        pyramid_write_ok = True
                        break
                    except Exception as e:
                        last_pyramid_write_err = e
                        if attempt < 2:
                            time.sleep(0.1 * (attempt + 1))
                if pyramid_write_ok:
                    print(
                        f"[Worker-{session_id}] Pyramid handoff stored in Redis "
                        f"(live_allowed={handoff.get('live_allowed')}, reason={handoff.get('reason')})"
                    )
                else:
                    print(
                        f"[Worker-{session_id}] Failed to store pyramid handoff in Redis "
                        f"after 3 attempts: {last_pyramid_write_err}"
                    )

        if ltp_stream is not None:
            try:
                ltp_stream.stop()
            except Exception as e:
                print(f"[Worker-{session_id}] LiveLTPStream stop error: {e}")


        simulation_stop = simulation_stop_latched
        try:
            if exit_redis_client:
                sim_key = simulation_stop_key(session_id)
                if exit_redis_client.get(sim_key):
                    simulation_stop = True
                    exit_redis_client.delete(sim_key)
        except Exception as e:
            print(f"[Worker-{session_id}] Simulation stop flag check error: {e}")

        # Third signal: a stop job only ever exists because a simulation stop was
        # requested, so it still identifies the path if both Redis reads came up empty.
        pending_stop_job = None
        try:
            pending_stop_job = SessionManager.read_stop_job(session_id)
            if not simulation_stop and pending_stop_job and pending_stop_job.get("status") == "stopping":
                simulation_stop = True
                print(
                    f"[Worker-{session_id}] Simulation stop inferred from pending stop job "
                    "(flag missing or expired)"
                )
        except Exception as e:
            print(f"[Worker-{session_id}] Stop job lookup error: {e}")

        if simulation_stop:
            health_queue.put({
                "session_id": session_id,
                "status": "simulation_stopped",
                "timestamp": time.time()
            })
            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "trading_status": "simulation_stopped",
                            "simulation_stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Simulation stopped in Mongo (session auth unchanged)")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark simulation stopped in Mongo: {e}")

            if stop_event.is_set():
                try:
                    if handoff is None:
                        handoff = dict(_PYRAMID_HANDOFF_BLOCKED)
                    SessionManager.complete_stop_job(
                        session_id,
                        handoff,
                        simulation_stop=True,
                    )
                except Exception as job_err:
                    print(f"[Worker-{session_id}] Failed to update stop job in Redis: {job_err}")
        else:
            health_queue.put({
                "session_id": session_id,
                "status": "stopped",
                "timestamp": time.time()
            })

            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "status": "stopped",
                            "trading_status": "stopped",
                            "stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Session status marked stopped in Mongo")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark session stopped in Mongo: {e}")

        # Cleanup
        mongo_client.close()

    except Exception as e:
        error_msg = f"Worker error: {str(e)}\n{traceback.format_exc()}"
        print(f"[Worker-{session_id}] {error_msg}")
        try:
            existing_job = SessionManager.read_stop_job(session_id)
            if existing_job and existing_job.get("status") == "stopping":
                SessionManager.fail_stop_job(session_id, error_msg)
        except Exception:
            pass
        health_queue.put({
            "session_id": session_id,
            "status": "error",
            "error": error_msg,
            "timestamp": time.time()
        })
