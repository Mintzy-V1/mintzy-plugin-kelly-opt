import os
import threading
from multiprocessing import Manager
from typing import Dict, Any, Optional

from client import MarketClient

from utils.redis_keys import (
    SESSION_META_SUFFIX,
    SESSION_PREFIX,
    SESSION_REDIS_TTL,
)

from .constants import (
    LiveStartPrepResult,
    PYRAMID_RESULT_PREFIX,
    PYRAMID_RESULT_TTL,
    SIMULATION_STOP_PREFIX,
    SIMULATION_STOP_TTL,
    STOP_JOB_PREFIX,
    STOP_JOB_TTL,
)
from .worker_process import WorkerProcess
from .registry import RegistryMixin
from .stop_jobs import StopJobsMixin
from .lifecycle import LifecycleMixin


class SessionManager(RegistryMixin, StopJobsMixin, LifecycleMixin):
    """
    Optimized session manager using multiprocessing for true parallelism.
    Each trader runs in its own process with isolated resources.
    """

    _workers: Dict[str, WorkerProcess] = {}
    _manager = Manager()
    _health_queue = _manager.Queue()
    _monitor_thread = None
    _monitor_stop = threading.Event()


    _market_client: MarketClient = MarketClient()

    @classmethod
    def _redis(cls):
        """Convenience accessor for MarketClient's Redis connection."""
        return cls._market_client.redis_client

    REDIS_KEY_PREFIX = SESSION_PREFIX
    REDIS_META_SUFFIX = SESSION_META_SUFFIX
    SESSION_REDIS_TTL = SESSION_REDIS_TTL
    SIMULATION_STOP_PREFIX = SIMULATION_STOP_PREFIX
    SIMULATION_STOP_TTL = SIMULATION_STOP_TTL
    PYRAMID_RESULT_PREFIX = PYRAMID_RESULT_PREFIX
    PYRAMID_RESULT_TTL = PYRAMID_RESULT_TTL
    STOP_JOB_PREFIX = STOP_JOB_PREFIX
    STOP_JOB_TTL = STOP_JOB_TTL

    # Configuration
    MAX_WORKERS = int(os.environ.get("MAX_TRADER_WORKERS", "6"))  # Limit concurrent processes
    HEALTH_CHECK_INTERVAL = 30  # seconds
    WORKER_TIMEOUT = 120  # seconds without heartbeat = dead


# Backward compat for code that references SessionManager.LiveStartPrepResult
SessionManager.LiveStartPrepResult = LiveStartPrepResult
