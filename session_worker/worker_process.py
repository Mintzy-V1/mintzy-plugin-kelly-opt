import time
from multiprocessing import Process, Queue, Event


class WorkerProcess:
    """Wrapper for a trader process with health monitoring"""

    def __init__(self, session_id: str, process: Process,
                 stop_event: Event, health_queue: Queue, strategy: str = None,
                 symbols: list = None):
        self.session_id = session_id
        self.process = process
        self.stop_event = stop_event
        self.health_queue = health_queue
        # Kept locally so a lost Redis meta key can't make a live worker look like
        # paper and get killed, and so the EOD symbol scope survives a republish.
        self.strategy = strategy
        self.symbols = symbols or []
        self.last_heartbeat = time.time()
        self.started_at = time.time()

    def is_alive(self) -> bool:
        return self.process.is_alive()

    def is_healthy(self, timeout: int = 60) -> bool:
        """Check if worker sent heartbeat recently"""
        return (time.time() - self.last_heartbeat) < timeout

    def update_heartbeat(self):
        self.last_heartbeat = time.time()
