import csv
import os
import threading
import time
from queue import Empty, Full, Queue


# ==================== ASYNC CSV LOGGER (non-blocking writes) ====================
class AsyncCsvLogger:
    """
    Background CSV writer used by the WS tick path. Callers do `write(path, row)`
    which is a non-blocking enqueue (microseconds). A daemon thread drains the
    queue, batches rows per file, and flushes every FLUSH_INTERVAL_SEC or when
    a file's batch reaches BATCH_SIZE  whichever comes first.

    Why this exists: synchronous open-append-close inside on_ltp_tick stalls
    the WS thread under high tick volume (25+ symbols at active hours can
    exceed 100 ticks/s, each potentially writing to multiple CSVs).
    """

    BATCH_SIZE = 100
    FLUSH_INTERVAL_SEC = 0.5
    QUEUE_MAXSIZE = 20000

    def __init__(self, name: str = "AsyncCsvLogger"):
        self._queue: Queue = Queue(maxsize=self.QUEUE_MAXSIZE)
        self._stop = threading.Event()
        self._registered: set = set()
        self._dropped_count = 0
        self._last_drop_warn_ts = 0.0
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()
        print(f"[ASYNC-CSV] writer thread started (batch={self.BATCH_SIZE}, "
              f"flush_interval={self.FLUSH_INTERVAL_SEC}s, max_queue={self.QUEUE_MAXSIZE})")

    def register(self, path: str, header: list) -> None:
        """Create the file with header if it doesn't exist. Safe to call repeatedly."""
        if path in self._registered:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if not os.path.exists(path):
                with open(path, "w", newline="") as f:
                    csv.writer(f).writerow(header)
                print(f"[ASYNC-CSV] created {path} with header={header}")
            self._registered.add(path)
        except Exception as e:
            print(f"[ASYNC-CSV] register({path}) failed: {e}")

    def write(self, path: str, row: list) -> None:
        """Non-blocking enqueue. Drops the row if the queue is full."""
        try:
            self._queue.put_nowait((path, row))
        except Full:
            self._dropped_count += 1
            now = time.time()
            if now - self._last_drop_warn_ts > 5.0:   # warn at most every 5s
                print(f"[ASYNC-CSV] queue full, dropped {self._dropped_count} rows "
                      f"(latest target={path})")
                self._last_drop_warn_ts = now

    def stop(self, drain_timeout_sec: float = 5.0) -> None:
        """Drain the queue and stop the writer thread."""
        print("[ASYNC-CSV] stop requested, draining queue...")
        deadline = time.time() + drain_timeout_sec
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.05)
        self._stop.set()
        self._thread.join(timeout=2.0)
        print(f"[ASYNC-CSV] stopped. total_dropped={self._dropped_count}")

    def _run(self) -> None:
        from collections import defaultdict
        buffers = defaultdict(list)
        last_flush = time.time()

        while not self._stop.is_set():
            try:
                path, row = self._queue.get(timeout=0.1)
                buffers[path].append(row)
                if len(buffers[path]) >= self.BATCH_SIZE:
                    self._flush_one(path, buffers)
            except Empty:
                pass

            if time.time() - last_flush >= self.FLUSH_INTERVAL_SEC:
                for path in list(buffers.keys()):
                    self._flush_one(path, buffers)
                last_flush = time.time()

        # Final drain on shutdown
        try:
            while True:
                path, row = self._queue.get_nowait()
                buffers[path].append(row)
        except Empty:
            pass
        for path in list(buffers.keys()):
            self._flush_one(path, buffers)

    def _flush_one(self, path: str, buffers: dict) -> None:
        rows = buffers.get(path)
        if not rows:
            return
        try:
            with open(path, "a", newline="") as f:
                csv.writer(f).writerows(rows)
            buffers[path] = []
        except Exception as e:
            print(f"[ASYNC-CSV] flush failed for {path}: {e}")
# ==================== END ASYNC CSV LOGGER ====================
