import csv
import os
import threading
import time
from datetime import datetime


# ==================== TIMING LOGGER ====================
class TimingLogger:
    """
    Thread-safe CSV logger for per-cycle timing probes.
    One row per timed event: timestamp, cycle, candle_key, event_label, elapsed_sec.
    File rotates daily: timing_log_YYYY-MM-DD.csv  (stored in self.log_dir).
    Usage:
        tlog = TimingLogger(log_dir)
        tlog.start_cycle(cycle_count, candle_key)
        t0 = time.time(); ...; tlog.record("LTP_FETCH", t0)
    """
    _lock = threading.Lock()

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._cycle = 0
        self._candle_key = ""
        self._cycle_start = None

    def start_cycle(self, cycle: int, candle_key: str):
        self._cycle = cycle
        self._candle_key = candle_key
        self._cycle_start = time.time()
        self._write("CYCLE_START", 0.0, note="")

    def record(self, label: str, t0: float, note: str = ""):
        elapsed = round(time.time() - t0, 3)
        self._write(label, elapsed, note)
        print(f"[TIMING] {label:<45} {elapsed:>7.3f}s  {note}")
        return elapsed

    def record_since_cycle_start(self, label: str, note: str = ""):
        if self._cycle_start is None:
            return
        elapsed = round(time.time() - self._cycle_start, 3)
        self._write(label, elapsed, note)
        print(f"[TIMING] {label:<45} {elapsed:>7.3f}s (since cycle start)  {note}")

    def _write(self, label: str, elapsed: float, note: str):
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = os.path.join(self.log_dir, f"timing_log_{date_str}.csv")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with self._lock:
            file_exists = os.path.exists(path)
            with open(path, "a", newline="") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["timestamp", "cycle", "candle_key", "event", "elapsed_sec", "note"])
                w.writerow([now_str, self._cycle, self._candle_key, label, elapsed, note])
# ========================================================
