import time
import threading
import logging
import concurrent.futures
from queue import Queue
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

import broker_angle


# ==================== PARALLEL ORDER EXECUTOR CLASSES ====================

@dataclass
class OrderRequest:
    symbol: str
    side: str
    qty: int
    order_type: str = "MARKET"
    product_type: str = "INTRADAY"
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    trigger_price: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class OrderResult:
    """Represents the result of an order execution"""
    symbol: str
    success: bool
    order_id: Optional[str] = None
    filled: bool = False
    avg_price: float = 0.0
    filled_qty: int = 0
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class RateLimiter:
    """Token bucket rate limiter for API calls"""

    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self.lock = threading.Lock()

    def acquire(self):
        """Wait until a call can be made within rate limits"""
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < self.time_window]
            
            if len(self.calls) >= self.max_calls:
                oldest_call = self.calls[0]
                sleep_time = self.time_window - (now - oldest_call)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls 
                                 if now - call_time < self.time_window]
            
            self.calls.append(now)


class ParallelOrderExecutor:
    """Executes orders in parallel while respecting API rate limits"""

    def __init__(self, broker, session, 
                 max_workers: int = 5,
                 order_rate_limit: int = 10,
                 order_rate_window: float = 1.0,
                 status_rate_limit: int = 20,
                 status_rate_window: float = 1.0):
        self.broker = broker
        self.session = session
        self.max_workers = max_workers
        
        self.order_limiter = RateLimiter(order_rate_limit, order_rate_window)
        self.status_limiter = RateLimiter(status_rate_limit, status_rate_window)
        
        self.order_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.stop_flag = threading.Event()

    def _worker(self):
        """Worker thread that processes orders from the queue"""
        while not self.stop_flag.is_set():
            try:
                try:
                    order_req = self.order_queue.get(timeout=0.5)
                except:
                    continue
                
                if order_req is None:
                    break
                
                result = self._execute_single_order(order_req)
                self.result_queue.put(result)
                self.order_queue.task_done()

            except Exception as e:
                print(f"[Worker Error] {e}")
                logging.exception("Order worker crashed")
                self.result_queue.put(
                  OrderResult(
                  symbol="UNKNOWN",
                  success=False,
                  error=str(e)
                    )
                      )

    def _execute_single_order(self, order_req: OrderRequest) -> OrderResult:
        """Execute a single order with rate limiting"""
        try:
            self.order_limiter.acquire()
            
            print(
                f"\n[EXECUTOR] Sending order ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ "
                f"symbol={order_req.symbol} "
                f"side={order_req.side} "
                f"qty={order_req.qty} "
                f"type={order_req.order_type} "
                f"product={order_req.product_type} "
                f"price={order_req.price} "
                f"sl={order_req.stop_loss} "
                f"trigger={order_req.trigger_price}"
            )

            order_response = self.broker.place_order(
                session=self.session,
                symbol=order_req.symbol,
                side=order_req.side,
                qty=order_req.qty,
                order_type=order_req.order_type,
                product_type=order_req.product_type,
                price=order_req.price,
                stop_loss=order_req.stop_loss,
                trigger_price=order_req.trigger_price,
                wait_for_confirmation=False
            )
            
            print("\n[EXECUTOR] place_order raw response:")
            print(order_response)

            
            order_id = None
            if isinstance(order_response, dict):
                order_id = order_response.get("order_id")
                if order_response.get("status") == "error":
                    return OrderResult(
                        symbol=order_req.symbol,
                        success=False,
                        error=order_response.get("error", "Unknown error"),
                        metadata=order_req.metadata
                    )
            
            if not order_id:
                return OrderResult(
                    symbol=order_req.symbol,
                    success=False,
                    error="No order ID received",
                    metadata=order_req.metadata
                )
            
            # Fetch order status after brief delay
            time.sleep(0.5)
            try:
                # Resilience fix (error_fix_detail.md #6): this diagnostic orderBook()
                # call previously had no timeout - a hang here blocked this
                # OrderWorker thread indefinitely even though the order itself
                # already succeeded above. Bounded via the same shared executor/
                # timeout broker_angle.py uses for all its own SDK calls.
                future = broker_angle._BROKER_CALL_EXECUTOR.submit(self.session["obj"].orderBook)
                ob = future.result(timeout=broker_angle.BROKER_API_TIMEOUT_SECONDS)
                if isinstance(ob, dict) and ob.get("status") and ob.get("data"):
                    for o in ob["data"]:
                        if str(o.get("orderid")) == str(order_id):
                            print("\n[ANGEL RMS DECISION]")
                            print("status         :", o.get("orderstatus"))
                            print("rejectionreason:", o.get("rejectionreason"))
                            print("statusmessage  :", o.get("statusmessage"))
                            print("text           :", o.get("text"))
            except Exception as e:
                print("[EXECUTOR] orderBook error:", e)
                
            # PHASE A: fire-and-forget (do NOT wait for confirmation)
            return OrderResult(
                symbol=order_req.symbol,
                success=True,          
                order_id=order_id,
                filled=False,          
                avg_price=0.0,
                filled_qty=0,
                error=None,
                metadata=order_req.metadata
            )
          
        except Exception as e:
            return OrderResult(
                symbol=order_req.symbol,
                success=False,
                error=str(e),
                metadata=order_req.metadata
            )

    def start(self):
        """Start worker threads"""
        self.stop_flag.clear()
        self.workers = []
        
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker, name=f"OrderWorker-{i}")
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def stop(self):
        """Stop worker threads gracefully"""
        self.stop_flag.set()
        for _ in self.workers:
            self.order_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)
        self.workers = []

    def submit_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        """Submit multiple orders for parallel execution"""
        if not self.workers:
            self.start()
        
        for order in orders:
            self.order_queue.put(order)
        
        results = []
        for _ in orders:
            try:
              result = self.result_queue.get(timeout=30)
            except Exception:
              result = OrderResult(
                  symbol="UNKNOWN",
                  success=False,
                  error="Order execution timeout"
              )
            results.append(result)
        
        self.order_queue.join()
        return results


class OrderBatcher:
    """Helper class to batch orders by type for efficient parallel execution"""

    def __init__(self):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []

    def add_order(self, order: OrderRequest, order_category: str = "general"):
        """Add order to appropriate batch"""
        category = order_category.lower()
        
        if category == "buy" or order.side.upper() == "BUY":
            self.buy_orders.append(order)
        elif category in ["sell", "short"] or order.side.upper() == "SELL":
            self.sell_orders.append(order)
        elif category == "cover":
            self.cover_orders.append(order)
        elif category == "exit":
            self.exit_orders.append(order)
        else:
            if order.side.upper() == "BUY":
                self.buy_orders.append(order)
            else:
                self.sell_orders.append(order)

    def get_all_orders(self, priority: str = "exit_first") -> List[OrderRequest]:
        """Get all orders in optimal execution order"""
        if priority == "exit_first":
            return (self.exit_orders + self.cover_orders + 
                   self.sell_orders + self.buy_orders)
        else:
            return (self.buy_orders + self.sell_orders + 
                   self.cover_orders + self.exit_orders)

    def clear(self):
        """Clear all batches"""
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []

    def is_empty(self) -> bool:
        """Check if all batches are empty"""
        return not (self.buy_orders or self.sell_orders or 
                   self.cover_orders or self.exit_orders)

    def get_count(self) -> int:
        """Get total number of orders"""
        return (len(self.buy_orders) + len(self.sell_orders) + 
                len(self.cover_orders) + len(self.exit_orders))

# ==================== END PARALLEL ORDER EXECUTOR ====================
