import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from queue import Queue
from typing import Any, Dict, List, Optional


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
    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self.lock = threading.Lock()
    
    def acquire(self):
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
        try:
            self.order_limiter.acquire()
            
            placed_at_ts = datetime.now()
            print(
                f"\n[DEBUG-ORDER] [{placed_at_ts.strftime('%H:%M:%S.%f')[:-3]}] Submitting {order_req.side} order for "
                f"symbol={order_req.symbol} "
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
                if order_id:
                    received_at_ts = datetime.now()
                    print(f"[DEBUG-ORDER] [{received_at_ts.strftime('%H:%M:%S.%f')[:-3]}] Received order_id {order_id} for {order_req.symbol}")
                
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
            
            #  PHASE A: fire-and-forget  return immediately, reconciler checks status
            # orderBook() is intentionally NOT called here  reconcile thread handles it
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
        self.stop_flag.clear()
        self.workers = []
        
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker, name=f"OrderWorker-{i}")
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
    
    def stop(self):
        self.stop_flag.set()
        for _ in self.workers:
            self.order_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)
        self.workers = []

    def submit_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        if not self.workers:
            self.start()
        
        for order in orders:
            self.order_queue.put(order)
        
        results = []
        for i, order in enumerate(orders):        #  enumerate so we know which order
            try:
                result = self.result_queue.get(timeout=10)  #  reduced from 30s to 10s
            except Exception:
                print(f"[TIMEOUT] Order {i+1}/{len(orders)} timed out: "
                    f"{order.symbol} {order.side} {order.qty}")
                result = OrderResult(
                    symbol=order.symbol,           #  now we know the symbol
                    success=False,
                    error="Order execution timeout",
                    metadata=order.metadata        #  preserve metadata for pending_orders
                )
                
            results.append(result)
        
        self.order_queue.join()
        return results
    
class OrderBatcher:    
    def __init__(self ,tlog=None):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
        self.tlog = tlog
    
    def add_order(self, order: OrderRequest, order_category: str = "general"):
        t0 = time.time()
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
        elapsed = time.time() - t0

        print(f"[TRACE] ADD_ORDER {order.symbol}: {elapsed:.6f}s")   
        if self.tlog:  #  guard added
            self.tlog.record("order batcher add order timing", t0, note="order batcher add order")    
    
    def get_all_orders(self, priority: str = "exit_first") -> List[OrderRequest]:
        if priority == "exit_first":
            return (self.exit_orders + self.cover_orders + 
                   self.sell_orders + self.buy_orders)
        else:
            return (self.buy_orders + self.sell_orders + 
                   self.cover_orders + self.exit_orders)
    
    def clear(self):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
    
    def is_empty(self) -> bool:
        return not (self.buy_orders or self.sell_orders or 
                   self.cover_orders or self.exit_orders)
    
    def get_count(self) -> int:
        return (len(self.buy_orders) + len(self.sell_orders) + 
                len(self.cover_orders) + len(self.exit_orders))

# ==================== END PARALLEL ORDER EXECUTOR ====================
