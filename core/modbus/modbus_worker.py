"""Worker that polls Modbus devices using grouped batches and emits decoded values.

This module provides a small `ModbusWorker` that accepts canonical tag mappings
as produced by `core.modbus_mapping.map_tag_to_pymodbus`, groups due tags using
`core.modbus_scheduler.group_reads`, reads via a `core.modbus_client.ModbusClient`
and emits `tag_polled` signals when values arrive.

Also supports writing to Modbus devices with:
- Duty Cycle: Balance read/write operations (default 10:1 read:write ratio)
- Write Only Latest Value: Multiple writes to same address override previous values
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional

from .modbus_scheduler import group_reads
from .modbus_client import ModbusClient
from .modbus_write_queue import WriteQueueManager


class Signal:
    def __init__(self):
        self._handlers = []

    def connect(self, handler):
        self._handlers.append(handler)

    def emit(self, *args, **kwargs):
        for h in list(self._handlers):
            try:
                h(*args, **kwargs)
            except Exception:
                pass


class ModbusWorker:
    def __init__(self, client: ModbusClient, default_scan_ms: int = 1000, max_regs: int = 120, inter_request_delay_ms: int = 0, out_coils: int = 2000, in_coils: int = 2000, duty_cycle_ratio: int = 1, max_pending_writes: int = 100):
        self.client = client
        self.default_scan_ms = int(default_scan_ms)
        self.max_regs = int(max_regs)  # Respect device configuration
        self.out_coils = int(out_coils)  # Max read count for output coils (FC01)
        self.in_coils = int(in_coils)  # Max read count for input coils (FC02)
        # Inter-request delay from device timing config (in milliseconds, converted to seconds)
        self.inter_request_delay = (inter_request_delay_ms / 1000.0)  # Direct conversion, respect config value
        
        # Write queue and duty cycle configuration
        self.duty_cycle_ratio = int(duty_cycle_ratio) or 1  # 讀取:寫入 比例, 預設 1:1
        self.max_pending_writes = int(max_pending_writes) or 100
        
        # Initialize write queue with error handling
        try:
            self._write_queue = WriteQueueManager(
                max_pending_writes=self.max_pending_writes,
                max_writes_per_batch=5,
                diag_callback=None  # Will be set from diag_callback if available
            )
        except Exception as e:
            # print(f"[WARNING] Failed to initialize WriteQueueManager: {e}")
            self._write_queue = None
        
        self._read_count = 0  # 讀取計數 for duty cycle
        
        self.tag_polled = Signal()
        self._tags: List[Dict[str, Any]] = []
        self._next_due: Dict[int, float] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # print(f"[MODBUS_WORKER] Initialized with max_regs={self.max_regs}, out_coils={self.out_coils}, in_coils={self.in_coils}, inter_request_delay={self.inter_request_delay*1000:.0f}ms, duty_cycle_ratio={self.duty_cycle_ratio}:1")

    def add_tag(self, tag_item: Dict[str, Any]):
        # tag_item expected to be canonical mapping with 'scan_rate_ms' optional
        if tag_item not in self._tags:
            self._tags.append(tag_item)
            idx = id(tag_item)
            now = time.monotonic()
            self._next_due[idx] = now

    def remove_tag(self, tag_item: Dict[str, Any]):
        try:
            self._tags.remove(tag_item)
        except ValueError:
            pass
        try:
            self._next_due.pop(id(tag_item), None)
        except Exception:
            pass

    def start(self):
        """Start polling in a background thread with its own event loop."""
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._run_in_thread, daemon=True)
            self._thread.start()

    def stop(self):
        """Stop the background worker thread."""
        self._running = False
        if self._task:
            try:
                self._task.cancel()
            except Exception:
                pass
            self._task = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        if self._loop:
            try:
                self._loop.close()
            except Exception:
                pass
            self._loop = None

    def _run_in_thread(self):
        """Run the asyncio event loop in a background thread."""
        try:
            # Create a new event loop for this thread
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            
            # Create and run the polling task
            self._task = self._loop.create_task(self._run_loop())
            self._loop.run_until_complete(self._task)
        except Exception as e:
            # Log but don't crash
            # print(f"[ERROR] ModbusWorker thread error: {e}")
            pass
            if self._loop:
                self._loop.close()
            self._running = False

    async def _run_loop(self):
        while self._running:
            try:
                now = time.monotonic()
                due = []
                for t in list(self._tags):
                    idx = id(t)
                    nd = self._next_due.get(idx, now)
                    if nd <= now:
                        due.append(t)

                if due:
                    # Ensure client is connected before first read
                    if self.client._client is None:
                        try:
                            await self.client.connect_async()
                        except Exception as e:
                            # print(f"[WARNING] Failed to connect Modbus client: {e}")
                            await asyncio.sleep(0.5)
                            continue
                    
                    # Process batches sequentially with device-configured delay between requests
                    batches = group_reads(due, max_regs=self.max_regs)
                    
                    for batch_idx, batch in enumerate(batches):
                        if not self._running:
                            break
                        
                        try:
                            results = await self.client.read_batch_async(batch)
                            
                            # emit per-tag results and schedule next due
                            if results:
                                for r in results:
                                    tag = r.get('tag')
                                    val = r.get('value')
                                    # DEBUG: log batch results
                                    tree_path = tag.get('tree_path', tag.get('name', 'Unknown'))
                                    # if 'Data' in tree_path or batch_idx == 7:
                                    #     print(f"[BATCH_RESULT] batch={batch_idx} tag={tree_path} addr={tag.get('address')} value_type={type(val).__name__} value_len={len(val) if isinstance(val, (list, tuple)) else 'N/A'}")
                                    
                                    self.tag_polled.emit(tag, val)
                                    idx = id(tag)
                                    scan = int(tag.get('scan_rate_ms') or self.default_scan_ms)
                                    self._next_due[idx] = time.monotonic() + (scan / 1000.0)
                            
                            # Increment read counter for duty cycle
                            self._read_count += 1
                            
                            # Check if we should execute write operations (Duty Cycle)
                            # duty_cycle_ratio = 10 means: execute write after every 10 reads
                            try:
                                if self._read_count >= self.duty_cycle_ratio and hasattr(self, '_write_queue') and self._write_queue and not self._write_queue.is_empty():
                                    await self._execute_pending_writes()
                                    self._read_count = 0  # Reset counter after executing writes
                            except Exception as e:
                                # print(f"[ERROR] Write queue execution failed: {e}")
                                # Continue with reading, don't break the loop
                                pass
                            
                            # Apply inter_request_delay AFTER receiving response, BEFORE next request
                            # This gives device time to recover between ADU exchanges
                            if batch_idx < len(batches) - 1:
                                await asyncio.sleep(self.inter_request_delay)
                                
                        except Exception as e:
                            # Log batch read failure but continue with next batch
                            batch_info = f"addr={batch.get('start')} count={batch.get('count')} tags={len(batch.get('tags', []))}"
                            # print(f"[WARNING] Batch {batch_idx} read failed ({batch_info}): {e}")
                            # Wait longer after failed batch to let device recover
                            await asyncio.sleep(1.0)
                
                # Also check for writes even if no reads are due (for write-only scenarios)
                try:
                    if hasattr(self, '_write_queue') and self._write_queue and not self._write_queue.is_empty():
                        if self.client._client is None:
                            try:
                                await self.client.connect_async()
                            except Exception:
                                pass
                        if self.client._client is not None:
                            await self._execute_pending_writes()
                except Exception as e:
                    # print(f"[ERROR] Standalone write execution failed: {e}")
                    pass

                # sleep before next cycle
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                # print(f"[ERROR] RuntimeLoop exception: {e}")
                await asyncio.sleep(0.5)
    
    async def _execute_pending_writes(self):
        """執行待執行的寫入操作 (Duty Cycle 觸發)"""
        try:
            pending_writes = self._write_queue.get_pending_writes()
            if not pending_writes:
                return
            
            for write_info in pending_writes:
                try:
                    address = write_info.get('address')
                    fc = write_info.get('fc')
                    value = write_info.get('value')
                    tag_info = write_info.get('tag_info', {})
                    
                    # Call write_async on the client with tag_info for proper byte_order handling
                    result = await self.client.write_async(address, value, fc, tag_info=tag_info)
                    
                    # Mark as completed
                    self._write_queue.mark_completed(address, fc)
                    
                except Exception as e:
                    # Mark as failed but keep in queue for retry
                    self._write_queue.mark_failed(address, fc, str(e))
        
        except Exception as e:
            # print(f"[ERROR] _execute_pending_writes: {e}")
            pass


def create_worker_for_client(client: ModbusClient, default_scan_ms: int = 1000, max_regs: int = 120, inter_request_delay_ms: int = 0) -> ModbusWorker:
    return ModbusWorker(client, default_scan_ms=default_scan_ms, max_regs=max_regs, inter_request_delay_ms=inter_request_delay_ms)

# Backwards compatibility: some modules import `AsyncPoller` and `Signal`.
AsyncPoller = ModbusWorker
__all__ = ["Signal", "ModbusWorker", "create_worker_for_client", "AsyncPoller"]
