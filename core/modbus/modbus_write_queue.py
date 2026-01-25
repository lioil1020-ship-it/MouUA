"""
Write Queue Manager for Modbus Operations
Implements "Write Only Latest Value" strategy and batch write optimization
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional, List
import threading


class WriteQueueManager:
    """
    管理 Modbus 寫入隊列
    
    特性:
    1. Write Only Latest Value: 同一點位只保留最新值
    2. Batch execution: 批次執行減少網絡開銷
    3. Thread-safe: 支援多線程訪問
    4. Diagnostics: 詳細的診斷日誌
    """
    
    def __init__(self, max_pending_writes: int = 100, max_writes_per_batch: int = 10, diag_callback: Optional[Any] = None):
        """
        初始化寫入隊列管理器
        
        Args:
            max_pending_writes: 最大待執行寫入數
            max_writes_per_batch: 單個批次最多寫入數
            diag_callback: 診斷回調函數
        """
        self.max_pending_writes = int(max_pending_writes)
        self.max_writes_per_batch = int(max_writes_per_batch)
        self.diag_callback = diag_callback
        
        # 使用字典存儲 {(address, fc): value}
        # 這實現了 "Write Only Latest Value" - 新值自動覆蓋舊值
        self._queue: Dict[tuple, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._stats = {
            'enqueued': 0,
            'executed': 0,
            'overwritten': 0,
            'failed': 0,
        }
    
    def enqueue(self, address: int, fc: int, value: Any, tag_info: Optional[Dict] = None) -> bool:
        """
        加入寫入請求
        
        如果相同的 (address, fc) 已存在，則用新值覆蓋 (Write Only Latest Value)
        
        Args:
            address: Modbus 地址
            fc: 函數碼 (5, 6, 15, 16)
            value: 要寫入的值
            tag_info: 可選的標籤資訊字典
        
        Returns:
            True 如果加入成功，False 如果隊列已滿
        """
        with self._lock:
            # 檢查隊列是否滿
            if len(self._queue) >= self.max_pending_writes and (address, fc) not in self._queue:
                if self.diag_callback:
                    try:
                        self.diag_callback(f"WRITE_QUEUE_FULL: max={self.max_pending_writes}")
                    except Exception:
                        pass
                return False
            
            key = (address, fc)
            
            # 檢查是否覆蓋現有值
            if key in self._queue:
                old_value = self._queue[key].get('value')
                self._stats['overwritten'] += 1
                if self.diag_callback:
                    try:
                        self.diag_callback(f"WRITE_QUEUE_OVERRIDE: addr={address} fc={fc} old={old_value} new={value}")
                    except Exception:
                        pass
            else:
                self._stats['enqueued'] += 1
                if self.diag_callback:
                    try:
                        self.diag_callback(f"WRITE_QUEUE_ENQUEUE: addr={address} fc={fc} value={value}")
                    except Exception:
                        pass
            
            # 存儲或更新值
            self._queue[key] = {
                'address': address,
                'fc': fc,
                'value': value,
                'tag_info': tag_info or {},
                'enqueue_time': time.monotonic(),
            }
            
            return True
    
    def get_pending_writes(self, max_count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        獲取待執行的寫入列表 (最多 max_writes_per_batch)
        
        Args:
            max_count: 最多返回多少個 (預設: max_writes_per_batch)
        
        Returns:
            待執行寫入的列表 [{address, fc, value, tag_info, ...}]
        """
        with self._lock:
            if not self._queue:
                return []
            
            max_count = max_count or self.max_writes_per_batch
            max_count = min(max_count, len(self._queue))
            
            # 提取前 max_count 項
            writes = []
            keys_to_remove = []
            
            for idx, (key, write_info) in enumerate(self._queue.items()):
                if idx >= max_count:
                    break
                writes.append(write_info)
                keys_to_remove.append(key)
            
            # 標記為已提取，但不移除 (execute 後移除)
            # 這樣如果執行失敗可以重試
            write_ids = [f"{w['address']}_{w['fc']}" for w in writes]
            
            return writes
    
    def mark_completed(self, address: int, fc: int) -> bool:
        """
        標記寫入操作為已完成 (移除隊列)
        
        Args:
            address: Modbus 地址
            fc: 函數碼
        
        Returns:
            True 如果移除成功
        """
        with self._lock:
            key = (address, fc)
            if key in self._queue:
                del self._queue[key]
                self._stats['executed'] += 1
                if self.diag_callback:
                    try:
                        self.diag_callback(f"WRITE_COMPLETED: addr={address} fc={fc}")
                    except Exception:
                        pass
                return True
            return False
    
    def mark_failed(self, address: int, fc: int, error: str) -> bool:
        """
        標記寫入操作為失敗
        
        Args:
            address: Modbus 地址
            fc: 函數碼
            error: 錯誤信息
        
        Returns:
            True 如果操作成功
        """
        with self._lock:
            self._stats['failed'] += 1
            if self.diag_callback:
                try:
                    self.diag_callback(f"WRITE_FAILED: addr={address} fc={fc} error={error}")
                except Exception:
                    pass
            return True
    
    def is_empty(self) -> bool:
        """檢查隊列是否為空"""
        with self._lock:
            return len(self._queue) == 0
    
    def get_count(self) -> int:
        """獲取隊列中待執行的寫入數"""
        with self._lock:
            return len(self._queue)
    
    def clear(self) -> None:
        """清空隊列"""
        with self._lock:
            self._queue.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計信息"""
        with self._lock:
            return {
                **self._stats,
                'pending_count': len(self._queue),
            }
    
    def __str__(self) -> str:
        """字符串表示"""
        with self._lock:
            count = len(self._queue)
            return f"WriteQueueManager(pending={count}, enqueued={self._stats['enqueued']}, executed={self._stats['executed']})"
