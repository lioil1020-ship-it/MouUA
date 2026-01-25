"""
Virtual scrolling table model for Monitor display.
Supports displaying large number of tags without creating all QTableWidgetItem objects.
"""
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex
from PyQt6.QtGui import QColor
from typing import Dict, List, Tuple, Any, Optional


class VirtualMonitorTableModel(QAbstractTableModel):
    """
    Table model for Monitor display with virtual scrolling support.
    
    Only maintains visible rows in memory. Fetches data from buffer on demand.
    Supports displaying 839+ tags without performance degradation.
    """
    
    def __init__(self, buffer_ref=None):
        super().__init__()
        self.buffer_ref = buffer_ref  # Reference to ModbusDataBuffer
        self.all_tags = []  # All tag paths and their metadata: [(tag_path, data_type, access), ...]
        self.row_offset = 0  # Current scroll position (first visible row index)
        self.visible_rows = 30  # Number of rows to keep in memory
        self.columns = 7
        
        # Cache for the visible rows only
        self.visible_data = {}  # {row: [tag_name, data_type, access, value, timestamp, quality, count]}
    
    def set_all_tags(self, tags: List[Tuple[str, str, str]]):
        """
        Set the complete list of tags to display.
        
        Args:
            tags: List of (tag_path, data_type, client_access) tuples
        """
        self.beginResetModel()
        self.all_tags = tags
        self.row_offset = 0
        self.visible_data = {}
        self.endResetModel()
        self.update_visible_rows()
    
    def rowCount(self, parent=QModelIndex()) -> int:
        """Total number of rows (all tags)."""
        return len(self.all_tags)
    
    def columnCount(self, parent=QModelIndex()) -> int:
        """Number of columns."""
        return self.columns
    
    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole) -> Any:
        """
        Get data for a cell.
        
        Virtual scrolling: only maintains dynamic data (values) in cache for visible rows.
        Static data (tag names, types, access) is always available from all_tags.
        """
        if not index.isValid():
            return None
        
        row = index.row()
        col = index.column()
        
        # Validate row
        if row < 0 or row >= len(self.all_tags):
            return None
        
        # Get tag metadata (always available)
        tag_path, data_type, access = self.all_tags[row]
        
        # Column 0-2: Always available from metadata
        if col == 0:
            if role == Qt.ItemDataRole.DisplayRole:
                return tag_path
        elif col == 1:
            if role == Qt.ItemDataRole.DisplayRole:
                return data_type
        elif col == 2:
            if role == Qt.ItemDataRole.DisplayRole:
                return access
        
        # Column 3-6: Dynamic data (value, timestamp, quality, update_count)
        # Only load these if row is in visible range
        visible_row = row - self.row_offset
        
        # Check if row is visible
        if self.row_offset <= row < self.row_offset + self.visible_rows:
            if visible_row not in self.visible_data:
                self._load_row_data(visible_row, row)
            
            row_data = self.visible_data.get(visible_row)
            if row_data:
                # Map column to dynamic data index
                # visible_data[visible_row] = [value, timestamp, quality, update_count]
                # col 3 -> index 0 (value)
                # col 4 -> index 1 (timestamp)
                # col 5 -> index 2 (quality)
                # col 6 -> index 3 (update_count)
                dynamic_idx = col - 3
                if 0 <= dynamic_idx < len(row_data):
                    if role == Qt.ItemDataRole.DisplayRole:
                        return row_data[dynamic_idx]
                    elif role == Qt.ItemDataRole.TextAlignmentRole:
                        return Qt.AlignmentFlag.AlignCenter
        
        return None
    
    def headerData(self, section: int, orientation: Qt.Orientation, role=Qt.ItemDataRole.DisplayRole) -> Any:
        """Get header text."""
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        
        if orientation == Qt.Orientation.Horizontal:
            headers = ["Tag Name", "Data Type", "ClientAccess", "Value", "Timestamp", "Quality", "Update Count"]
            return headers[section] if section < len(headers) else None
        
        return str(section + 1)  # Row numbers
    
    def _load_row_data(self, visible_row: int, absolute_row: int):
        """
        Load dynamic data for a specific row from buffer.
        Only caches columns 3-6 (value, timestamp, quality, update_count).
        Columns 0-2 (tag_path, data_type, access) come from all_tags.
        
        Args:
            visible_row: Index in visible rows (0-29)
            absolute_row: Absolute row index in all tags
        """
        if absolute_row < 0 or absolute_row >= len(self.all_tags):
            return
        
        tag_path, data_type, access = self.all_tags[absolute_row]
        
        # Try to get value from buffer
        value = ""
        timestamp = ""
        quality = ""
        update_count = ""
        
        if self.buffer_ref:
            try:
                # Use get_tag_data() to get complete tag data including value, timestamp, quality, etc.
                tag_data = self.buffer_ref.get_tag_data(tag_path)
                if tag_data:
                    value = str(tag_data.get('value', '')) if tag_data.get('value') is not None else ""
                    # Format timestamp if available
                    ts = tag_data.get('timestamp')
                    if ts:
                        from datetime import datetime
                        try:
                            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            timestamp = str(ts)
                    quality = str(tag_data.get('quality', 'Unknown'))
                    update_count = str(tag_data.get('update_count', 0))
                else:
                    # DEBUG: tag not found in buffer
                    import logging
                    if absolute_row < 10 or absolute_row % 50 == 0:
                        logging.debug(f"[VIRTUAL_LOAD] row={absolute_row} tag={tag_path} NOT FOUND in buffer")
                    # Return empty - tag not in buffer yet
            except Exception as e:
                import logging
                logging.error(f"Error loading row data for {tag_path}: {e}")
        
        # Store dynamic data in cache (columns 3-6)
        # Index mapping: [0]=value, [1]=timestamp, [2]=quality, [3]=update_count
        self.visible_data[visible_row] = [
            value,
            timestamp,
            quality,
            update_count
        ]
    
    def update_visible_rows(self, first_visible_row: int = None):
        """
        Update which rows are visible (called when user scrolls).
        
        Args:
            first_visible_row: Index of first visible row (from scroll position)
        """
        if first_visible_row is not None:
            self.row_offset = first_visible_row
        
        # Clamp offset
        max_offset = max(0, len(self.all_tags) - self.visible_rows)
        self.row_offset = min(self.row_offset, max_offset)
        
        # DEBUG
        import logging
        logging.info(f"[UPDATE_VISIBLE] offset={self.row_offset}, total_tags={len(self.all_tags)}")
        
        # Clear old cache
        self.visible_data = {}
        
        # Load visible rows
        for visible_idx in range(self.visible_rows):
            absolute_idx = self.row_offset + visible_idx
            if absolute_idx < len(self.all_tags):
                self._load_row_data(visible_idx, absolute_idx)
        
        # Notify table to refresh visible items
        # Emit dataChanged for the visible range
        start_index = self.index(self.row_offset, 0)
        end_index = self.index(min(self.row_offset + self.visible_rows - 1, len(self.all_tags) - 1), self.columns - 1)
        logging.info(f"[UPDATE_VISIBLE] emitting dataChanged from row {self.row_offset} to {min(self.row_offset + self.visible_rows - 1, len(self.all_tags) - 1)}")
        self.dataChanged.emit(start_index, end_index)
    
    def update_tag_value(self, tag_path: str, value: Any, timestamp: str, quality: str, update_count: int):
        """
        Update a tag's value in the visible rows.
        
        Args:
            tag_path: Full tag path (e.g., "Channel1.Device1.Set.WIRE")
            value: New value
            timestamp: Timestamp string
            quality: Quality string
            update_count: Update count
        """
        if not self.buffer_ref:
            return
        
        # Find which absolute row this tag is in
        for abs_row, (tag, _, _) in enumerate(self.all_tags):
            if tag == tag_path:
                # Check if it's in visible range
                if self.row_offset <= abs_row < self.row_offset + self.visible_rows:
                    visible_row = abs_row - self.row_offset
                    # Store only dynamic data (columns 3-6)
                    # [0]=value, [1]=timestamp, [2]=quality, [3]=update_count
                    self.visible_data[visible_row] = [
                        str(value) if value is not None else "",
                        timestamp,
                        quality,
                        str(update_count)
                    ]
                    # Notify model of change - emit for all 4 dynamic columns
                    start_index = self.index(abs_row, 3)  # Value column
                    end_index = self.index(abs_row, 6)    # Update Count column
                    self.dataChanged.emit(start_index, end_index)
                break
