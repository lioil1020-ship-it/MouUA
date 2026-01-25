"""Serialization and export utilities for project configurations.

This module handles exporting the project tree to various formats:
- JSON (complete project structure)
- CSV (device tags)
"""

import csv
import json
import os

from core.config import GROUP_SEPARATOR
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional


def normalize_address_number(addr: Any) -> int:
    """Extract numeric address value for sorting."""
    try:
        if addr is None:
            return 0
        s = str(addr)
        m = re.search(r"(\d+)", s)
        if m:
            return int(m.group(1))
        return 0
    except Exception:
        return 0


def is_array_tag(tag_dict: Dict[str, Any]) -> bool:
    """Check if a tag represents an array type."""
    try:
        # Check metadata
        meta = tag_dict.get('__meta__')
        if isinstance(meta, dict) and meta.get('is_array'):
            return True
        
        # Check data type
        dt = str(tag_dict.get('data_type') or '')
        if 'array' in dt.lower():
            return True
        
        # Check address format
        addr = str(tag_dict.get('address') or '')
        if re.search(r"\[\d+\]", addr):
            return True
        
        # Check tag name
        name = str(tag_dict.get('name') or '')
        if 'array' in name.lower():
            return True
        
        return False
    except Exception:
        return False


def export_tags_to_csv(device_item, filepath: str) -> None:
    """Export tags under device_item to CSV format.
    
    The CSV format matches the template in CPM-12D.csv with columns:
    Tag Name, Address, Data Type, Respect Data Type, Client Access, Scan Rate,
    Scaling, Raw Low, Raw High, Scaled Low, Scaled High, Scaled Data Type,
    Clamp Low, Clamp High, Eng Units, Description, Negate Value
    
    Groups are represented by dot-separated names (Group1.Group2.TagName).
    
    Args:
        device_item: QTreeWidgetItem for the device
        filepath: Path where CSV will be written
    """
    if not filepath:
        return
    
    try:
        from PyQt6.QtCore import Qt
    except ImportError:
        return
    
    # Collect all tags by walking the tree
    rows = []
    
    def walk_tags(parent, prefix=None):
        """Recursively walk tree and collect tag rows."""
        for i in range(parent.childCount()):
            child = parent.child(i)
            try:
                item_type = child.data(0, Qt.ItemDataRole.UserRole)
            except Exception:
                item_type = None
            
            if item_type == 'Tag':
                try:
                    name = child.text(0) or ''
                    # Build qualified name with group path using GROUP_SEPARATOR
                    if prefix:
                        qname = f"{prefix}{GROUP_SEPARATOR}{name}"
                    else:
                        qname = name
                    
                    addr = child.data(4, Qt.ItemDataRole.UserRole)
                    dtype = child.data(2, Qt.ItemDataRole.UserRole)
                    desc = child.data(1, Qt.ItemDataRole.UserRole) or ''
                    access = child.data(3, Qt.ItemDataRole.UserRole)
                    scan_rate = child.data(5, Qt.ItemDataRole.UserRole)
                    scaling = child.data(6, Qt.ItemDataRole.UserRole)
                    
                    if not isinstance(scaling, dict):
                        scaling = None
                    
                    row = {
                        'name': qname,
                        'address': addr,
                        'data_type': dtype,
                        'description': desc,
                        'access': access,
                        'scan_rate': scan_rate,
                    }
                    
                    # Store metadata
                    try:
                        meta = child.data(7, Qt.ItemDataRole.UserRole)
                        if isinstance(meta, dict):
                            row['__meta__'] = meta
                    except Exception:
                        pass
                    
                    # Add scaling fields if present and type is not 'None'
                    if scaling and scaling.get('type') != 'None':
                        row['scaling'] = scaling.get('type', '')
                        row['raw_low'] = scaling.get('raw_low')
                        row['raw_high'] = scaling.get('raw_high')
                        row['scaled_low'] = scaling.get('scaled_low')
                        row['scaled_high'] = scaling.get('scaled_high')
                        row['scaled_type'] = scaling.get('scaled_type', '')
                        row['clamp_low'] = scaling.get('clamp_low')
                        row['clamp_high'] = scaling.get('clamp_high')
                        row['units'] = scaling.get('units')
                        row['negate'] = scaling.get('negate')
                        row['has_scaling'] = True
                    else:
                        row['has_scaling'] = False
                    
                    row['respect_data_type'] = 1
                    rows.append(row)
                except Exception:
                    pass
            else:
                # Treat as group/container
                subname = child.text(0) or ''
                new_prefix = f"{prefix}{GROUP_SEPARATOR}{subname}" if prefix and subname else (subname or prefix)
                walk_tags(child, new_prefix)
    
    walk_tags(device_item, None)
    
    # Sort rows: non-array tags first, then array tags, both by address
    scalars = [r for r in rows if not is_array_tag(r)]
    arrays = [r for r in rows if is_array_tag(r)]
    
    def sort_key(row):
        addr_num = (row.get('__meta__', {}).get('addrnum') 
                   if isinstance(row.get('__meta__'), dict) 
                   else normalize_address_number(row.get('address')))
        return (addr_num, row.get('name', ''))
    
    scalars.sort(key=sort_key)
    arrays.sort(key=sort_key)
    rows = scalars + arrays
    
    # Write CSV
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Always include all fieldnames in header
    fieldnames = [
        'Tag Name', 'Address', 'Data Type', 'Respect Data Type', 'Client Access', 'Scan Rate',
        'Scaling', 'Raw Low', 'Raw High', 'Scaled Low', 'Scaled High', 'Scaled Data Type',
        'Clamp Low', 'Clamp High', 'Eng Units', 'Description', 'Negate Value'
    ]
    
    try:
        with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                # Convert access format: Read/Write -> R/W, Read Only -> RO
                access = row.get('access', 'R/W')
                if access == 'Read/Write':
                    access = 'R/W'
                elif access == 'Read Only':
                    access = 'RO'
                
                # Convert data type: Word(Array) -> Word Array, etc.
                data_type = row.get('data_type', '')
                data_type = data_type.replace('(Array)', ' Array')
                
                # Remove leading zeros from address (e.g., 000103 -> 103, but keep 400000 [25])
                address = row.get('address', '')
                if address:
                    # Handle array format like "400000 [25]"
                    if '[' in address:
                        parts = address.split(' [')
                        addr_part = parts[0].lstrip('0') or '0'
                        address = f"{addr_part} [{parts[1]}"
                    else:
                        address = address.lstrip('0') or '0'
                
                # When scaling type is 'None', don't export scaling-related content
                if row.get('has_scaling', False):
                    scaling_content = row.get('scaling', '')
                    raw_low = row.get('raw_low', '')
                    raw_high = row.get('raw_high', '')
                    scaled_low = row.get('scaled_low', '')
                    scaled_high = row.get('scaled_high', '')
                    scaled_type = row.get('scaled_type', '')
                    clamp_low = row.get('clamp_low', '')
                    clamp_high = row.get('clamp_high', '')
                    units = row.get('units', '')
                    negate = row.get('negate', '')
                else:
                    scaling_content = ''
                    raw_low = ''
                    raw_high = ''
                    scaled_low = ''
                    scaled_high = ''
                    scaled_type = ''
                    clamp_low = ''
                    clamp_high = ''
                    units = ''
                    negate = ''
                
                out_row = {
                    'Tag Name': row.get('name', ''),
                    'Address': address,
                    'Data Type': data_type,
                    'Respect Data Type': row.get('respect_data_type', 1),
                    'Client Access': access,
                    'Scan Rate': row.get('scan_rate', ''),
                    'Scaling': scaling_content,
                    'Raw Low': raw_low,
                    'Raw High': raw_high,
                    'Scaled Low': scaled_low,
                    'Scaled High': scaled_high,
                    'Scaled Data Type': scaled_type,
                    'Clamp Low': clamp_low,
                    'Clamp High': clamp_high,
                    'Eng Units': units,
                    'Description': row.get('description', ''),
                    'Negate Value': negate
                }
                writer.writerow({k: (v if v is not None else '') for k, v in out_row.items()})
    except Exception:
        pass


__all__ = [
    "normalize_address_number",
    "is_array_tag",
    "export_tags_to_csv",
]
