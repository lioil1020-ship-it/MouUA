"""Data validation and normalization utilities for controllers.

This module provides helper functions for normalizing user inputs and 
ensuring consistent data formats across channels, devices, and tags.
"""

from typing import Any, Dict, Optional


# Size mapping for address increments by data type
# Shared constant so other code/tests can rely on same sizing rules.
SIZE_MAP = {
    "Boolean": 1,
    "Boolean(Array)": 1,
    "Char": 1,
    "Byte": 1,
    "Short": 1,
    "Word": 1,
    "Int": 1,           # 16-bit signed integer (1 register)
    "DInt": 2,          # 32-bit signed integer (2 registers)
    "Long": 2,          # 32-bit signed (2 registers, NOT 4)
    "DWord": 2,         # 32-bit unsigned (2 registers)
    "Float": 2,         # 32-bit IEEE float (2 registers)
    "Double": 4,        # 64-bit IEEE float (4 registers)
    "Real": 2,          # 32-bit IEEE float (2 registers)
    "BCD": 1,           # Binary Coded Decimal (1 register)
    "LBCD": 2,          # Long Binary Coded Decimal (2 registers)
    "LLong": 4,         # 64-bit signed (4 registers)
    "QWord": 8,         # 128-bit unsigned (8 registers)
    "String": 6,        # String (6 registers = 12 bytes)
}


def to_numeric_flag(v: Any) -> Any:
    """Normalize enable/disable/boolean-like values to integer 1/0 where possible.
    
    Converts common human-friendly strings to standardized numeric flags:
    - True, "enable", "enabled", "1", "on" -> 1
    - False, "disable", "disabled", "0", "off" -> 0
    - Numbers stay as ints
    - Other values returned as-is
    """
    try:
        if v is None:
            return v
        if isinstance(v, bool):
            return 1 if v else 0
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return s
        low = s.lower()
        if low in ("enable", "enabled", "true", "1", "on"):
            return 1
        if low in ("disable", "disabled", "false", "0", "off"):
            return 0
        # try integer conversion
        try:
            return int(float(s))
        except Exception:
            return s
    except Exception:
        return v


def normalize_dict_flags(d: Any) -> Any:
    """Apply to_numeric_flag to all values in a dict."""
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        out[k] = to_numeric_flag(v)
    return out


def is_tcp_like_driver(driver_type: Optional[str]) -> bool:
    """Check if driver type indicates TCP/Ethernet communication."""
    try:
        drv_str = str(driver_type or '').lower()
        return any(x in drv_str for x in ("over tcp", "ethernet", "tcp"))
    except Exception:
        return False


def parse_adapter_string(adapter_str: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse adapter string and extract interface name and IP.
    
    Handles formats like:
    - "Interface Name (192.168.1.1)"
    - "192.168.1.1 - Interface Name"
    
    Returns: (interface_name, ip_address)
    """
    if not adapter_str or not isinstance(adapter_str, str):
        return None, None
    
    # Format: "Interface Name (IP)"
    if '(' in adapter_str and adapter_str.endswith(')'):
        try:
            name_part, ip_part = adapter_str.rsplit('(', 1)
            return name_part.strip(), ip_part.strip(') ').strip()
        except Exception:
            return adapter_str, None
    
    # Format: "IP - Interface Name"
    if ' - ' in adapter_str:
        try:
            left, right = adapter_str.split(' - ', 1)
            # If left looks like IP (contains dots), assume IP - Name format
            if left.count('.') == 3:
                return right.strip(), left.strip()
            else:
                return left.strip(), right.strip()
        except Exception:
            return adapter_str, None
    
    # Plain string
    return adapter_str, None


def format_adapter_with_ip(name: str, ip: Optional[str] = None) -> str:
    """Format adapter name and IP into standard display format."""
    if not name:
        name = "Default"
    if ip:
        return f"{name} ({ip})"
    return name


__all__ = [
    "SIZE_MAP",
    "to_numeric_flag",
    "normalize_dict_flags",
    "is_tcp_like_driver",
    "parse_adapter_string",
    "format_adapter_with_ip",
]
