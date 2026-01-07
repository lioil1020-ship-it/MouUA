"""Small helpers to coerce values for OPC UA update.

This module contains minimal helpers used by `OPC_UA.py` to coerce list/tuple
values into scalar UA-compatible values based on expected dtype strings.
"""
from typing import Any


def coerce_value_for_dtype(value: Any, dtype: str):
    """Coerce `value` (possibly list/tuple) into a single scalar or leave as-is.

    - If dtype contains 'float' or 'double' and value is a list with 1 element,
      return float(elem).
    - If dtype contains 'int' or 'word' and value is a list with 1 element,
      return int(elem).
    - Otherwise return value unchanged.
    """
    try:
        l_dtype = (dtype or "").lower()
        if isinstance(value, (list, tuple)) and len(value) == 1:
            v = value[0]
            if v is None:
                return None
            if 'float' in l_dtype or 'double' in l_dtype:
                try:
                    return float(v)
                except Exception:
                    return v
            if 'int' in l_dtype or 'word' in l_dtype or 'long' in l_dtype:
                try:
                    return int(v)
                except Exception:
                    return v
        return value
    except Exception:
        return value
