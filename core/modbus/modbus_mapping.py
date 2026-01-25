"""Convert project `role` structures (Channel/Device/Tag) to a pymodbus-friendly canonical dict.

This module provides:
- parse_address(addr_str, zero_based=False) -> (address_type, zero_based_address)
- map_tag_to_pymodbus(tag_dict, device_dict=None, channel_dict=None) -> canonical dict

The implementation is intentionally conservative and returns a plain dict
that a higher-level runtime/scheduler can use to call pymodbus client methods.
"""
from typing import Tuple, Optional, Dict, Any
import re
import logging

from ..config import MODBUS_ADDRESS_RANGES

logger = logging.getLogger(__name__)


def _check_address_range(idx: int, addr_ranges=None) -> Optional[Tuple[str, int]]:
    """Check if address falls within any range and return (type, offset) or None."""
    if addr_ranges is None:
        addr_ranges = MODBUS_ADDRESS_RANGES
    for addr_range in addr_ranges:
        if addr_range["min"] <= idx <= addr_range["max"]:
            return addr_range["type"], addr_range["offset"]
    return None


def map_endian_names_to_constants(byte_order: str | None, word_order: str | None, bit_order: str | None, dword_order: str | None = None, treat_longs_as_decimals: bool | None = None) -> Dict[str, str | bool]:
    """Map human-friendly endianness strings to canonical names.

    Handles both string names and numeric codes:
    - byte_order: 1=Enabled(Modbus Big-Endian)/0=Disabled(Intel Little-Endian)
      BUT: The actual behavior shows that Enabled(1) needs little-endian byte swap
    - word_order: 1=First Word Low/0=First Word High (for 32-bit values)
    - dword_order: 1=First DWord Low/0=First DWord High (for 64-bit values)
    - bit_order: 1=MSB/0=LSB (Modicon Bit Order)
    - treat_longs_as_decimals: True/False for encoding 64-bit as 0-99999999 range
    
    NOTE: Device configuration uses inverse logic for byte_order
    """
    res = {
        "byte_order": "big",
        "word_order": "low_high",
        "dword_order": "low_high",
        "bit_order": "lsb",
        "treat_longs_as_decimals": False,
    }
    try:
        if byte_order is not None:
            s = str(byte_order).lower()
            # Naming: Enable(1) = big-endian (Modbus standard, network byte order)
            #         Disable(0) = little-endian (Intel format)
            if s == "0" or "disable" in s or "little" in s or "intel" in s:
                res["byte_order"] = "little"
            else:  # 1, "enable", or default to Modbus standard
                res["byte_order"] = "big"
        if word_order is not None:
            s = str(word_order).lower()
            # 1 = First Word Low (low_high), 0 = First Word High (high_low)
            # Check for explicit "high_low" or numeric "0" first
            if s == "0" or s == "high_low" or s == "high-low":
                res["word_order"] = "high_low"
            else:
                res["word_order"] = "low_high"
        if dword_order is not None:
            s = str(dword_order).lower()
            # 1 = First DWord Low (low_high), 0 = First DWord High (high_low)
            if s == "0" or s == "high_low" or s == "high-low":
                res["dword_order"] = "high_low"
            else:
                res["dword_order"] = "low_high"
        if bit_order is not None:
            s = str(bit_order).lower()
            # 1 = MSB (Modicon), 0 = LSB (normal/disabled)
            if s == "1" or "enable" in s or "msb" in s or "modicon" in s:
                res["bit_order"] = "msb"
            else:
                res["bit_order"] = "lsb"
        if treat_longs_as_decimals is not None:
            s = str(treat_longs_as_decimals).lower()
            res["treat_longs_as_decimals"] = s in ("1", "true", "yes", "enable", "enabled")
    except Exception:
        pass
    return res


def parse_address(addr: str, zero_based: bool = False) -> Tuple[str, int, str]:
    """Parse an address string into (address_type, zero_based_address, raw).

    address_type: one of 'holding_register','input_register','coil','discrete_input'
    """
    if addr is None:
        raise ValueError("addr is None")
    raw = str(addr).strip()
    s = raw.lower()
    
    # Extract the numeric part first (handles cases like "424576 [50]")
    # This regex extracts the first sequence of digits
    m_numeric = re.search(r"(\d+)", s)
    if m_numeric:
        s_numeric = m_numeric.group(1)
    else:
        s_numeric = s

    # explicit labels
    if s.startswith("coil") or s.startswith("c:") or s.startswith("co"):
        m = re.search(r"(\d+)$", s)
        if m:
            idx = int(m.group(1))
            # Modbus Coil: 1-65536 (6-digit: 000001-065536)
            if 1 <= idx <= 65536:
                # zero_based: 0 → idx - 0, zero_based: 1 → idx - 1
                return "coil", idx - zero_based, raw
            return "coil", 0, raw
    if s.startswith("discrete") or s.startswith("di"):
        m = re.search(r"(\d+)$", s)
        if m:
            idx = int(m.group(1))
            # Modbus Discrete Input: 100001-165536
            if 100001 <= idx <= 165536:
                # zero_based: 0 → idx - 100000, zero_based: 1 → idx - 100000 - 1
                return "discrete_input", idx - 100000 - zero_based, raw
            return "discrete_input", 0, raw
    if s.startswith("holding") or s.startswith("hr") or s.startswith("h:"):
        m = re.search(r"(\d+)$", s)
        if m:
            idx = int(m.group(1))
            result = _check_address_range(idx)
            if result and result[0] == "holding_register":
                addr_type, offset = result
                result_addr = idx - offset - zero_based
                return addr_type, result_addr, raw
            return "holding_register", 0, raw
    if s.startswith("input") or s.startswith("ir"):
        m = re.search(r"(\d+)$", s)
        if m:
            idx = int(m.group(1))
            result = _check_address_range(idx)
            if result and result[0] == "input_register":
                addr_type, offset = result
                result_addr = idx - offset - zero_based
                return addr_type, result_addr, raw
            return "input_register", 0, raw

    # colon-separated form TYPE:ADDR e.g. 4:400001 or 3:300001
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            prefix = int(parts[0])
            idx = int(parts[1])
            # Map prefix (0-4) to address type
            prefix_map = {
                0: "coil",
                1: "discrete_input",
                3: "input_register",
                4: "holding_register",
            }
            if prefix in prefix_map:
                result = _check_address_range(idx)
                if result and result[0] == prefix_map[prefix]:
                    addr_type, offset = result
                    result_addr = idx - offset - zero_based
                    return addr_type, result_addr, raw
            return prefix_map.get(prefix, "holding_register"), 0, raw

    # numeric-only heuristics
    # Try to match the full string first, then fall back to extracted numeric part
    if re.fullmatch(r"\d+", s):
        n = int(s)
    elif re.fullmatch(r"\d+\s*\[.*\]", s) or re.match(r"\d+", s):
        # Handle array notation like "424576 [50]" or just use first numeric match
        n = int(s_numeric)
    else:
        n = None
    
    if n is not None:
        # Modbus 6-digit addressing per IEC 61131
        # Check address ranges using centralized constants
        for addr_range in MODBUS_ADDRESS_RANGES:
            if addr_range["min"] <= n <= addr_range["max"]:
                result_addr = n - addr_range["offset"] - zero_based
                return addr_range["type"], result_addr, raw
        # Special case: coil address 0
        if n == 0:
            return "coil", 0, raw

    # fallback: treat as holding register and leave index as 0
    return "holding_register", 0, raw


def _normalize_data_type(dt: str) -> Tuple[str, int]:
    """Return canonical data_type and register/count size.
    
    Handles:
    - Base types: bool, float32, float64, uint16, uint32, int16, int32
    - Array types: float32[50], uint16[], "Long Array[50]", "Float Array", etc.
    """
    if not dt:
        return "uint16", 1
    s = dt.lower()
    
    # Extract array count first if present [n]
    array_count = None
    array_match = re.search(r"\[\s*(\d*)\s*\]", s)
    if array_match:
        array_count = int(array_match.group(1)) if array_match.group(1).isdigit() else 1
        # Remove the [n] part for further processing
        s_base = re.sub(r"\s*\[\s*\d*\s*\]", "", s)
    else:
        s_base = s
    
    # Now determine the base type
    base_type = None
    base_count = 1
    
    if "bool" in s_base or "boolean" in s_base:
        base_type, base_count = "bool", 1
    elif "float64" in s_base or "double" in s_base:
        base_type, base_count = "float64", 4
    elif "float" in s_base or "float32" in s_base:
        base_type, base_count = "float32", 2
    elif "qword" in s_base or "uint64" in s_base or "int64" in s_base:
        base_type, base_count = "uint64", 4
    elif "dword" in s_base or "uint32" in s_base or "int32" in s_base or "long" in s_base:
        base_type, base_count = "uint32", 2
    elif "short" in s_base or "int16" in s_base:
        base_type, base_count = "int16", 1
    elif "byte" in s_base or "uint8" in s_base:
        base_type, base_count = "uint8", 1
    elif "bcd" in s_base or "lbcd" in s_base:
        # BCD and LBCD are special numeric formats (typically 1-2 registers)
        # Treat as uint16 for now (can be refined later if needed)
        base_type, base_count = "uint16", 1
    elif "string" in s_base or "char" in s_base:
        # String/Char types: typically 1 character per register or 2 per register
        # For now, treat as uint16 (can be refined if needed)
        base_type, base_count = "uint16", 1
    elif "word" in s_base or "uint16" in s_base or "int" in s_base:
        base_type, base_count = "uint16", 1
    else:
        base_type, base_count = "uint16", 1
    
    # If array count was detected, return array format
    if array_count is not None:
        return f"{base_type}[]", base_count * array_count
    
    # If "array" word was found in original, return as array with single element
    if "array" in s:
        return f"{base_type}[]", base_count
    
    # Otherwise return scalar type
    return base_type, base_count


def map_tag_to_pymodbus(tag: Dict[str, Any], device: Optional[Dict[str, Any]] = None,
                         channel: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Map a role `Tag` dict (and optional parent Device/Channel) to canonical pymodbus dict.

    Expected minimal keys on `tag`: Description, Data Type, Client Access, Address, Scan Rate, Scaling, Metadata
    Device expected to provide: Device ID, Timing, Data Access, Encoding, Block Sizes
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # read common fields with safe defaults
    name = tag.get("Description") or tag.get("name") or tag.get("id")
    data_type_raw = tag.get("Data Type") or tag.get("data_type") or "uint16"
    client_access = (tag.get("Client Access") or tag.get("access") or "R").upper()
    addr = tag.get("Address") or tag.get("address")
    scan = tag.get("Scan Rate") or tag.get("scan_rate_ms") or tag.get("scan") or None
    scaling = tag.get("Scaling") or tag.get("scaling")
    metadata = tag.get("Metadata") or tag.get("metadata") or {}
    
    # DEBUG: Log Data tags
    if 'Data' in (name or ''):
        logger.info(f"[MAP_TAG_DEBUG] name={name} addr_input={addr}")

    device_unit = None
    zero_based = False
    zero_based_bit = False
    data_access = {}
    encoding = {}
    block_sizes = {}
    if device:
        device_unit = device.get("Device ID") or device.get("unit_id")
        # Convert "Enable"/"Disable" strings to boolean, default to False
        zero_based_raw = device.get("Data Access", {}).get("zero_based") or device.get("zero_based")
        if isinstance(zero_based_raw, str):
            zero_based = zero_based_raw.lower() == "enable"
        else:
            zero_based = bool(zero_based_raw)
        
        # Get zero_based_bit for Coil/Discrete addressing
        zero_based_bit_raw = device.get("Data Access", {}).get("zero_based_bit") or device.get("zero_based_bit")
        if isinstance(zero_based_bit_raw, str):
            zero_based_bit = zero_based_bit_raw.lower() == "enable"
        else:
            zero_based_bit = bool(zero_based_bit_raw)
        
        data_access = device.get("Data Access") or {}
        encoding = device.get("Encoding") or {}
        block_sizes = device.get("Block Sizes") or {}

    # DEBUG: Log zero_based parameter
    if 'Data' in (name or '') or 'TOUData' in (name or ''):
        logger.info(f"[PARSE_ADDRESS] name={name} addr={addr} zero_based_raw={zero_based_raw if device else 'NO_DEVICE'} zero_based={zero_based} zero_based_bit={zero_based_bit}")

    # Determine which zero_based setting to use based on data type
    # Coil/Discrete use zero_based_bit (inverted: 0=1-base, 1=0-base)
    # Register types use zero_based (0=0-base, 1=1-base)
    is_boolean = data_type_raw and ('bool' in data_type_raw.lower() or 'boolean' in data_type_raw.lower())
    if is_boolean:
        # For bits: 0 = 1-base (need zero_based=1), 1 = 0-base (need zero_based=0)
        # So we need to invert: zero_based_bit_for_parse = 1 - zero_based_bit
        zero_based_bit_for_parse = 1 - int(zero_based_bit)
        address_type, address_zero, raw = parse_address(addr if addr is not None else "", zero_based=zero_based_bit_for_parse)
    else:
        address_type, address_zero, raw = parse_address(addr if addr is not None else "", zero_based=zero_based)

    dtype, count = _normalize_data_type(data_type_raw)
    
    # Store the array_element_count for later use in decoding
    # This is extracted from address like "428672 [58]"
    # We don't multiply count here because scheduler can't handle > 120 registers
    # Instead, we'll use this for proper array decoding in modbus_client
    array_elem_count = tag.get("array_element_count")
    if array_elem_count and not dtype.endswith('[]'):
        # Only set if it's an array type
        pass  # Will be handled in canonical
    
    # Determine if this is an array tag based on dtype
    # dtype ends with '[]' if it's an array (e.g., 'float32[]', 'uint16[]')
    is_array_tag = dtype.endswith('[]')
    
    # For array tags, extract array element count from address if present
    # E.g., address "428672 [58]" means 58 elements
    if is_array_tag:
        array_elem_match = re.search(r'\[\s*(\d+)\s*\]', raw or "")
        if array_elem_match:
            array_elem_count = int(array_elem_match.group(1))
            # Recalculate count based on element count and type size
            # count is the number of registers per element (from _normalize_data_type)
            base_registers = count  # This is registers per element
            count = array_elem_count * base_registers
            logger.info(f"[ARRAY_COUNT_CALC] name={name} dtype={dtype} array_elem_count={array_elem_count} base_regs={base_registers} final_count={count} raw={raw}")

    # determine write func preference
    write_func = None
    if dtype == "bool":
        write_func = 5
    else:
        # prefer func_06 for single register when enabled
        if data_access.get("func_06"):
            write_func = 6
        else:
            write_func = 16

    canonical = {
        "role": "Tag",
        "name": name,
        "unit_id": int(device_unit) if device_unit is not None else None,
        "address_type": address_type,
        "address": int(address_zero),
        "count": int(count),
        "data_type": dtype,
        "byte_order": encoding.get("byte_order") or 1,
        "word_order": encoding.get("word_order") or 1,
        "dword_order": encoding.get("dword_order") or 1,
        "bit_order": encoding.get("bit_order") or 0,
        "treat_longs_as_decimals": encoding.get("treat_longs_as_decimals") or 0,
        "scaling": scaling,
        "access": client_access,
        "write_func": write_func,
        "is_array": is_array_tag or bool(metadata.get("is_array")),  # Use dtype-based detection or metadata
        "addrnum": metadata.get("addrnum"),
        "scan_rate_ms": int(scan) if scan is not None else None,
        "block_hint": block_sizes.get("hold_regs") or block_sizes.get("int_regs") or None,
        "raw_address_str": raw,
        "array_element_count": array_elem_count,  # For Array tags: how many elements
    }

    return canonical


if __name__ == "__main__":
    # quick manual test
    sample_tag = {
        "Description": "TempSensor1",
        "Data Type": "Float",
        "Client Access": "R",
        "Address": "40001",
        "Scan Rate": 1000,
        "Scaling": {"type": "linear", "raw_low": 0, "raw_high": 65535, "scaled_low": 0, "scaled_high": 100},
        "Metadata": {"addrnum": 1}
    }
    sample_device = {
        "Device ID": 5,
        "Data Access": {"zero_based": False, "func_06": True},
        "Encoding": {"byte_order": "big", "word_low": "low_high"},
        "Block Sizes": {"hold_regs": 120}
    }
    print(map_tag_to_pymodbus(sample_tag, sample_device))
