# Lightweight wrapper around pymodbus clients (migrated into core).
# This is a near-copy of the root `modbus_client.py` but with imports
# adjusted to use the `core` package-local `pymodbus_trace` shim.

from __future__ import annotations

import asyncio
import inspect
import re
import struct
import traceback
from typing import Optional, Any, Dict, List

ModbusTcpClient = None
ModbusSerialClient = None
_import_errors: List[tuple[str, str]] = []
try:
    from pymodbus.client.sync import ModbusTcpClient, ModbusSerialClient  # type: ignore
except Exception as e:  # pragma: no cover
    _import_errors.append(("pymodbus.client.sync", str(e)))
    try:
        from pymodbus.client import ModbusTcpClient, ModbusSerialClient  # type: ignore
    except Exception as e2:  # pragma: no cover
        _import_errors.append(("pymodbus.client", str(e2)))

from .modbus_scheduler import group_reads
from .modbus_mapping import map_endian_names_to_constants


class ModbusClient:
    """Light wrapper around pymodbus client with batch-read + decode helpers."""
    def __init__(self, mode: str = "tcp", host: str | None = None, port: int = 502, unit: int = 1, connect_timeout: float = 3.0, request_timeout: float = 2.0, diag_callback: Optional[Any] = None, data_access: Optional[Dict] = None, encoding: Optional[Dict] = None, **kwargs):
        self.mode = (mode or "tcp").lower()
        self.host = host
        self.port = int(port or 502)
        self.unit = int(unit or 1)
        self.connect_timeout = float(connect_timeout or 3.0)
        self.request_timeout = float(request_timeout or 2.0)
        self.data_access = data_access or {}
        self.encoding = encoding or {}
        self.kwargs = kwargs or {}
        self._client = None
        self.diag_callback = diag_callback
        # Request retry configuration (from attempts_before_timeout)
        self.max_attempts = int(kwargs.get("max_attempts", 1)) if isinstance(kwargs, dict) else 1
        self.max_attempts = max(1, self.max_attempts)  # At least 1 attempt

    async def connect_async(self) -> bool:
        if self.mode in ("tcp", "overtcp"):
            if ModbusTcpClient is None:
                msg = "pymodbus is required for ModbusClient (TCP)."
                try:
                    msg += " Import attempts: " + ", ".join(f"{p}:{e}" for p, e in _import_errors)
                except Exception:
                    pass
                raise ImportError(msg)

            def _sync():
                # pymodbus 3.x requires keyword arguments
                client_kw = {"host": self.host, "port": self.port, "timeout": self.connect_timeout}
                try:
                    self._client = ModbusTcpClient(**client_kw)
                except Exception as e:
                    # Fallback without timeout
                    try:
                        self._client = ModbusTcpClient(host=self.host, port=self.port)
                    except Exception:
                        self._client = None
                ok = False
                try:
                    if self._client is not None:
                        ok = bool(self._client.connect())
                except Exception:
                    ok = False
                return ok

            # Attempt connection with retries based on connect_timeout/connect_attempts
            # Note: connect_attempts is extracted from kwargs (passed during initialization)
            connect_attempts = int(self.kwargs.get("connect_attempts", 1)) if isinstance(self.kwargs, dict) else 1
            connect_attempts = max(1, connect_attempts)  # At least 1 attempt
            
            for attempt in range(connect_attempts):
                result = await asyncio.to_thread(_sync)
                if result:
                    if self.diag_callback:
                        self.diag_callback(f"CONNECTED: {self.host}:{self.port} (attempt {attempt+1}/{connect_attempts})")
                    return True
                if attempt < connect_attempts - 1 and self.diag_callback:
                    self.diag_callback(f"CONNECTION_RETRY: attempt {attempt+1}/{connect_attempts} failed, retrying...")
            
            if self.diag_callback:
                self.diag_callback(f"CONNECTION_FAILED: {self.host}:{self.port} after {connect_attempts} attempts")
            return False
            
        elif self.mode == "rtu":
            if ModbusSerialClient is None:
                raise ImportError("pymodbus is required for ModbusClient (RTU).")

            def _sync():
                ser_port = self.kwargs.get("serial_port") or self.kwargs.get("port") or self.host
                base_kw = {"port": ser_port, "baudrate": int(self.kwargs.get("baudrate", 9600)), "timeout": self.request_timeout}
                try:
                    self._client = ModbusSerialClient(**base_kw)
                except Exception:
                    try:
                        self._client = ModbusSerialClient(ser_port)
                    except Exception:
                        self._client = None
                ok = False
                try:
                    if self._client is not None:
                        ok = bool(self._client.connect())
                except Exception:
                    ok = False
                return ok

            # Attempt connection with retries
            connect_attempts = int(self.kwargs.get("connect_attempts", 1)) if isinstance(self.kwargs, dict) else 1
            connect_attempts = max(1, connect_attempts)  # At least 1 attempt
            
            for attempt in range(connect_attempts):
                result = await asyncio.to_thread(_sync)
                if result:
                    if self.diag_callback:
                        self.diag_callback(f"CONNECTED: {self.kwargs.get('serial_port')} (attempt {attempt+1}/{connect_attempts})")
                    return True
                if attempt < connect_attempts - 1 and self.diag_callback:
                    self.diag_callback(f"CONNECTION_RETRY: attempt {attempt+1}/{connect_attempts} failed, retrying...")
            
            if self.diag_callback:
                self.diag_callback(f"CONNECTION_FAILED: {self.kwargs.get('serial_port')} after {connect_attempts} attempts")
            return False
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    async def close_async(self) -> None:
        def _sync():
            try:
                if self._client:
                    try:
                        self._client.close()
                    except Exception:
                        pass
            except Exception:
                pass
            self._client = None

        await asyncio.to_thread(_sync)

    # --- flexible call helper (kept from original) ---
    async def _call_method_flexible_async(self, method, address, count):
        """Call pymodbus 3.x sync methods using asyncio.to_thread.
        
        pymodbus 3.x methods are synchronous and use keyword arguments.
        For read_coils and read_discrete_inputs, 'count' is keyword-only!
        """
        method_name = getattr(method, '__name__', 'unknown')
        
        # For read_coils and read_discrete_inputs, count is keyword-only
        if method_name in ('read_coils', 'read_discrete_inputs'):
            attempts = [
                ("kwargs_coils", lambda: (True, method, {"address": int(address), "count": int(count)})),
            ]
        else:
            # For read_holding_registers and read_input_registers
            attempts = [
                ("kwargs_int", lambda: (True, method, {"address": int(address), "count": int(count)})),
                ("positional_int", lambda: (False, method, (int(address), int(count)))),
            ]

        for name, get_args in attempts:
            try:
                is_kwargs, func, args = get_args()
                if is_kwargs:
                    res = await asyncio.to_thread(func, **args)
                else:
                    res = await asyncio.to_thread(func, *args)
                return res
            except Exception as e:
                if self.diag_callback:
                    try:
                        self.diag_callback(f"FALLBACK_FAIL[{name}]: err={str(e)[:100]}")
                    except Exception:
                        pass
                continue

        # Final fallback (should not reach here if attempts work)
        try:
            res = await asyncio.to_thread(method, address=int(address), count=int(count))
            return res
        except Exception as e:
            if self.diag_callback:
                try:
                    self.diag_callback(f"CALL_EXHAUSTED: method={getattr(method,'__name__',repr(method))}")
                except Exception:
                    pass
            raise

    async def _execute_with_retry(self, method_func, *args, **kwargs):
        """
        通用重試邏輯 - 適用於所有讀寫操作 (FC1-6, 15-16)
        Kepware "Attempts Before Timeout" 實現
        """
        max_attempts = kwargs.pop("max_attempts", self.max_attempts)
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                # Check if method_func is async or sync
                if asyncio.iscoroutinefunction(method_func):
                    result = await method_func(*args, **kwargs)
                else:
                    result = await asyncio.to_thread(method_func, *args, **kwargs)
                
                if attempt > 0 and self.diag_callback:
                    try:
                        self.diag_callback(f"RETRY_SUCCESS: method={getattr(method_func,'__name__',repr(method_func))} on attempt {attempt + 1}/{max_attempts}")
                    except Exception:
                        pass
                return result
            except Exception as e:
                last_error = e
                if attempt < max_attempts - 1:
                    if self.diag_callback:
                        try:
                            self.diag_callback(f"RETRY_ATTEMPT: method={getattr(method_func,'__name__',repr(method_func))} attempt {attempt + 1}/{max_attempts} failed, retrying...")
                        except Exception:
                            pass
                    await asyncio.sleep(0.1)  # Short delay before retry
                continue
        
        # All attempts failed
        if self.diag_callback:
            try:
                self.diag_callback(f"RETRY_EXHAUSTED: method={getattr(method_func,'__name__',repr(method_func))} after {max_attempts} attempts")
            except Exception:
                pass
        raise last_error

    @staticmethod
    def _registers_to_bytes(registers: list[int]) -> bytes:
        b = bytearray()
        try:
            for r in registers:
                v = int(r) & 0xFFFF
                b.extend(v.to_bytes(2, "big"))
        except Exception:
            pass
        return bytes(b)

    async def read_async(self, address: int, count: int, function_code: int, encoding: Optional[dict] = None):
        # Convert Modbus 1-based format address (400005) to actual protocol address (5)
        try:
            addr_int = int(address)
            # If address >= 10000, it's in 1-based format, extract the offset
            if addr_int >= 10000:
                # Remove the first digit(s) to get the actual offset
                # 400005 → 5, 300005 → 5, etc.
                addr_int = addr_int % 10000
            address = addr_int
        except (ValueError, TypeError):
            pass
        
        if function_code == 1:
            method_name = "read_coils"
        elif function_code == 2:
            method_name = "read_discrete_inputs"
        elif function_code == 3:
            method_name = "read_holding_registers"
        elif function_code == 4:
            method_name = "read_input_registers"
        else:
            raise ValueError(f"Unsupported function code: {function_code}")

        if self._client is None:
            try:
                if self.diag_callback:
                    self.diag_callback(f"CLIENT_NOT_CONNECTED: Attempting connection to {self.host}:{self.port}")
                ok = await self.connect_async()
                if not ok:
                    if self.diag_callback:
                        self.diag_callback(f"CONNECTION_FAILED: {self.host}:{self.port}")
            except Exception as e:
                if self.diag_callback:
                    self.diag_callback(f"CONNECTION_ERROR: {str(e)[:80]}")

        method = getattr(self._client, method_name, None) if self._client else None
        if method is None:
            msg = f"Underlying client missing or method {method_name} not found"
            if self.diag_callback:
                self.diag_callback(f"METHOD_MISSING: {msg}")
            raise AttributeError(msg)

        if self.diag_callback:
            self.diag_callback(f"READING: method={method_name} addr={address} count={count}")
        
        # Use retry mechanism for read operations
        res = await self._execute_with_retry(
            self._call_method_flexible_async,
            method, address, count,
            max_attempts=self.max_attempts
        )
        
        try:
            if method_name in ("read_coils", "read_discrete_inputs"):
                if hasattr(res, "bits") and res.bits is not None:
                    res.bits_list = list(res.bits)
                    res.data_bytes = b""
                    if self.diag_callback:
                        self.diag_callback(f"BITS_READ: method={method_name} addr={address} count={count} bits_count={len(res.bits_list)}")
                else:
                    res.data_bytes = getattr(res, "data", b"") or b""
                    if self.diag_callback:
                        self.diag_callback(f"BITS_READ_FAILED: method={method_name} addr={address} count={count}")
            elif hasattr(res, "registers") and res.registers is not None and len(res.registers) > 0:
                res.data_bytes = self._registers_to_bytes(list(res.registers))
            else:
                res.data_bytes = getattr(res, "data", b"") or b""
        except Exception as e:
            res.data_bytes = b""
            if self.diag_callback:
                self.diag_callback(f"READ_EXCEPTION: method={method_name} addr={address} error={str(e)[:80]}")

        return res

    # --- batch read + decode ---
    @staticmethod
    def _apply_word_byte_order(raw: bytes, byte_order: str, word_order: str) -> bytes:
        # raw is sequence of 2-byte registers in network order (big-endian per register)
        words = [raw[i:i+2] for i in range(0, len(raw), 2)]
        if byte_order == "little":
            words = [w[::-1] for w in words]
        if word_order == "high_low":
            words = list(reversed(words))
        return b"".join(words)

    @staticmethod
    def _decode_scalar_from_bytes(b: bytes, dtype: str, byte_order: str = "big"):
        # dtype like 'uint16','uint32','float32','float64','bool'
        # byte_order: 'big' (big-endian) or 'little' (little-endian)
        # Note: This function receives the byte_order that was used in _apply_word_byte_order()
        # so it knows how to interpret the transformed bytes
        
        endian_char = '<' if byte_order == 'little' else '>'
        
        if dtype == "bool":
            return bool(b[0] & 1)
        if dtype.startswith("uint"):
            return int.from_bytes(b, byte_order, signed=False)
        if dtype.startswith("int"):
            signed = True
            return int.from_bytes(b, byte_order, signed=signed)
        if dtype in ("float32", "float"):
            try:
                return struct.unpack(f'{endian_char}f', b)[0]
            except Exception:
                return None
        if dtype == "float64" or dtype == "double":
            try:
                return struct.unpack(f'{endian_char}d', b)[0]
            except Exception:
                return None
        return int.from_bytes(b, byte_order)

    @staticmethod
    def _encode_scalar_to_bytes(value: Any, dtype: str, byte_order: str = "big") -> bytes:
        """Encode a scalar value to bytes for writing to device.
        
        This is the inverse operation of _decode_scalar_from_bytes().
        Encodes scalar values to byte representation, handling byte_order and word_order.
        
        Returns raw bytes in network order (big-endian per word) that will be transformed
        by _reverse_apply_word_byte_order() before sending to device.
        """
        endian_char = '<' if byte_order == 'little' else '>'
        
        if dtype == "bool":
            return bytes([0xFF if value else 0x00])
        
        if dtype in ("float32", "float"):
            try:
                return struct.pack(f'{endian_char}f', float(value))
            except Exception:
                return b'\x00\x00\x00\x00'
        
        if dtype == "float64" or dtype == "double":
            try:
                return struct.pack(f'{endian_char}d', float(value))
            except Exception:
                return b'\x00\x00\x00\x00\x00\x00\x00\x00'
        
        # Integer types
        if dtype.startswith("int"):
            size_match = re.search(r'(\d+)', dtype)
            if size_match:
                bits = int(size_match.group(1))
                num_bytes = bits // 8
            else:
                num_bytes = 2  # default to 16-bit
            return int(value).to_bytes(num_bytes, byte_order, signed=True)
        
        # Unsigned integer types (default)
        size_match = re.search(r'(\d+)', dtype)
        if size_match:
            bits = int(size_match.group(1))
            num_bytes = bits // 8
        else:
            num_bytes = 2  # default to 16-bit
        return int(value).to_bytes(num_bytes, byte_order, signed=False)

    @staticmethod
    def _reverse_apply_word_byte_order(raw: bytes, byte_order: str, word_order: str) -> bytes:
        """Reverse the transformation applied by _apply_word_byte_order().
        
        This is used before sending write data to device.
        """
        words = [raw[i:i+2] for i in range(0, len(raw), 2)]
        
        # Reverse words if word_order="low_high"
        # (we have [high_word, low_word] from big-endian, need to send [low_word, high_word])
        if word_order == "low_high":
            words = list(reversed(words))
        
        result = b"".join(words)
        return result






    @staticmethod
    def _apply_bit_order(value: int, num_bits: int, bit_order: str) -> int:
        """Apply bit order transformation (Modicon Bit Order).
        
        Args:
            value: Integer value with bits to transform
            num_bits: Number of bits (16 for register, 8 for byte)
            bit_order: 'msb' (Modicon/enabled) or 'lsb' (normal/disabled)
        
        Returns:
            Transformed integer value
        """
        if bit_order == "lsb" or num_bits <= 1:
            return value
        
        # Reverse bit order within the value
        result = 0
        for i in range(num_bits):
            if value & (1 << i):
                result |= (1 << (num_bits - 1 - i))
        return result

    @staticmethod
    def _apply_word_order_to_dwords(words: list, word_order: str, dword_order: str) -> list:
        """Apply word and dword order transformations for 32-bit and 64-bit values.
        
        Args:
            words: List of 16-bit word values (big-endian already)
            word_order: 'low_high' or 'high_low' (First Word Low/High)
            dword_order: 'low_high' or 'high_low' (First DWord Low/High for 64-bit)
        
        Returns:
            Reordered list of words
        """
        if len(words) < 2:
            return words
        
        result = []
        i = 0
        
        if len(words) >= 4:
            # 64-bit value (4 words)
            dword_low = words[0:2]   # First 2 words
            dword_high = words[2:4]  # Last 2 words
            
            # Apply dword_order
            if dword_order == "high_low":
                dword_low, dword_high = dword_high, dword_low
            
            # Apply word_order within each dword
            if word_order == "high_low":
                dword_low = [dword_low[1], dword_low[0]]
                dword_high = [dword_high[1], dword_high[0]]
            
            result = dword_low + dword_high
        else:
            # 32-bit value (2 words)
            if word_order == "high_low":
                result = [words[1], words[0]]
            else:
                result = words
        
        return result

    @staticmethod
    def _encode_32bit_value(value: Any, dtype: str, byte_order: str, word_order: str) -> bytes:
        """Encode a 32-bit value (uint32, int32, float32) to bytes in big-endian format.
        
        Returns 4 bytes in BIG-ENDIAN format (network byte order).
        Word order transformation will be applied later by _reverse_apply_word_byte_order if needed.
        """
        # Always use big-endian for internal representation
        if dtype in ("float32", "float"):
            raw_bytes = struct.pack('>f', float(value))
        elif dtype == "uint32":
            raw_bytes = int(value).to_bytes(4, 'big', signed=False)
        elif dtype == "int32":
            raw_bytes = int(value).to_bytes(4, 'big', signed=True)
        else:
            raw_bytes = b'\x00\x00\x00\x00'
        
        # Return bytes without word order transformation
        # Word order will be applied by _reverse_apply_word_byte_order
        return raw_bytes

    @staticmethod
    def _encode_64bit_value(value: Any, dtype: str, byte_order: str, word_order: str, dword_order: str, treat_longs_as_decimals: bool = False) -> bytes:
        """Encode a 64-bit value (uint64, int64, float64) to bytes in big-endian format.
        
        Returns 8 bytes in BIG-ENDIAN format (network byte order).
        Word/dword order transformation will be applied later by _reverse_apply_word_byte_order if needed.
        """
        if treat_longs_as_decimals and dtype in ("uint64", "int64"):
            # Format: [High Word (0-9999)][Low Word (0-9999)]
            # Value = High * 10000 + Low
            int_val = int(value)
            if int_val > 99999999:
                int_val = 99999999
            high_word = int_val // 10000
            low_word = int_val % 10000
            # Pack as 4 uint16 values: 0, high, 0, low
            raw_bytes = struct.pack('>HHHH', 0, high_word & 0xFFFF, 0, low_word & 0xFFFF)
        else:
            # Always use big-endian for internal representation
            if dtype == "float64" or dtype == "double":
                raw_bytes = struct.pack('>d', float(value))
            elif dtype == "uint64":
                raw_bytes = int(value).to_bytes(8, 'big', signed=False)
            elif dtype == "int64":
                raw_bytes = int(value).to_bytes(8, 'big', signed=True)
            else:
                return b'\x00\x00\x00\x00\x00\x00\x00\x00'
        
        # Return bytes without word/dword order transformation
        # Word/dword order will be applied by _reverse_apply_word_byte_order
        return raw_bytes

    @staticmethod
    def _decode_32bit_value(raw_bytes: bytes, dtype: str, byte_order: str, word_order: str) -> Any:
        """Decode a 32-bit value from raw bytes with proper word order handling.
        
        Input bytes are in device format (what we read from Modbus).
        """
        if len(raw_bytes) < 4:
            return None
        
        # Split into words
        words = [raw_bytes[i:i+2] for i in range(0, 4, 2)]
        
        # Reverse words if word_order="low_high" 
        # (device stores [low_word, high_word], but big-endian needs [high_word, low_word])
        if word_order == "low_high":
            words = list(reversed(words))
        
        ordered_bytes = b"".join(words)
        
        # Interpret as big-endian
        try:
            if dtype in ("float32", "float"):
                return struct.unpack('>f', ordered_bytes)[0]
            elif dtype == "uint32":
                return int.from_bytes(ordered_bytes, 'big', signed=False)
            elif dtype == "int32":
                return int.from_bytes(ordered_bytes, 'big', signed=True)
        except Exception:
            return None

    @staticmethod
    def _decode_64bit_value(raw_bytes: bytes, dtype: str, byte_order: str, word_order: str, dword_order: str, treat_longs_as_decimals: bool = False) -> Any:
        """Decode a 64-bit value from raw bytes with proper word/dword order handling.
        
        Input bytes are in device format (what we read from Modbus).
        We need to convert back to big-endian internal format.
        """
        if len(raw_bytes) < 8:
            return None
        
        # Split into 4 words (2-byte each)
        words = [raw_bytes[i:i+2] for i in range(0, 8, 2)]
        
        # Undo byte_order transformation
        if byte_order == "little":
            words = [w[::-1] for w in words]
        
        # Undo word_order transformation (within each dword)
        if word_order == "low_high":
            # Reverse each dword pair to convert [low, high] to [high, low]
            words = [
                words[1], words[0],  # Reverse first dword pair
                words[3], words[2],  # Reverse second dword pair
            ]
        
        # Undo dword_order transformation
        if dword_order == "low_high":
            # Reverse the two dwords to convert [low_dword, high_dword] to [high_dword, low_dword]
            words = words[2:4] + words[0:2]
        
        ordered_bytes = b"".join(words)
        
        # Interpret as big-endian
        try:
            if treat_longs_as_decimals and dtype in ("uint64", "int64"):
                # Decode from [0][High(0-9999)][0][Low(0-9999)] format
                high_word = int.from_bytes(ordered_bytes[2:4], 'big', signed=False)
                low_word = int.from_bytes(ordered_bytes[6:8], 'big', signed=False)
                return high_word * 10000 + low_word
            elif dtype in ("float64", "double"):
                return struct.unpack('>d', ordered_bytes)[0]
            elif dtype == "uint64":
                return int.from_bytes(ordered_bytes, 'big', signed=False)
            elif dtype == "int64":
                return int.from_bytes(ordered_bytes, 'big', signed=True)
        except Exception:
            return None


    async def write_async(self, address: int, values: Any, function_code: int, tag_info: Optional[Dict] = None) -> Any:
        """
        Write to device using specified function code (FC5, FC6, FC15, FC16)
        Supports retry mechanism with full Modbus byte/word/dword/bit order handling
        
        Args:
            address: Modbus address (supports 1-based format like 400005)
            values: Value(s) to write
            function_code: FC5, FC6, FC15, or FC16
            tag_info: Optional tag information with byte_order, word_order, dword_order, bit_order, data_type, treat_longs_as_decimals
        
        Modbus 1-based address format support:
        - 400005 → 5 (Holding Registers, 5th register)
        - 00005 → 5 (Coils, 5th coil)
        - 30005 → 5 (Input Registers, 5th register)
        """
        tag_info = tag_info or {}
        
        # Convert Modbus 1-based format address (400005) to actual protocol address (5)
        original_address = address
        try:
            addr_int = int(address)
            if addr_int >= 10000:
                addr_int = addr_int % 10000
            address = addr_int
            if self.diag_callback:
                self.diag_callback(f"[ADDR_CONVERT] {original_address} → {address}")
        except (ValueError, TypeError) as e:
            if self.diag_callback:
                self.diag_callback(f"[ADDR_CONVERT_FAILED] {original_address} error: {e}")
        
        # Get all endianness settings
        dtype = tag_info.get('data_type', 'uint16')
        byte_order_cfg = tag_info.get('byte_order')
        word_order_cfg = tag_info.get('word_order')
        dword_order_cfg = tag_info.get('dword_order')
        bit_order_cfg = tag_info.get('bit_order')
        treat_longs_decimals = tag_info.get('treat_longs_as_decimals', False)
        
        # Normalize dtype to handle variations like 'float', 'int', 'uint', 'word', 'bool', etc.
        dtype_lower = str(dtype).lower() if dtype else 'uint16'
        
        # Map common variations to canonical types
        dtype_map = {
            'float': 'float32',
            'int': 'int16',
            'uint': 'uint16',
            'word': 'uint16',
            'dword': 'uint32',
            'qword': 'uint64',
            'bool': 'bool',
            'boolean': 'bool',
            'coil': 'bool',
            'bit': 'bool',
            'long': 'int32',
            'ulong': 'uint32',
            'double': 'float64',
        }
        
        # Use exact match first, then partial match
        if dtype_lower in dtype_map:
            dtype = dtype_map[dtype_lower]
        else:
            # Check if it contains any of the key patterns
            for pattern, canonical in dtype_map.items():
                if pattern in dtype_lower:
                    dtype = canonical
                    break
        
        # Map to canonical forms
        eo = map_endian_names_to_constants(byte_order_cfg, word_order_cfg, bit_order_cfg, dword_order_cfg, treat_longs_decimals)
        byte_order = eo.get('byte_order', 'big')
        word_order = eo.get('word_order', 'low_high')
        dword_order = eo.get('dword_order', 'low_high')
        bit_order = eo.get('bit_order', 'lsb')
        treat_longs_decimals = eo.get('treat_longs_as_decimals', False)
        
        # Determine method name and encode value based on function code
        if function_code == 5:
            method_name = "write_coil"
            scalar_value = values if not isinstance(values, (list, tuple)) else values[0]
            write_value = bool(scalar_value)
            
            # Apply bit order for coil
            if bit_order == "msb":
                bit_val = 1 if write_value else 0
                write_value = self._apply_bit_order(bit_val, 1, bit_order) == 1
                
        elif function_code == 6:
            method_name = "write_register"
            scalar_value = values if not isinstance(values, (list, tuple)) else values[0]
            
            # Handle different data types
            if dtype in ('uint32', 'int32', 'float32', 'float'):
                # 32-bit types are not suitable for FC6 (1 register)
                # Try to encode, but warn user
                encoded = self._encode_32bit_value(scalar_value, dtype, byte_order, word_order)
                ordered = self._reverse_apply_word_byte_order(encoded, byte_order, word_order)
                write_value = int.from_bytes(ordered[0:2], 'big')
                if self.diag_callback:
                    self.diag_callback(f"[WARNING_FC6_32BIT] addr={address} dtype={dtype} - use FC16 for full precision")
            else:
                # Standard 16-bit register
                write_value = int(scalar_value) & 0xFFFF
                if bit_order == "msb":
                    write_value = self._apply_bit_order(write_value, 16, bit_order)
                
        elif function_code == 15:
            method_name = "write_coils"
            write_value = [bool(v) for v in (values if isinstance(values, (list, tuple)) else [values])]
            
            # Apply bit order for each coil
            if bit_order == "msb":
                write_value = [self._apply_bit_order(1 if v else 0, 1, bit_order) == 1 for v in write_value]
                
        elif function_code == 16:
            method_name = "write_registers"
            
            # Handle different data types for FC16
            is_array = dtype.endswith('[]') if dtype else False
            base_dtype = dtype[:-2] if is_array else dtype
            
            if is_array:
                # Array of values
                write_value = []
                for scalar_value in (values if isinstance(values, (list, tuple)) else [values]):
                    if base_dtype in ('uint64', 'int64', 'float64', 'double'):
                        encoded = self._encode_64bit_value(scalar_value, base_dtype, byte_order, word_order, dword_order, treat_longs_decimals)
                    elif base_dtype in ('uint32', 'int32', 'float32', 'float'):
                        encoded = self._encode_32bit_value(scalar_value, base_dtype, byte_order, word_order)
                    else:
                        encoded = self._encode_scalar_to_bytes(scalar_value, base_dtype, byte_order)
                    
                    ordered = self._reverse_apply_word_byte_order(encoded, byte_order, word_order)
                    regs = [int.from_bytes(ordered[i:i+2], 'big') for i in range(0, len(ordered), 2)]
                    if bit_order == "msb":
                        regs = [self._apply_bit_order(r, 16, bit_order) for r in regs]
                    write_value.extend(regs)
            elif dtype in ('uint64', 'int64', 'float64', 'double'):
                # 64-bit scalar
                scalar_value = values if not isinstance(values, (list, tuple)) else values[0]
                encoded = self._encode_64bit_value(scalar_value, dtype, byte_order, word_order, dword_order, treat_longs_decimals)
                ordered = self._reverse_apply_word_byte_order(encoded, byte_order, word_order)
                write_value = [int.from_bytes(ordered[i:i+2], 'big') for i in range(0, 8, 2)]
                if bit_order == "msb":
                    write_value = [self._apply_bit_order(r, 16, bit_order) for r in write_value]
            elif dtype in ('uint32', 'int32', 'float32', 'float'):
                # 32-bit scalar
                scalar_value = values if not isinstance(values, (list, tuple)) else values[0]
                encoded = self._encode_32bit_value(scalar_value, dtype, byte_order, word_order)
                ordered = self._reverse_apply_word_byte_order(encoded, byte_order, word_order)
                write_value = [int.from_bytes(ordered[i:i+2], 'big') for i in range(0, 4, 2)]
                if bit_order == "msb":
                    write_value = [self._apply_bit_order(r, 16, bit_order) for r in write_value]
            else:
                # Standard 16-bit registers
                write_value = [int(v) & 0xFFFF for v in (values if isinstance(values, (list, tuple)) else [values])]
                if bit_order == "msb":
                    write_value = [self._apply_bit_order(r, 16, bit_order) for r in write_value]
        else:
            raise ValueError(f"Unsupported write function code: {function_code}")

        # Ensure client is connected
        if self._client is None:
            try:
                if self.diag_callback:
                    self.diag_callback(f"CLIENT_NOT_CONNECTED: Attempting connection to {self.host}:{self.port}")
                ok = await self.connect_async()
                if not ok:
                    if self.diag_callback:
                        self.diag_callback(f"CONNECTION_FAILED: {self.host}:{self.port}")
            except Exception as e:
                if self.diag_callback:
                    self.diag_callback(f"CONNECTION_ERROR: {str(e)[:80]}")

        method = getattr(self._client, method_name, None) if self._client else None
        if method is None:
            msg = f"Underlying client missing or method {method_name} not found"
            if self.diag_callback:
                self.diag_callback(f"METHOD_MISSING: {msg}")
            raise AttributeError(msg)

        if self.diag_callback:
            self.diag_callback(f"WRITING: method={method_name} addr={address} value={write_value} fc={function_code}")
        
        # Build and log TX ADU frame
        try:
            import struct
            # Modbus TCP ADU format for write operations:
            # [Unit ID (1B)] [Function Code (1B)] [Address (2B)] [Quantity/Count] [Data]
            # For FC 5 (Write Single Coil): [Value (2B)] - 0xFF00 for ON, 0x0000 for OFF
            # For FC 6 (Write Single Register): [Value (2B)]
            # For FC 15 (Write Multiple Coils): [Quantity (2B)] [Byte Count (1B)] [Coil Values...]
            # For FC 16 (Write Multiple Registers): [Quantity (2B)] [Byte Count (1B)] [Register Values...]
            
            addr_bytes = struct.pack('>H', int(address) & 0xFFFF)
            
            if function_code == 5:
                # FC 5: Write Single Coil - value is 0xFF00 (ON) or 0x0000 (OFF)
                coil_val = 0xFF00 if write_value else 0x0000
                data_bytes = struct.pack('>H', coil_val)
                adu_data = bytes([self.unit, function_code]) + addr_bytes + data_bytes
            elif function_code == 6:
                # FC 6: Write Single Register - value is 16-bit unsigned
                reg_val = int(write_value) & 0xFFFF
                data_bytes = struct.pack('>H', reg_val)
                adu_data = bytes([self.unit, function_code]) + addr_bytes + data_bytes
            elif function_code == 15:
                # FC 15: Write Multiple Coils
                coil_count = len(write_value)
                byte_count = (coil_count + 7) // 8
                coil_bytes = bytearray(byte_count)
                for i, val in enumerate(write_value):
                    if val:
                        coil_bytes[i // 8] |= (1 << (i % 8))
                adu_data = (bytes([self.unit, function_code]) + addr_bytes + 
                           struct.pack('>H', coil_count) + bytes([byte_count]) + bytes(coil_bytes))
            elif function_code == 16:
                # FC 16: Write Multiple Registers - this is what we use for float32
                reg_count = len(write_value) if isinstance(write_value, list) else 1
                byte_count = reg_count * 2
                reg_bytes = b''
                for reg in (write_value if isinstance(write_value, list) else [write_value]):
                    reg_bytes += struct.pack('>H', int(reg) & 0xFFFF)
                adu_data = (bytes([self.unit, function_code]) + addr_bytes + 
                           struct.pack('>H', reg_count) + bytes([byte_count]) + reg_bytes)
            else:
                adu_data = b""
            
            # Convert to hex for logging
            if adu_data:
                adu_hex = ' '.join(f'{b:02X}' for b in adu_data)
                if self.diag_callback:
                    self.diag_callback(f"[TX_ADU] FC{function_code} addr={address} data={adu_hex} (len={len(adu_data)})")
        except Exception as e:
            if self.diag_callback:
                self.diag_callback(f"[TX_ADU_ERROR] Failed to build ADU: {e}")
        
        # Use retry mechanism for write operations
        res = await self._execute_with_retry(
            method,
            address,
            write_value,
            max_attempts=self.max_attempts
        )
        
        return res

    async def read_batch_async(self, batch: dict) -> List[dict]:
        """Read a grouped batch (as produced by `group_reads`) and decode values for tags.

        Returns list of dicts: { 'tag': tag_dict, 'value': ..., 'raw': bytes }
        """
        atype = batch.get('address_type')
        unit = batch.get('unit_id')
        start = int(batch.get('start', 0))
        count = int(batch.get('count', 0))
        tags = list(batch.get('tags', []))

        fc_map = {
            'coil': 1,
            'discrete_input': 2,
            'holding_register': 3,
            'input_register': 4,
        }
        fc = fc_map.get(atype)
        if fc is None:
            raise ValueError(f"Unknown address_type: {atype}")

        # set unit for this operation
        prev_unit = self.unit
        try:
            if unit is not None:
                try:
                    self.unit = int(unit)
                except Exception:
                    pass

            res = await self.read_async(start, count, fc)

            results = []
            if fc in (1, 2):
                bits = getattr(res, 'bits_list', None) or []
                for t in tags:
                    off = int(t.get('address', 0)) - start
                    val = None
                    if 0 <= off < len(bits):
                        val = 1 if bits[off] else 0  # Convert bool to 1/0
                    results.append({'tag': t, 'value': val, 'raw': None})
                return results

            # register responses
            data = getattr(res, 'data_bytes', b"") or b""
            
            # DEBUG: Log batch information
            start_addr = int(batch.get('start', 0))
            batch_count = int(batch.get('count', 0))
            tag_count = len(tags)
            # if tag_count > 0 and any('Array' in t.get('name', '') or 'Array' in t.get('tree_path', '') for t in tags):
            #     print(f"[BATCH_READ] start={start_addr} count={batch_count} tags={tag_count} data_len={len(data)}")
            
            for t in tags:
                t_addr = int(t.get('address', 0))
                t_count = int(t.get('count', 1))
                off_regs = t_addr - start
                off_bytes = off_regs * 2
                needed = t_count * 2
                
                raw = b""
                if off_bytes + needed <= len(data) and off_bytes >= 0:
                    raw = data[off_bytes:off_bytes + needed]
                
                # resolve endian hints - now with dword_order and treat_longs_as_decimals
                eo = map_endian_names_to_constants(
                    t.get('byte_order'), 
                    t.get('word_order'), 
                    t.get('bit_order'),
                    t.get('dword_order'),
                    t.get('treat_longs_as_decimals')
                )
                byte_order = eo.get('byte_order', 'big')
                word_order = eo.get('word_order', 'low_high')
                dword_order = eo.get('dword_order', 'low_high')
                bit_order = eo.get('bit_order', 'lsb')
                treat_longs_decimals = eo.get('treat_longs_as_decimals', False)

                dtype = t.get('data_type') or 'uint16'
                
                # arrays
                if dtype.endswith('[]'):
                    base = dtype[:-2]
                    # Calculate element size based on base type
                    if base in ('float64', 'double', 'uint64', 'int64'):
                        elem_size_bytes = 8
                    elif base in ('uint32', 'int32', 'float32', 'float'):
                        elem_size_bytes = 4
                    elif base in ('uint8', 'int8', 'byte'):
                        elem_size_bytes = 2
                    else:
                        elem_size_bytes = 2
                    
                    elems = []
                    tag_name = t.get('name', t.get('tree_path', 'Unknown'))
                    
                    for i in range(0, len(raw), elem_size_bytes):
                        chunk = raw[i:i+elem_size_bytes]
                        if len(chunk) > 0:
                            # Decode based on element size
                            if base in ('uint64', 'int64', 'float64', 'double'):
                                if len(chunk) >= 8:
                                    elem_val = self._decode_64bit_value(chunk, base, byte_order, word_order, dword_order, treat_longs_decimals)
                                else:
                                    elem_val = None
                            elif base in ('uint32', 'int32', 'float32', 'float'):
                                if len(chunk) >= 4:
                                    elem_val = self._decode_32bit_value(chunk, base, byte_order, word_order)
                                else:
                                    elem_val = None
                            else:
                                # 16-bit element
                                elem_ordered = self._apply_word_byte_order(chunk, byte_order, word_order)
                                elem_val = self._decode_scalar_from_bytes(elem_ordered, base, byte_order)
                                if bit_order == "msb" and elem_val is not None:
                                    if isinstance(elem_val, int):
                                        elem_val = self._apply_bit_order(elem_val, 16, bit_order)
                            
                            elems.append(elem_val)
                    
                    val = elems
                else:
                    # Scalar value (not array)
                    if dtype in ('uint64', 'int64', 'float64', 'double'):
                        if len(raw) >= 8:
                            val = self._decode_64bit_value(raw, dtype, byte_order, word_order, dword_order, treat_longs_decimals)
                        else:
                            val = None
                    elif dtype in ('uint32', 'int32', 'float32', 'float'):
                        if len(raw) >= 4:
                            val = self._decode_32bit_value(raw, dtype, byte_order, word_order)
                        else:
                            val = None
                    else:
                        # 16-bit scalar
                        ordered = self._apply_word_byte_order(raw, byte_order, word_order)
                        val = self._decode_scalar_from_bytes(ordered, dtype, byte_order)
                        if bit_order == "msb" and val is not None:
                            if isinstance(val, int):
                                val = self._apply_bit_order(val, 16, bit_order)

                results.append({'tag': t, 'value': val, 'raw': raw})

            return results
        finally:
            self.unit = prev_unit


__all__ = ["ModbusClient"]
