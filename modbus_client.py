"""Lightweight wrapper around pymodbus clients.

This file provides a compact, robust `ModbusClient` compatible with the
existing project API (connect_async/read_async/close_async and sync
convenience methods). It intentionally contains fewer nested try/except
blocks than the previous iteration to avoid syntax/indentation pitfalls.

Supports modes: "tcp" (Modbus TCP), "overtcp" (Modbus RTU over TCP),
and "rtu" (serial RTU).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import traceback
from typing import Optional, Any

ModbusTcpClient = None
ModbusSerialClient = None
_import_errors: list[tuple[str, str]] = []
try:
    # pymodbus v2+ uses client.sync or client depending on packaging
    from pymodbus.client.sync import ModbusTcpClient, ModbusSerialClient  # type: ignore
except Exception as e:  # pragma: no cover - import fallback
    _import_errors.append(("pymodbus.client.sync", str(e)))
    try:
        from pymodbus.client import ModbusTcpClient, ModbusSerialClient  # type: ignore
    except Exception as e2:
        _import_errors.append(("pymodbus.client", str(e2)))


class ModbusClient:
    """Compatibility wrapper used by controllers/poller.

    Constructor signature matches callers in this project.
    """

    def __init__(self, mode: str = "tcp", host: str | None = None, port: int = 502, unit: int = 1, connect_timeout: float = 3.0, request_timeout: float = 2.0, diag_callback: Optional[Any] = None, **kwargs):
        self.mode = (mode or "tcp").lower()
        self.host = host
        self.port = port
        self.unit = int(unit or 1)
        self.connect_timeout = float(connect_timeout or 3.0)
        self.request_timeout = float(request_timeout or 2.0)
        self.kwargs = kwargs or {}
        self._client = None
        self.diag_callback = diag_callback
        # when True, pymodbus trace_packet is registered and will emit real TX/RX
        # when True, a custom trace wrapper has been installed on the
        # underlying transport and will emit real TX/RX
        self._trace_enabled = False
        # lazy import for trace helper
        try:
            from pymodbus_trace import install_trace_for_client, uninstall_trace_for_client  # type: ignore
            self._trace_installer = install_trace_for_client
            self._trace_uninstaller = uninstall_trace_for_client
        except Exception:
            self._trace_installer = None
            self._trace_uninstaller = None

    def _trace_packet(self, sending: bool, data: bytes) -> bytes:
        """pymodbus trace_packet callback: forward actual bytes to diag_callback."""
        try:
            if data:
                hex_s = " ".join(f"{b:02X}" for b in data)
                prefix = "TX" if sending else "RX"
                # First, attempt to call the UI diag callback if present.
                try:
                    if self.diag_callback:
                        self.diag_callback(f"{prefix}: | {hex_s} |")
                except Exception:
                    pass
                # Also append raw trace to Diagnostics.txt so writes are captured
                # even if the UI filter misses them (helps debugging missing FC06/FC16).
                try:
                    from datetime import datetime as _dt
                    t = _dt.now()
                    ms = int(t.microsecond / 1000)
                    ts = f"{t.strftime('%H:%M:%S')}.{ms:03d}"
                    with open("Diagnostics.txt", "a", encoding="utf-8") as _f:
                        _f.write(f"{ts}\t{prefix}: | {hex_s} |\n")
                except Exception:
                    pass
        except Exception:
            pass
        return data

    # --- sync convenience ---
    def connect(self) -> bool:
        return asyncio.run(self.connect_async())

    def close(self) -> None:
        return asyncio.run(self.close_async())

    def read(self, address: int, count: int, function_code: int, encoding: Optional[dict] = None):
        return asyncio.run(self.read_async(address, count, function_code, encoding=encoding))

    # --- async implementations ---
    async def connect_async(self) -> bool:
        if self.mode in ("tcp", "overtcp"):
            if ModbusTcpClient is None:
                msg = "pymodbus is required for ModbusClient (TCP)."
                try:
                    msg += " import attempts:" + ",".join(f"{p}:{e}" for p, e in _import_errors)
                except Exception:
                    pass
                raise ImportError(msg)

            def _sync():
                # Build kwargs conservatively
                client_kw = {"host": self.host, "port": self.port, "timeout": self.connect_timeout}
                # allow caller to request RTU framer over TCP by passing framer class
                if self.mode == "overtcp":
                    # caller may provide 'framer' in kwargs
                    client_kw.update({k: v for k, v in self.kwargs.items() if k in ("framer",)})
                # If a diag callback is provided try to register pymodbus
                # `trace_packet` as a lightweight fallback so many pymodbus
                # client implementations will emit TX/RX bytes directly.
                # We still attempt the custom wrapper after connect as well.
                try:
                    if self.diag_callback:
                        client_kw["trace_packet"] = self._trace_packet
                except Exception:
                    pass
                try:
                    self._client = ModbusTcpClient(**client_kw)
                except TypeError:
                    # fallback positional
                    self._client = ModbusTcpClient(self.host, self.port)
                ok = self._client.connect()
                # if connected and we have a diag callback, install custom trace
                try:
                    if ok and self.diag_callback and getattr(self, '_trace_installer', None):
                        try:
                            self._trace_installer(self._client, self.diag_callback)
                            self._trace_enabled = True
                        except Exception:
                            pass
                except Exception:
                    pass
                return ok

            return await asyncio.to_thread(_sync)

        elif self.mode == "rtu":
            if ModbusSerialClient is None:
                raise ImportError("pymodbus is required for ModbusClient (RTU).")

            def _sync():
                # construct and normalize serial params
                ser_port = self.kwargs.get("serial_port") or self.kwargs.get("port") or self.host
                try:
                    if isinstance(ser_port, str):
                        s = ser_port.strip()
                        # numeric like '3' -> 'COM3'
                        if s.isdigit():
                            ser_port = f"COM{int(s)}"
                        else:
                            # normalize 'com3' -> 'COM3'
                            import re

                            m = re.match(r"(?i)^com(\d+)$", s)
                            if m:
                                ser_port = f"COM{int(m.group(1))}"
                            else:
                                ser_port = s
                    else:
                        # try converting numeric values
                        if isinstance(ser_port, (int, float)):
                            ser_port = f"COM{int(ser_port)}"
                except Exception:
                    pass

                # build kwargs for serial client
                base_kw = {"port": ser_port, "baudrate": int(self.kwargs.get("baudrate", 9600)), "timeout": self.request_timeout}
                # Do NOT pass unit/slave identifiers into the serial client's
                # constructor kwargs — some pymodbus serial client constructors
                # do not accept those keyword args and will raise TypeError.
                # Instead, we'll set these as attributes on the instantiated
                # client object after construction (if the object supports them).

                # forward common serial args including framer (but NOT 'method')
                # Some pymodbus serial client constructors do not accept a
                # 'method' keyword and will raise TypeError; avoid passing it.
                for k in ("parity", "stopbits", "bytesize", "rtscts", "xonxoff", "framer"):
                    if k in self.kwargs:
                        base_kw[k] = self.kwargs[k]

                # keep a local method string for diagnostics/framer selection
                try:
                    md = self.kwargs.get("method", "rtu")
                except Exception:
                    md = "rtu"

                # resolve framer string names to class objects when provided
                try:
                    frv = base_kw.get('framer', None)
                    if isinstance(frv, str) and frv:
                        try:
                            from pymodbus.framer.rtu_framer import ModbusRtuFramer as _ModbusRtuFramer
                            base_kw['framer'] = _ModbusRtuFramer
                        except Exception:
                            try:
                                from pymodbus.transaction import ModbusRtuFramer as _ModbusRtuFramer
                                base_kw['framer'] = _ModbusRtuFramer
                            except Exception:
                                # leave as-is (string) if resolution fails
                                pass
                except Exception:
                    pass

                # When possible, pass `trace_packet` to the serial client as a fallback
                try:
                    if self.diag_callback:
                        base_kw["trace_packet"] = self._trace_packet
                except Exception:
                    pass

                # Quick pyserial sanity-check: attempt to open the port directly
                # This helps distinguish constructor API mismatches from OS/permission/port errors.
                try:
                    try:
                        from serial import Serial as _Serial  # type: ignore
                        _sp = None
                        try:
                            _sp = _Serial(port=ser_port, baudrate=base_kw.get('baudrate'), timeout=base_kw.get('timeout'))
                            try:
                                _sp.close()
                            except Exception:
                                pass
                            if self.diag_callback:
                                try:
                                    self.diag_callback(f"SERIAL PORT TEST: port={ser_port} open_ok=True")
                                except Exception:
                                    pass
                        except Exception:
                            if self.diag_callback:
                                try:
                                    self.diag_callback(f"SERIAL PORT TEST: port={ser_port} open_ok=False err={traceback.format_exc().splitlines()[-1]}")
                                except Exception:
                                    pass
                    except Exception:
                        # pyserial not available or import failed; skip test
                        if self.diag_callback:
                            try:
                                self.diag_callback("SERIAL PORT TEST: pyserial unavailable; skipped")
                            except Exception:
                                pass
                except Exception:
                    pass

                # instantiate serial client with robust exception capture
                inst_exc = None
                try:
                    self._client = ModbusSerialClient(**base_kw)
                except Exception:
                    inst_exc = traceback.format_exc()
                    # try without 'method'/'framer' if constructor doesn't accept them
                    try:
                        alt = {kk: vv for kk, vv in base_kw.items() if kk not in ("method", "framer")}
                        self._client = ModbusSerialClient(**alt)
                    except Exception:
                        try:
                            # some pymodbus versions expect an explicit 'method' keyword
                            try_method_kw = {"method": md, "port": ser_port, "baudrate": base_kw.get("baudrate"), "timeout": base_kw.get("timeout")}
                            for kk in ("parity", "stopbits", "bytesize", "rtscts", "xonxoff"):
                                if kk in base_kw:
                                    try_method_kw[kk] = base_kw[kk]
                            self._client = ModbusSerialClient(**try_method_kw)
                        except Exception:
                            try:
                                # minimal positional fallback
                                self._client = ModbusSerialClient(ser_port)
                            except Exception:
                                # final fallback - leave client as None and record inst_exc
                                if not inst_exc:
                                    inst_exc = traceback.format_exc()
                                self._client = None

                # After we have an instantiated client object, set common
                # unit/slave-like attributes on the object (if supported).
                try:
                    if self._client is not None:
                        for _attr in ("unit", "slave", "unit_id", "slave_id", "device_id", "device", "default_unit"):
                            try:
                                setattr(self._client, _attr, int(self.unit))
                            except Exception:
                                pass
                except Exception:
                    pass

                # serial connect: many clients open on first use; call connect() and capture exceptions
                conn_exc = None
                ok = False
                try:
                    if self._client is not None:
                        ok = bool(self._client.connect())
                except Exception:
                    conn_exc = traceback.format_exc()
                    ok = False

                # install serial/socket wrappers if available
                try:
                    if ok and self.diag_callback and getattr(self, '_trace_installer', None) and self._client is not None:
                        try:
                            self._trace_installer(self._client, self.diag_callback)
                            self._trace_enabled = True
                        except Exception:
                            pass
                except Exception:
                    pass

                # emit diagnostic about serial connect attempt
                try:
                    if self.diag_callback:
                        try:
                            fr = self.kwargs.get('framer', None)
                            msg = f"SERIAL CONNECT: port={ser_port} baud={base_kw.get('baudrate')} timeout={self.request_timeout} method={md} framer={fr} ok={ok}"
                            try:
                                if self._client is not None:
                                    msg += f" client={type(self._client).__name__}"
                            except Exception:
                                pass
                            # append exception info if instantiation or connect raised
                            try:
                                if inst_exc:
                                    msg += f" inst_exc=" + inst_exc.splitlines()[-1]
                            except Exception:
                                pass
                            try:
                                if conn_exc:
                                    msg += f" conn_exc=" + conn_exc.splitlines()[-1]
                            except Exception:
                                pass
                            self.diag_callback(msg)
                        except Exception:
                            try:
                                self.diag_callback("SERIAL CONNECT: attempted")
                            except Exception:
                                pass
                except Exception:
                    pass

                return ok

            return await asyncio.to_thread(_sync)

        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    async def close_async(self) -> None:
        def _sync():
            try:
                if self._client:
                    # uninstall any installed trace wrapper before closing
                    try:
                        if getattr(self, '_trace_uninstaller', None):
                            try:
                                self._trace_uninstaller(self._client)
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        self._client.close()
                    except Exception:
                        pass
            except Exception:
                pass
            self._client = None

        await asyncio.to_thread(_sync)

    def _call_method_flexible(self, method, address, count):
        """Call a pymodbus method trying common signatures."""
        try:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            kw = {}
            for p in params:
                pn = p.lower()
                if pn in ("address", "addr", "starting_address", "start"):
                    kw[p] = address
                elif pn in ("count", "quantity", "qty"):
                    kw[p] = count
                elif pn in ("unit", "slave", "device_id", "unit_id", "slave_id", "device"):
                    kw[p] = self.unit
            if kw:
                try:
                    return method(**kw)
                except Exception:
                    # emit a concise diagnostic about the failed kw call
                    try:
                        if getattr(self, 'diag_callback', None):
                            try:
                                self.diag_callback(
                                    f"CALL_FAIL: method={getattr(method, '__name__', repr(method))} kwargs={kw} exc={traceback.format_exc().splitlines()[-1]}"
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
        except Exception:
            pass

        # Emit debug info about the target method/signature to help diagnose
        try:
            if getattr(self, 'diag_callback', None):
                try:
                    try:
                        sig = inspect.signature(method)
                        params = list(sig.parameters.keys())
                        self.diag_callback(f"DEBUG_CALL: method={getattr(method, '__name__', repr(method))} params={params} unit={self.unit}")
                    except Exception:
                        self.diag_callback(f"DEBUG_CALL: method={repr(method)} unable to introspect signature; unit={self.unit}")
                except Exception:
                    pass
        except Exception:
            pass

        # common fallbacks
        # ensure underlying client object has common unit-like attributes set
        try:
            if getattr(self, '_client', None) is not None:
                for _attr in ("unit", "slave", "unit_id", "slave_id", "device_id", "device", "default_unit"):
                    try:
                        if hasattr(self._client, _attr):
                            try:
                                setattr(self._client, _attr, self.unit)
                            except Exception:
                                pass
                    except Exception:
                        pass
        except Exception:
            pass

        # Broad set of fallback call patterns to accommodate differing
        # pymodbus client signatures across versions/wrappers.
        attempts = (
            ("pos_unit_kw", lambda: method(address, count, unit=self.unit)),
            ("pos_slave_kw", lambda: method(address, count, slave=self.unit)),
            ("pos_unit_pos", lambda: method(address, count, self.unit)),
            ("pos_unit_pos_no_resp", lambda: method(address, count, self.unit, False)),
            ("simple_pos", lambda: method(address, count)),
            ("swapped_pos", lambda: method(count, address)),
            ("device_id_no_resp_false", lambda: method(address, count, device_id=self.unit, no_response_expected=False)),
            ("device_id_no_resp_true", lambda: method(address, count, device_id=self.unit, no_response_expected=True)),
            ("addr_count_bool", lambda: method(address, count, False)),
            ("device_id_only", lambda: method(address, count, device_id=self.unit)),
        )
        for i, (name, attempt) in enumerate(attempts, start=1):
            try:
                return attempt()
            except Exception:
                # log each fallback failure concisely to aid diagnosis
                try:
                    if getattr(self, 'diag_callback', None):
                        try:
                            self.diag_callback(
                                f"FALLBACK_FAIL[{i}:{name}]: method={getattr(method, '__name__', repr(method))} exc={traceback.format_exc().splitlines()[-1]}"
                            )
                        except Exception:
                            pass
                except Exception:
                    pass
                continue
        raise RuntimeError("Unable to call modbus method")

    async def read_async(self, address: int, count: int, function_code: int, encoding: Optional[dict] = None):
        encoding = encoding or {}
        if self.mode in ("tcp", "overtcp"):
            if ModbusTcpClient is None:
                raise ImportError("pymodbus required for TCP")

            def _sync_read():
                if not self._client:
                    # connect synchronously if needed
                    self.connect()
                try:
                    setattr(self._client, "timeout", float(self.request_timeout))
                except Exception:
                    pass
                if function_code == 1:
                    return self._call_method_flexible(self._client.read_coils, address, count)
                if function_code == 2:
                    return self._call_method_flexible(self._client.read_discrete_inputs, address, count)
                if function_code == 3:
                    return self._call_method_flexible(self._client.read_holding_registers, address, count)
                if function_code == 4:
                    return self._call_method_flexible(self._client.read_input_registers, address, count)
                raise ValueError("Unsupported function code")

            result = await asyncio.to_thread(_sync_read)

        elif self.mode == "rtu":
            if ModbusSerialClient is None:
                raise ImportError("pymodbus required for RTU")

            def _sync_read():
                if not self._client:
                    self.connect()
                try:
                    setattr(self._client, "timeout", float(self.request_timeout))
                except Exception:
                    pass
                if function_code == 1:
                    return self._call_method_flexible(self._client.read_coils, address, count)
                if function_code == 2:
                    return self._call_method_flexible(self._client.read_discrete_inputs, address, count)
                if function_code == 3:
                    return self._call_method_flexible(self._client.read_holding_registers, address, count)
                if function_code == 4:
                    return self._call_method_flexible(self._client.read_input_registers, address, count)
                raise ValueError("Unsupported function code")

            result = await asyncio.to_thread(_sync_read)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

        # attach normalized data_bytes when registers present
        try:
            if result and not getattr(result, "isError", lambda: False)():
                if hasattr(result, "registers") and result.registers is not None:
                    regs = result.registers
                    b = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs)
                    try:
                        setattr(result, "data_bytes", b)
                    except Exception:
                        pass
                elif hasattr(result, "bits") and result.bits is not None:
                    bits = result.bits
                    bb = bytearray()
                    cur = 0
                    for i, bit in enumerate(bits):
                        if bit:
                            cur |= (1 << (i % 8))
                        if (i % 8) == 7:
                            bb.append(cur)
                            cur = 0
                    if len(bits) % 8:
                        bb.append(cur)
                    try:
                        setattr(result, "data_bytes", bytes(bb))
                    except Exception:
                        pass
        except Exception:
            pass

        if not result or getattr(result, "isError", lambda: False)():
            raise IOError("Modbus read failed")

        return result

    # simple async write wrappers used by controllers
    async def write_register_async(self, address: int, value: int):
        def _sync():
            if self._client is None:
                raise RuntimeError("client not connected")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "write_register", None)
            if m is None:
                raise AttributeError("Underlying client has no write_register")
            try:
                return m(address, int(value))
            except TypeError:
                return m(address, int(value), unit=self.unit)

        return await asyncio.to_thread(_sync)

    async def write_registers_async(self, address: int, values: list):
        def _sync():
            if self._client is None:
                raise RuntimeError("client not connected")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "write_registers", None) or getattr(self._client, "write_multiple_registers", None)
            if m is None:
                raise AttributeError("Underlying client has no write_registers")
            try:
                return m(address, list(values))
            except TypeError:
                return m(address, list(values), unit=self.unit)

        return await asyncio.to_thread(_sync)

    async def write_coil_async(self, address: int, value: bool):
        def _sync():
            if self._client is None:
                raise RuntimeError("client not connected")
            m = getattr(self._client, "write_coil", None)
            if m is None:
                raise AttributeError("Underlying client has no write_coil")
            try:
                return m(address, bool(value))
            except TypeError:
                return m(address, bool(value), unit=self.unit)

        return await asyncio.to_thread(_sync)

    async def read_async(self, address: int, count: int, function_code: int, encoding: dict | None = None):
        if self.mode in ("tcp", "overtcp"):
            if ModbusTcpClient is None:
                msg = "pymodbus is required for ModbusClient (TCP)."
                try:
                    msg += " Import attempts: " + ", ".join(f"{p}:{e}" for p, e in _import_errors)
                except Exception:
                    pass
                raise ImportError(msg)

            def _sync_read():
                if not self._client:
                    self.connect()
                # ensure client's timeout for socket/read operations is the configured request_timeout
                try:
                    setattr(self._client, "timeout", float(self.request_timeout))
                except Exception:
                    pass
                if function_code == 1:
                    return self._call_method_flexible(self._client.read_coils, address, count)
                elif function_code == 2:
                    return self._call_method_flexible(self._client.read_discrete_inputs, address, count)
                elif function_code == 3:
                    return self._call_method_flexible(self._client.read_holding_registers, address, count)
                elif function_code == 4:
                    return self._call_method_flexible(self._client.read_input_registers, address, count)
                else:
                    raise ValueError("Unsupported function code: %r" % (function_code,))

            result = await asyncio.to_thread(_sync_read)

        elif self.mode == "rtu":
            if ModbusSerialClient is None:
                raise ImportError("pymodbus is required for ModbusClient (RTU).")

            def _sync_read():
                if not self._client:
                    self.connect()
                try:
                    setattr(self._client, "timeout", float(self.request_timeout))
                except Exception:
                    pass
                if function_code == 1:
                    return self._call_method_flexible(self._client.read_coils, address, count)
                elif function_code == 2:
                    return self._call_method_flexible(self._client.read_discrete_inputs, address, count)
                elif function_code == 3:
                    return self._call_method_flexible(self._client.read_holding_registers, address, count)
                elif function_code == 4:
                    return self._call_method_flexible(self._client.read_input_registers, address, count)
                else:
                    raise ValueError("Unsupported function code: %r" % (function_code,))

            result = await asyncio.to_thread(_sync_read)

        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

        # Attach normalized data_bytes to the result when possible so callers
        # can inspect raw bytes after applying device encoding options.
        try:
            if result and not getattr(result, "isError", lambda: False)():
                # registers -> normalized bytes
                if hasattr(result, "registers") and result.registers is not None:
                    try:
                        data_bytes = self._normalize_register_bytes(result.registers, encoding or {})
                        try:
                            setattr(result, "data_bytes", data_bytes)
                        except Exception:
                            pass
                    except Exception:
                        pass
                # bits -> assemble into bytes
                elif hasattr(result, "bits") and result.bits is not None:
                    try:
                        bits = result.bits
                        bdata = bytearray()
                        byte_val = 0
                        for i, bit in enumerate(bits):
                            if bit:
                                byte_val |= (1 << (i % 8))
                            if (i % 8) == 7:
                                bdata.append(byte_val)
                                byte_val = 0
                        if len(bits) % 8:
                            bdata.append(byte_val)
                        try:
                            setattr(result, "data_bytes", bytes(bdata))
                        except Exception:
                            pass
                    except Exception:
                        pass
        except Exception:
            pass

        if not result or getattr(result, "isError", lambda: False)():
            raise IOError(f"Modbus read failed: {result}")

        # Emit RX diagnostic (MBAP for TCP, RTU ADU for RTU) if requested
        try:
            # If pymodbus trace_packet is active we already receive real wire bytes
            if self.diag_callback and not getattr(self, '_trace_enabled', False):
                # build pdu from result
                try:
                    if hasattr(result, "bits") and result.bits is not None:
                        bits = result.bits
                        bdata = bytearray()
                        cur = 0
                        for i, bit in enumerate(bits):
                            if bit:
                                cur |= (1 << (i % 8))
                            if (i % 8) == 7:
                                bdata.append(cur)
                                cur = 0
                        if len(bits) % 8:
                            bdata.append(cur)
                        pdu = bytes([function_code, len(bdata)]) + bytes(bdata)
                    elif hasattr(result, "data_bytes") and result.data_bytes is not None:
                        db = result.data_bytes
                        pdu = bytes([function_code, len(db)]) + db
                    elif hasattr(result, "registers") and result.registers is not None:
                        regs = result.registers
                        db = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs)
                        pdu = bytes([function_code, len(db)]) + db
                    else:
                        # fallback to textual representation
                        pdu = str(result).encode("utf-8")
                except Exception:
                    pdu = str(result).encode("utf-8")

                mode_s = (self.mode or "tcp").lower()
                if mode_s == "rtu":
                    # build RTU adu: unit + pdu + crc16 (little-endian)
                    try:
                        adu = bytes([int(self.unit)]) + pdu

                        def _crc16(data: bytes) -> int:
                            crc = 0xFFFF
                            for b in data:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            return crc & 0xFFFF

                        crc = _crc16(adu)
                        crc_bytes = crc.to_bytes(2, "little")
                        adu_rt = adu + crc_bytes
                        hex_rx = " ".join(f"{b:02X}" for b in adu_rt)
                    except Exception:
                        hex_rx = "".join(f"{b:02X}" for b in pdu)
                else:
                    # MBAP header: txid(2) proto(2) len(2) unit(1)
                    try:
                        txid = 0
                        proto = 0
                        mbap_len = len(pdu) + 1
                        mbap = txid.to_bytes(2, "big") + proto.to_bytes(2, "big") + mbap_len.to_bytes(2, "big") + int(self.unit).to_bytes(1, "big")
                        adu = mbap + pdu
                        hex_rx = " ".join(f"{b:02X}" for b in adu)
                    except Exception:
                        hex_rx = " ".join(f"{b:02X}" for b in pdu)

                try:
                    self.diag_callback(f"RX: | {hex_rx} |")
                except Exception:
                    pass
        except Exception:
            pass

        return result

    # --- async write implementations ---
    async def write_register_async(self, address: int, value: int):
        def _sync():
            if self._client is None:
                raise RuntimeError("ModbusClient not connected - call connect_async() first")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            # try common signatures
            m = getattr(self._client, "write_register", None)
            if m is None:
                raise AttributeError("Underlying client has no write_register")
            try:
                # persist a call-log so writes are visible even if UI filters hide them
                try:
                    from datetime import datetime as _dt
                    t = _dt.now()
                    ms = int(t.microsecond / 1000)
                    ts = f"{t.strftime('%H:%M:%S')}.{ms:03d}"
                    with open("Diagnostics.txt", "a", encoding="utf-8") as _f:
                        _f.write(f"{ts}\tWRITE_CALL: FC06 addr={address} value={value}\n")
                except Exception:
                    pass
                return m(address, value)
            except TypeError:
                try:
                    return m(address, int(value), unit=self.unit)
                except Exception:
                    return m(int(address), int(value))

        return await asyncio.to_thread(_sync)

    async def write_registers_async(self, address: int, values: list):
        def _sync():
            # 确保 client 已连接（应该已经在 connect_async() 中连接）
            if self._client is None:
                raise RuntimeError("ModbusClient not connected - call connect_async() first")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "write_registers", None) or getattr(self._client, "write_multiple_registers", None)
            if m is None:
                raise AttributeError("Underlying client has no write_registers")
            
            # 發送診斷回調（僅保留可讀訊息；實際的 TX/RX bytes 由 pymodbus trace_packet 提供）
            try:
                # always persist a write-call log to file so we can verify writes
                try:
                    val_hex = ' '.join(f'{v:04X}' for v in values)
                    from datetime import datetime as _dt
                    t = _dt.now()
                    ms = int(t.microsecond / 1000)
                    ts = f"{t.strftime('%H:%M:%S')}.{ms:03d}"
                    with open("Diagnostics.txt", "a", encoding="utf-8") as _f:
                        _f.write(f"{ts}\tWRITE_CALL: FC16 addr={address} values={val_hex}\n")
                except Exception:
                    pass
                if self.diag_callback:
                    try:
                        # keep a human-readable diag as well (may be filtered by UI)
                        self.diag_callback(f"[WRITE_REGS] write_registers(addr={address}, values={values})")
                    except Exception:
                        pass
            except Exception:
                pass
            
            try:
                result = m(address, list(values))
            except TypeError:
                try:
                    result = m(address, list(values), unit=self.unit)
                except Exception:
                    result = m(int(address), list(values))

            return result

        return await asyncio.to_thread(_sync)

    async def write_coil_async(self, address: int, value: bool):
        def _sync():
            if self._client is None:
                raise RuntimeError("ModbusClient not connected - call connect_async() first")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "write_coil", None)
            if m is None:
                raise AttributeError("Underlying client has no write_coil")
            try:
                try:
                    from datetime import datetime as _dt
                    t = _dt.now()
                    ms = int(t.microsecond / 1000)
                    ts = f"{t.strftime('%H:%M:%S')}.{ms:03d}"
                    with open("Diagnostics.txt", "a", encoding="utf-8") as _f:
                        _f.write(f"{ts}\tWRITE_CALL: FC05 addr={address} value={int(bool(value))}\n")
                except Exception:
                    pass
                return m(address, bool(value))
            except TypeError:
                try:
                    return m(address, bool(value), unit=self.unit)
                except Exception:
                    return m(int(address), bool(value))

        return await asyncio.to_thread(_sync)

    async def write_coils_async(self, address: int, values: list):
        def _sync():
            if self._client is None:
                raise RuntimeError("ModbusClient not connected - call connect_async() first")
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "write_coils", None) or getattr(self._client, "write_multiple_coils", None)
            if m is None:
                raise AttributeError("Underlying client has no write_coils")
            try:
                return m(address, list(values))
            except TypeError:
                try:
                    return m(address, list(values), unit=self.unit)
                except Exception:
                    return m(int(address), list(values))

        return await asyncio.to_thread(_sync)

    async def mask_write_register_async(self, address: int, and_mask: int, or_mask: int):
        def _sync():
            if not self._client:
                self.connect()
            try:
                setattr(self._client, "timeout", float(self.request_timeout))
            except Exception:
                pass
            m = getattr(self._client, "mask_write_register", None) or getattr(self._client, "mask_write", None)
            if m is None:
                raise AttributeError("Underlying client has no mask_write_register")
            try:
                return m(address, int(and_mask), int(or_mask))
            except TypeError:
                try:
                    return m(address, int(and_mask), int(or_mask), unit=self.unit)
                except Exception:
                    return m(int(address), int(and_mask), int(or_mask))

        return await asyncio.to_thread(_sync)
