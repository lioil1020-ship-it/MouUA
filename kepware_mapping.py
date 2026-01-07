"""Helpers to map Kepware Modbus channel/device parameters to pymodbus client parameters.

This module centralizes conversion rules described in the Gemini mapping notes
(1-based -> 0-based addressing, timeout units, framer choices, byte/word order
naming conventions, serial parameter normalization, attempts/inter-request delay).
"""
from typing import Tuple, Optional, Dict, Any


def map_kepware_to_pymodbus(driver_name: str, ch_params: dict, encoding: dict, block_sizes: dict, data_access: dict, device_timing: dict, host: Optional[str], port: Optional[int]) -> Tuple[str, Optional[str], Optional[int], dict]:
    """Map Kepware channel/device settings to (mode, host_local, port_local, client_params).

    Returns:
      mode: one of 'tcp', 'rtu', 'overtcp'
      host_local: host to pass to TCP client or None for RTU serial
      port_local: port to pass to TCP client or None for RTU serial
      client_params: dict of keyword args to pass to ModbusClient wrapper
    """
    drv = (driver_name or "").strip() if driver_name is not None else ""
    drv_s = drv or "Modbus TCP/IP Ethernet"
    client_params: Dict[str, Any] = {}
    host_local = host
    port_local = port

    # Normalize and attach common device hints
    client_params.setdefault("encoding", encoding or {})
    client_params.setdefault("block_sizes", block_sizes or {})
    client_params.setdefault("data_access", data_access or {})

    # Map timing/attempts
    try:
        attempts_local = int(device_timing.get("attempts", 1)) if device_timing else 1
    except Exception:
        attempts_local = 1
    try:
        inter_ms = int(device_timing.get("inter_req_delay", 0)) if device_timing else 0
    except Exception:
        inter_ms = 0
    client_params.setdefault("attempts", attempts_local)
    client_params.setdefault("inter_req_delay", inter_ms)

    # Driver-specific mapping
    if drv_s == "Modbus RTU Serial":
        mode = "rtu"
        # serial port may be stored under different keys
        ser = None
        for k in ("com", "port", "serial_port", "device"):
            if k in (ch_params or {}):
                ser = ch_params.get(k)
                break
        if not ser:
            ser = host
        client_params["serial_port"] = ser
        try:
            client_params["baudrate"] = int((ch_params or {}).get("baud", 9600))
        except Exception:
            client_params["baudrate"] = 9600
        # passenger through popular serial flags if present
        for src_key, dst_key in (
            ("xonxoff", "xonxoff"),
            ("rtscts", "rtscts"),
            ("dsrdtr", "dsrdtr"),
            ("exclusive", "exclusive"),
        ):
            if (ch_params or {}).get(src_key) is not None:
                client_params[dst_key] = (ch_params or {}).get(src_key)
        # parity/stopbits/bytesize
        if (ch_params or {}).get("parity") is not None:
            p = str((ch_params or {}).get("parity")).strip()
            if p.lower() in ("none", "n"):
                client_params["parity"] = "N"
            elif p.lower().startswith("e"):
                client_params["parity"] = "E"
            elif p.lower().startswith("o"):
                client_params["parity"] = "O"
            else:
                client_params["parity"] = p
        for k in ("stopbits", "stop_bits", "stop"):
            if (ch_params or {}).get(k) is not None:
                try:
                    client_params["stopbits"] = float((ch_params or {}).get(k))
                    break
                except Exception:
                    pass
        for k in ("data_bits", "databits", "bytesize"):
            if (ch_params or {}).get(k) is not None:
                try:
                    client_params["bytesize"] = int((ch_params or {}).get(k))
                    break
                except Exception:
                    pass
        # allow explicit framer override
        if (ch_params or {}).get("framer") is not None:
            client_params["framer"] = (ch_params or {}).get("framer")
        # map flow control UI values to pyserial flags
        try:
            flow_val = (ch_params or {}).get("flow")
            if flow_val is not None:
                fv = str(flow_val).strip().lower()
                if fv == "" or fv == "none":
                    pass
                else:
                    # XON/XOFF textual match
                    if "xon" in fv or "xonxoff" in fv:
                        client_params["xonxoff"] = True
                    # Both RTS and DTR requested
                    if "rts" in fv and "dtr" in fv:
                        client_params["rtscts"] = True
                        client_params["dsrdtr"] = True
                    # RTS variants
                    elif "rts" in fv:
                        client_params["rtscts"] = True
                    # DTR only
                    elif "dtr" in fv:
                        client_params["dsrdtr"] = True
        except Exception:
            pass
        # host_local not used for RTU
        host_local = None
        port_local = None
        return mode, host_local, port_local, client_params

    if drv_s == "Modbus RTU over TCP":
        mode = "overtcp"
        # keep TCP host/port
        return mode, host_local, port_local, client_params

    # default: Modbus TCP/IP Ethernet and variants
    mode = "tcp"
    return mode, host_local, port_local, client_params
