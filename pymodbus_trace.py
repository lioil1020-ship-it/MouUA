"""Custom trace installer for pymodbus clients.

Implements a conservative, dependency-free way to monitor all TX/RX
traffic by wrapping underlying socket/serial methods after the client
has connected. This complements pymodbus' own trace facilities and
works across common pymodbus client implementations.

The installer tries to wrap `send`, `sendall`, `recv` on sockets and
`write`/`read` on serial-like objects. It stores originals on the
client object so it can be uninstalled safely.
"""
from __future__ import annotations

import types
from typing import Callable
import weakref
import inspect
import asyncio
import socket

# global registry mapping socket instance id -> diag_callback
_GLOBAL_SOCKET_DIAG_REG = {}
# optional map to hold weakref finalizers so we can cancel/cleanup when uninstalling
_GLOBAL_SOCKET_FINALIZERS = {}
_GLOBAL_ADDR_DIAG_REG = {}
_GLOBAL_SOCKET_IN_SENDALL: dict = {}


def register_addr_diag(host, port, diag_callback):
    try:
        if host is None or port is None or diag_callback is None:
            return
        _GLOBAL_ADDR_DIAG_REG[(str(host), int(port))] = diag_callback
    except Exception:
        pass


def _remove_sock_mapping(sid):
    try:
        _GLOBAL_SOCKET_DIAG_REG.pop(sid, None)
    except Exception:
        pass
    try:
        f = _GLOBAL_SOCKET_FINALIZERS.pop(sid, None)
        if f is not None:
            try:
                f.detach()
            except Exception:
                pass
    except Exception:
        pass

def _hexify(b: bytes) -> str:
    try:
        return " ".join(f"{x:02X}" for x in b)
    except Exception:
        try:
            return str(b)
        except Exception:
            return "<unprintable>"


def install_trace_for_client(client, diag_callback: Callable[[str], None]):
    """Install conservative wrappers to observe TX/RX bytes on `client`.

    The function attempts multiple common locations for underlying
    transport objects and wraps their send/recv or write/read methods.
    It stores original callables on `client._trace_orig` for uninstallation.
    """
    if client is None or diag_callback is None:
        return

    orig = {}
    installed_any = False
    addr_registered = False
    # accumulation buffer for RX fragments so we can emit a growing,
    # assembled RX payload instead of many small fragments.
    try:
        client._trace_accum = {'rx': bytearray(), 'max_len': 8192}
    except Exception:
        pass

    def _emit(side: str, data_bytes: bytes):
        try:
            if data_bytes is None:
                return
            diag_callback(f"{side}: | {_hexify(data_bytes)} |")
        except Exception:
            pass

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

    def _process_accumulator(acc, client_obj):
        """Try to extract complete Modbus frames from acc['rx'] and emit them.

        For TCP (MBAP) we use the length field (bytes 4-5) -> total = 6 + length.
        For RTU we attempt CRC16 detection: find a split where last two bytes
        match CRC16 of preceding bytes.
        """
        buf = acc.get('rx', bytearray())
        max_len = acc.get('max_len', 8192)
        mode = None
        try:
            mode = getattr(client_obj, 'mode', None)
            if mode:
                mode = (mode or '').lower()
        except Exception:
            mode = None

        emitted_any = False
        # loop extracting frames
        while True:
            if not buf:
                break
            # MBAP / TCP
            if mode == 'tcp' or mode == 'overtcp':
                if len(buf) < 7:
                    break
                # length is bytes 4-5
                length = (buf[4] << 8) | buf[5]
                total = 6 + int(length)
                if len(buf) < total:
                    break
                frame = bytes(buf[:total])
                try:
                    _emit('RX', frame)
                except Exception:
                    pass
                buf = buf[total:]
                emitted_any = True
                continue

            # RTU: try CRC-based frame boundary detection
            if mode == 'rtu' or mode == 'serial':
                # need at least unit(1) + func(1) + crc(2)
                if len(buf) < 4:
                    break
                found = False
                # try to find a frame end where CRC matches
                # search from minimal frame length up to current buffer
                for l in range(4, len(buf) + 1):
                    candidate = bytes(buf[:l])
                    if len(candidate) < 4:
                        continue
                    body = candidate[:-2]
                    crc_bytes = candidate[-2:]
                    if _crc16(body) == int.from_bytes(crc_bytes, 'little'):
                        try:
                            _emit('RX', candidate)
                        except Exception:
                            pass
                        buf = buf[l:]
                        emitted_any = True
                        found = True
                        break
                if not found:
                    # no valid frame yet; if buffer grows too large, flush best-effort
                    if len(buf) > max_len:
                        try:
                            _emit('RX', bytes(buf))
                        except Exception:
                            pass
                        buf = bytearray()
                        emitted_any = True
                    break
                continue

            # Unknown mode: flush entire buffer as a single RX emit if non-empty
            try:
                _emit('RX', bytes(buf))
            except Exception:
                pass
            buf = bytearray()
            emitted_any = True

        # update accumulator
        acc['rx'] = bytearray(buf)
        # keep sized
        if len(acc['rx']) > max_len:
            acc['rx'] = acc['rx'][-max_len:]
        return emitted_any

    def _wrap_send(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                # avoid duplicate emits when sendall wrapper already emitted
                try:
                    sid = id(getattr(data, '___dummy', None))
                except Exception:
                    sid = None
                try:
                    # when bound via types.MethodType, `self` is available as first arg in kwargs via closure; fallback to None
                    pass
                except Exception:
                    pass
                # Do not clear the RX accumulator here; allow parser to
                # assemble based on protocol framing (MBAP/CRC).
                try:
                    # attempt to obtain socket self from bound method via kwargs inspection
                    sock_self = None
                    if args:
                        # when called as sock.send(data), args empty; but when bound, the wrapper is bound to sock via MethodType
                        pass
                    # derive socket id from closure if possible
                except Exception:
                    sock_self = None

                try:
                    b = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                except Exception:
                    try:
                        b = bytes(str(data), 'utf-8')
                    except Exception:
                        b = None

                # When sendall wrapper is in progress for this socket, skip emitting here
                try:
                    sock = getattr(orig_fn, '__self__', None)
                    sid = id(sock) if sock is not None else None
                except Exception:
                    sid = None

                if sid is not None and _GLOBAL_SOCKET_IN_SENDALL.get(sid):
                    # skip duplicate emit
                    pass
                else:
                    if b:
                        try:
                            diag_callback(f"TX: | {_hexify(b)} |")
                        except Exception:
                            pass
            except Exception:
                pass
            return orig_fn(data, *args, **kwargs)

        return _wrapped

    def _wrap_sendall(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                try:
                    sock = getattr(orig_fn, '__self__', None)
                    sid = id(sock) if sock is not None else None
                except Exception:
                    sid = None
                try:
                    if sid is not None:
                        _GLOBAL_SOCKET_IN_SENDALL[sid] = True
                except Exception:
                    pass
                try:
                    b = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                except Exception:
                    try:
                        b = bytes(str(data), 'utf-8')
                    except Exception:
                        b = None
                if b:
                    try:
                        diag_callback(f"TX: | {_hexify(b)} |")
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                return orig_fn(data, *args, **kwargs)
            finally:
                try:
                    if sid is not None:
                        _GLOBAL_SOCKET_IN_SENDALL.pop(sid, None)
                except Exception:
                    pass

        return _wrapped

    def _wrap_recv(orig_fn):
        def _wrapped(bufsize, *args, **kwargs):
            data = orig_fn(bufsize, *args, **kwargs)
            try:
                if not data:
                    return data
                try:
                    incoming = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                except Exception:
                    try:
                        incoming = bytes(str(data), 'utf-8')
                    except Exception:
                        incoming = b""

                # append to accumulator
                if hasattr(client, '_trace_accum'):
                    acc = client._trace_accum
                    acc['rx'].extend(incoming)
                    # cap to max_len
                    if len(acc['rx']) > acc.get('max_len', 8192):
                        acc['rx'] = acc['rx'][-acc.get('max_len', 8192):]
                    _process_accumulator(acc, client)
                else:
                    try:
                        diag_callback(f"RX: | {_hexify(incoming)} |")
                    except Exception:
                        pass
            except Exception:
                pass
            return data

        return _wrapped

    def _wrap_read(orig_fn):
        def _wrapped(size=-1, *args, **kwargs):
            data = orig_fn(size, *args, **kwargs)
            try:
                if data:
                    try:
                        if hasattr(client, '_trace_accum') and isinstance(data, (bytes, bytearray)):
                            acc = client._trace_accum
                            acc['rx'].extend(bytes(data))
                            if len(acc['rx']) > acc.get('max_len', 8192):
                                acc['rx'] = acc['rx'][-acc.get('max_len', 8192):]
                            _process_accumulator(acc, client)
                        else:
                            emit = data if isinstance(data, (bytes, bytearray)) else bytes(str(data), 'utf-8')
                    except Exception:
                        emit = data
                    try:
                        if 'emit' in locals() and emit:
                            try:
                                diag_callback(f"RX: | {_hexify(emit if isinstance(emit, (bytes, bytearray)) else bytes(str(emit), 'utf-8'))} |")
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
            return data

        return _wrapped

    def _wrap_write(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                # Do not reset RX accumulator here; allow RX parser to assemble frames
                try:
                    b = data if isinstance(data, (bytes, bytearray)) else bytes(str(data), 'utf-8')
                except Exception:
                    b = None
                if b:
                    diag_callback(f"TX: | {_hexify(b)} |")
            except Exception:
                pass
            return orig_fn(data, *args, **kwargs)

        return _wrapped

    # Try common socket attribute locations
    try:
        sock = getattr(client, "socket", None)
        if sock is None:
            # some clients keep the socket under "_socket" or "_sock"
            sock = getattr(client, "_socket", None) or getattr(client, "_sock", None)
        if sock is not None:
            # wrap send/sendall/recv if present and not already wrapped
            if hasattr(sock, "send") and not getattr(sock, "_trace_wrapped_send", False):
                orig['sock_send'] = sock.send
                sock.send = types.MethodType(_wrap_send(sock.send), sock)
                sock._trace_wrapped_send = True
                installed_any = True
            if hasattr(sock, "sendall") and not getattr(sock, "_trace_wrapped_sendall", False):
                orig['sock_sendall'] = sock.sendall
                sock.sendall = types.MethodType(_wrap_sendall(sock.sendall), sock)
                sock._trace_wrapped_sendall = True
                installed_any = True
            if hasattr(sock, "recv") and not getattr(sock, "_trace_wrapped_recv", False):
                orig['sock_recv'] = sock.recv
                sock.recv = types.MethodType(_wrap_recv(sock.recv), sock)
                sock._trace_wrapped_recv = True
                installed_any = True
            # Do NOT register to the global socket/addr registry when
            # instance-level wrappers were successfully attached. The
            # global monkeypatch emits per-recv fragments which can
            # duplicate the instance-level accumulated emits. If no
            # instance wrapper is possible, a fallback registration will
            # be performed later so the global wrapper can find the
            # diag callback by address.
    except Exception:
        pass

    # Fallback global monkeypatch: if instance-level assignment to the
    # socket instance failed (C-level methods), try wrapping on the
    # socket.socket class so newly-created sockets are traced too.
    try:
        try:
            s_mod = socket
            # Only install once per process
            if not getattr(s_mod.socket, '_trace_wrapped_global', False):
                try:
                    orig_send = s_mod.socket.send
                except Exception:
                    orig_send = None
                try:
                    orig_sendall = s_mod.socket.sendall
                except Exception:
                    orig_sendall = None
                try:
                    orig_recv = s_mod.socket.recv
                except Exception:
                    orig_recv = None

                if orig_send is not None:
                    def _global_send(self, data, *args, __orig=orig_send, **kwargs):
                        try:
                            try:
                                b = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                            except Exception:
                                try:
                                    b = bytes(str(data), 'utf-8')
                                except Exception:
                                    b = None
                            if b:
                                try:
                                    cb = _GLOBAL_SOCKET_DIAG_REG.get(id(self))
                                    if cb is None:
                                        # try to find by peername or sockname
                                        try:
                                            peer = None
                                            try:
                                                peer = self.getpeername()
                                            except Exception:
                                                try:
                                                    peer = self.getsockname()
                                                except Exception:
                                                    peer = None
                                            if peer and isinstance(peer, tuple):
                                                cb = _GLOBAL_ADDR_DIAG_REG.get((str(peer[0]), int(peer[1])))
                                        except Exception:
                                            pass
                                    if cb:
                                        cb(f"TX: | {_hexify(b)} |")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        return __orig(self, data, *args, **kwargs)

                    try:
                        s_mod.socket.send = _global_send
                    except Exception:
                        pass

                if orig_sendall is not None:
                    def _global_sendall(self, data, *args, __orig=orig_sendall, **kwargs):
                        try:
                            try:
                                b = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                            except Exception:
                                try:
                                    b = bytes(str(data), 'utf-8')
                                except Exception:
                                    b = None
                            if b:
                                try:
                                    cb = _GLOBAL_SOCKET_DIAG_REG.get(id(self))
                                    if cb is None:
                                        try:
                                            peer = None
                                            try:
                                                peer = self.getpeername()
                                            except Exception:
                                                try:
                                                    peer = self.getsockname()
                                                except Exception:
                                                    peer = None
                                            if peer and isinstance(peer, tuple):
                                                cb = _GLOBAL_ADDR_DIAG_REG.get((str(peer[0]), int(peer[1])))
                                        except Exception:
                                            pass
                                    if cb:
                                        cb(f"TX: | {_hexify(b)} |")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        return __orig(self, data, *args, **kwargs)

                    try:
                        s_mod.socket.sendall = _global_sendall
                    except Exception:
                        pass

                if orig_recv is not None:
                    def _global_recv(self, bufsize, *args, __orig=orig_recv, **kwargs):
                        data = __orig(self, bufsize, *args, **kwargs)
                        try:
                            if data:
                                try:
                                    incoming = data if isinstance(data, (bytes, bytearray)) else bytes(data)
                                except Exception:
                                    try:
                                        incoming = bytes(str(data), 'utf-8')
                                    except Exception:
                                        incoming = b""
                                try:
                                    cb = _GLOBAL_SOCKET_DIAG_REG.get(id(self))
                                    if cb is None:
                                        try:
                                            peer = None
                                            try:
                                                peer = self.getpeername()
                                            except Exception:
                                                try:
                                                    peer = self.getsockname()
                                                except Exception:
                                                    peer = None
                                            if peer and isinstance(peer, tuple):
                                                cb = _GLOBAL_ADDR_DIAG_REG.get((str(peer[0]), int(peer[1])))
                                        except Exception:
                                            pass
                                    if cb:
                                        cb(f"RX: | {_hexify(incoming)} |")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        return data

                    try:
                        s_mod.socket.recv = _global_recv
                    except Exception:
                        pass

                try:
                    setattr(s_mod.socket, '_trace_wrapped_global', True)
                    installed_any = True
                except Exception:
                    pass

                # record originals so uninstall can restore
                try:
                    orig['global_sock_send'] = orig_send
                except Exception:
                    pass
                try:
                    orig['global_sock_sendall'] = orig_sendall
                except Exception:
                    pass
                try:
                    orig['global_sock_recv'] = orig_recv
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        pass

    # Try serial-like attribute (pyserial)
    try:
        ser = getattr(client, "serial", None) or getattr(client, "_serial", None) or getattr(client, "transport", None)
        if ser is not None:
            if hasattr(ser, "write") and not getattr(ser, "_trace_wrapped_write", False):
                orig['ser_write'] = ser.write
                ser.write = types.MethodType(_wrap_write(ser.write), ser)
                ser._trace_wrapped_write = True
                installed_any = True
            if hasattr(ser, "read") and not getattr(ser, "_trace_wrapped_read", False):
                orig['ser_read'] = ser.read
                ser.read = types.MethodType(_wrap_read(ser.read), ser)
                ser._trace_wrapped_read = True
                installed_any = True
    except Exception:
        pass

    # Fallback: some pymodbus variants accept `trace_packet` callback attribute
    try:
        if hasattr(client, 'trace_packet') and not getattr(client, '_trace_wrapped_trace_packet', False):
            try:
                orig['trace_packet'] = client.trace_packet
            except Exception:
                orig['trace_packet'] = None
            try:
                client.trace_packet = lambda sending, data: diag_callback(f"{('TX' if sending else 'RX')}: | {_hexify(data)} |")
                client._trace_wrapped_trace_packet = True
                installed_any = True
            except Exception:
                pass
    except Exception:
        pass

    # Try wrapping coroutine-based send/recv (e.g. websockets.WebSocketClientProtocol)
    try:
        # candidate objects to inspect for async send/recv
        candidates = []
        try:
            candidates.append(client)
        except Exception:
            pass
        try:
            if hasattr(client, '_client'):
                candidates.append(getattr(client, '_client'))
        except Exception:
            pass
        try:
            if hasattr(client, 'protocol'):
                candidates.append(getattr(client, 'protocol'))
        except Exception:
            pass

        for cand in candidates:
            if cand is None:
                continue
            # wrap async send if present
            try:
                if hasattr(cand, 'send') and inspect.iscoroutinefunction(getattr(cand, 'send')) and not getattr(cand, '_trace_wrapped_async_send', False):
                    orig_send = getattr(cand, 'send')

                    async def _async_send_wrapper(data, *args, __orig=orig_send, **kwargs):
                        try:
                            b = None
                            if isinstance(data, (bytes, bytearray)):
                                b = data
                            elif isinstance(data, str):
                                try:
                                    b = data.encode('utf-8')
                                except Exception:
                                    b = None
                            else:
                                try:
                                    b = bytes(data)
                                except Exception:
                                    b = None
                            if b:
                                diag_callback(f"TX: | {_hexify(b)} |")
                        except Exception:
                            pass
                        return await __orig(data, *args, **kwargs)

                    try:
                        cand.send = _async_send_wrapper
                        cand._trace_wrapped_async_send = True
                        orig['async_send'] = orig_send
                        installed_any = True
                    except Exception:
                        pass
                # wrap async recv if present
                if hasattr(cand, 'recv') and inspect.iscoroutinefunction(getattr(cand, 'recv')) and not getattr(cand, '_trace_wrapped_async_recv', False):
                    orig_recv = getattr(cand, 'recv')

                    async def _async_recv_wrapper(*args, __orig=orig_recv, **kwargs):
                        data = await __orig(*args, **kwargs)
                        try:
                            if data is not None:
                                b = None
                                if isinstance(data, (bytes, bytearray)):
                                    b = data
                                elif isinstance(data, str):
                                    try:
                                        b = data.encode('utf-8')
                                    except Exception:
                                        b = None
                                else:
                                    try:
                                        b = bytes(data)
                                    except Exception:
                                        b = None
                                if b:
                                    # append to accumulator if present
                                    if hasattr(client, '_trace_accum'):
                                        try:
                                            client._trace_accum['rx'].extend(b)
                                            _process_accumulator(client._trace_accum, client)
                                        except Exception:
                                            pass
                                    else:
                                        diag_callback(f"RX: | {_hexify(b)} |")
                        except Exception:
                            pass
                        return data

                    try:
                        cand.recv = _async_recv_wrapper
                        cand._trace_wrapped_async_recv = True
                        orig['async_recv'] = orig_recv
                        installed_any = True
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass

    # store originals so uninstall can restore
    try:
        client._trace_orig = orig
    except Exception:
        pass
    # If we did not manage to attach any instance-level wrappers, register
    # this client's host:port in the global addr registry so the global
    # monkeypatch can route emits to the provided diag_callback.
    try:
        if not installed_any:
            try:
                chost = getattr(client, 'host', None)
                cport = getattr(client, 'port', None)
                if chost is not None and cport is not None:
                    try:
                        _GLOBAL_ADDR_DIAG_REG[(str(chost), int(cport))] = diag_callback
                        addr_registered = True
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass
    # ensure accumulator present
    try:
        # mark client as having trace installed when either instance-level
        # wrappers were attached or we registered a host:port in the global
        # registry. This ensures callers that avoid emitting synthetic TX/RX
        # will not duplicate diagnostics when the global fallback is used.
        if installed_any or addr_registered:
            try:
                client._trace_accum = {'rx': bytearray(), 'max_len': 8192}
            except Exception:
                pass
            try:
                client._trace_installed = True
            except Exception:
                pass
        else:
            try:
                client._trace_installed = False
            except Exception:
                pass
    except Exception:
        pass
    # Emit a small diagnostic so caller can see that transport-level tracing is active
    try:
        if installed_any and diag_callback:
            try:
                # if we have a socket, report its id for debugging
                s = getattr(client, 'socket', None) or getattr(client, '_socket', None) or getattr(client, '_sock', None)
                sid = id(s) if s is not None else None
                diag_callback(f"TRACE_INSTALLED: socket_id={sid} installed=True")
            except Exception:
                try:
                    diag_callback("TRACE_INSTALLED: installed=True")
                except Exception:
                    pass
    except Exception:
        pass
    # Emit additional details about what was wrapped/registered
    try:
        if installed_any and diag_callback:
            try:
                wrapped = list(orig.keys())
                s = getattr(client, 'socket', None) or getattr(client, '_socket', None) or getattr(client, '_sock', None)
                sid = id(s) if s is not None else None
                chost = getattr(client, 'host', None)
                cport = getattr(client, 'port', None)
                addr_reg = (str(chost), int(cport)) if (chost is not None and cport is not None) else None
                addr_registered = addr_reg in _GLOBAL_ADDR_DIAG_REG if addr_reg is not None else False
                sock_registered = sid in _GLOBAL_SOCKET_DIAG_REG if sid is not None else False
                diag_callback(f"TRACE_DETAILS: wrapped={wrapped} socket_id={sid} sock_registered={sock_registered} addr_registered={addr_registered}")
            except Exception:
                pass
    except Exception:
        pass


def uninstall_trace_for_client(client):
    """Restore previously-wrapped methods if present."""
    if client is None:
        return
    orig = getattr(client, '_trace_orig', None)
    if not orig:
        return

    try:
        sock = getattr(client, 'socket', None) or getattr(client, '_socket', None) or getattr(client, '_sock', None)
        if sock is not None:
            try:
                if 'sock_send' in orig:
                    sock.send = orig['sock_send']
            except Exception:
                pass
            try:
                if 'sock_sendall' in orig:
                    sock.sendall = orig['sock_sendall']
            except Exception:
                pass
            try:
                if 'sock_recv' in orig:
                    sock.recv = orig['sock_recv']
            except Exception:
                pass
            try:
                # remove mapping for this socket
                try:
                    _remove_sock_mapping(id(sock))
                except Exception:
                    pass
            except Exception:
                pass
    except Exception:
        pass

    try:
        ser = getattr(client, 'serial', None) or getattr(client, '_serial', None) or getattr(client, 'transport', None)
        if ser is not None:
            try:
                if 'ser_write' in orig:
                    ser.write = orig['ser_write']
            except Exception:
                pass
            try:
                if 'ser_read' in orig:
                    ser.read = orig['ser_read']
            except Exception:
                pass
    except Exception:
        pass

    try:
        if 'trace_packet' in orig and orig.get('trace_packet') is not None:
            try:
                client.trace_packet = orig.get('trace_packet')
            except Exception:
                pass
    except Exception:
        pass

    try:
        del client._trace_orig
    except Exception:
        pass
    try:
        if hasattr(client, '_trace_accum'):
            del client._trace_accum
    except Exception:
        pass
    try:
        if hasattr(client, '_trace_installed'):
            try:
                client._trace_installed = False
            except Exception:
                pass
    except Exception:
        pass
    # also remove any remaining registry entries that reference this client's callback
    try:
        try:
            cb = getattr(client, 'diag_callback', None)
        except Exception:
            cb = None
        if cb is not None:
            # remove all entries whose callback equals this client's diag_callback
            to_del = [sid for sid, v in list(_GLOBAL_SOCKET_DIAG_REG.items()) if v == cb]
            for sid in to_del:
                try:
                    _remove_sock_mapping(sid)
                except Exception:
                    pass
    except Exception:
        pass
