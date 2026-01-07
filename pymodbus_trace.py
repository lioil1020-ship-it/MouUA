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

    def _wrap_send(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                diag_callback(f"TX: | {_hexify(data)} |")
            except Exception:
                pass
            return orig_fn(data, *args, **kwargs)

        return _wrapped

    def _wrap_sendall(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                diag_callback(f"TX: | {_hexify(data)} |")
            except Exception:
                pass
            return orig_fn(data, *args, **kwargs)

        return _wrapped

    def _wrap_recv(orig_fn):
        def _wrapped(bufsize, *args, **kwargs):
            data = orig_fn(bufsize, *args, **kwargs)
            try:
                if data:
                    diag_callback(f"RX: | {_hexify(data)} |")
            except Exception:
                pass
            return data

        return _wrapped

    def _wrap_read(orig_fn):
        def _wrapped(size=-1, *args, **kwargs):
            data = orig_fn(size, *args, **kwargs)
            try:
                if data:
                    if isinstance(data, (bytes, bytearray)):
                        diag_callback(f"RX: | {_hexify(bytes(data))} |")
                    else:
                        # serial read may return int or str
                        try:
                            diag_callback(f"RX: | {str(data)} |")
                        except Exception:
                            pass
            except Exception:
                pass
            return data

        return _wrapped

    def _wrap_write(orig_fn):
        def _wrapped(data, *args, **kwargs):
            try:
                diag_callback(f"TX: | {_hexify(data if isinstance(data, (bytes, bytearray)) else bytes(str(data), 'utf-8'))} |")
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
            if hasattr(sock, "sendall") and not getattr(sock, "_trace_wrapped_sendall", False):
                orig['sock_sendall'] = sock.sendall
                sock.sendall = types.MethodType(_wrap_sendall(sock.sendall), sock)
                sock._trace_wrapped_sendall = True
            if hasattr(sock, "recv") and not getattr(sock, "_trace_wrapped_recv", False):
                orig['sock_recv'] = sock.recv
                sock.recv = types.MethodType(_wrap_recv(sock.recv), sock)
                sock._trace_wrapped_recv = True
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
            if hasattr(ser, "read") and not getattr(ser, "_trace_wrapped_read", False):
                orig['ser_read'] = ser.read
                ser.read = types.MethodType(_wrap_read(ser.read), ser)
                ser._trace_wrapped_read = True
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
            except Exception:
                pass
    except Exception:
        pass

    # store originals so uninstall can restore
    try:
        client._trace_orig = orig
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
