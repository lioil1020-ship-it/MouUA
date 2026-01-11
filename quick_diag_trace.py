import asyncio
import sys
from modbus_client import ModbusClient

async def main():
    host = '192.168.11.10'
    port = 502
    unit = 1
    print('Creating client...')
    c = ModbusClient(mode='tcp', host=host, port=port, unit=unit, diag_callback=print)
    print('client created, calling connect_async()')
    try:
        ok = await c.connect_async()
        print('connect_async ok=', ok)
    except Exception as e:
        print('connect_async exception:', e)
    # inspect internals
    try:
        cli = getattr(c, '_client', None)
        print('_client:', type(cli))
        print('_transport_trace_installed():', c._transport_trace_installed())
        print("_trace_orig present:", hasattr(cli, '_trace_orig') if cli is not None else False)
        print("_trace_accum present:", hasattr(cli, '_trace_accum') if cli is not None else hasattr(c, '_trace_accum'))
        try:
            sock = getattr(cli, 'socket', None) or getattr(cli, '_socket', None) or getattr(cli, '_sock', None)
            ser = getattr(cli, 'serial', None) or getattr(cli, '_serial', None) or getattr(cli, 'transport', None)
            print('socket obj:', type(sock) if sock is not None else None)
            print('serial obj:', type(ser) if ser is not None else None)
            if sock is not None:
                print('sock._trace_wrapped_send:', getattr(sock, '_trace_wrapped_send', False))
                print('sock._trace_wrapped_recv:', getattr(sock, '_trace_wrapped_recv', False))
            if ser is not None:
                print('ser._trace_wrapped_write:', getattr(ser, '_trace_wrapped_write', False))
                print('ser._trace_wrapped_read:', getattr(ser, '_trace_wrapped_read', False))
        except Exception as e:
            print('inspect transport error:', e)
    except Exception as e:
        print('inspect error:', e)

    # perform a read to trigger TX/RX
    try:
        print('Performing read_async...')
        res = await c.read_async(95, 2, 3)
        print('read complete, res:', res)
        print('res.data_bytes:', getattr(res, 'data_bytes', None))
        try:
            enc = res.encode()
            print('res.encode():', enc)
        except Exception as e:
            print('res.encode() exception:', e)
    except Exception as e:
        print('read_async failed:', e)
    try:
        await c.close_async()
    except Exception:
        pass

if __name__ == '__main__':
    asyncio.run(main())
