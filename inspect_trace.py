from modbus_client import ModbusClient
import asyncio

async def main():
    c = ModbusClient(mode='tcp', host='192.168.11.10', port=502, unit=1, diag_callback=print)
    await c.connect_async()
    cli = getattr(c, '_client', None)
    print('client type:', type(cli))
    print('trace_orig keys:', list(getattr(cli, '_trace_orig', {}).keys()) if cli else None)
    sock = getattr(cli, 'socket', None) or getattr(cli, '_socket', None) or getattr(cli, '_sock', None)
    print('socket obj type:', type(sock) if sock else None)
    if sock is not None:
        print('has send:', hasattr(sock, 'send'))
        print('has recv:', hasattr(sock, 'recv'))
        print('sock._trace_wrapped_send:', getattr(sock, '_trace_wrapped_send', None))
        print('sock._trace_wrapped_recv:', getattr(sock, '_trace_wrapped_recv', None))
    ser = getattr(cli, 'serial', None) or getattr(cli, '_serial', None) or getattr(cli, 'transport', None)
    print('serial obj type:', type(ser) if ser else None)
    print('client._trace_accum present:', hasattr(cli, '_trace_accum') if cli else False)
    await c.close_async()

if __name__ == '__main__':
    asyncio.run(main())
