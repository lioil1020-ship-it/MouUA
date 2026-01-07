import time, os, sys, traceback
from opcua import Client

# ensure project root on sys.path
root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root not in sys.path:
    sys.path.insert(0, root)

try:
    from OPC_UA import OPCServer
except Exception as e:
    print('Failed to import OPC_UA.OPCServer:', e)
    raise

PORT = int(os.environ.get('OPC_TEST_PORT', '48480'))
cfg = {'application_Name': 'ModUA', 'host_name': '127.0.0.1', 'port': PORT, 'namespace': 'ModUA'}

print('Starting headless OPC UA server with config:', cfg)
server = None
try:
    server = OPCServer(cfg)
    server.start()
    time.sleep(1.0)
except Exception as e:
    print('Failed to start server:', e)
    traceback.print_exc()

endpoints = [f'opc.tcp://127.0.0.1:{PORT}', f'opc.tcp://localhost:{PORT}']
for ep in endpoints:
    print('\nTrying', ep)
    try:
        client = Client(ep)
        try:
            client.set_timeout(3)
        except Exception:
            pass
        try:
            client.connect()
            print('Connected to', ep)
            client.disconnect()
        except Exception as e:
            print('Connect failed:', e)
    except Exception as e:
        print('Client init failed:', e)

print('\nListing listening TCP ports (psutil):')
try:
    import psutil
    for c in psutil.net_connections(kind='inet'):
        if c.status == 'LISTEN':
            laddr = f'{c.laddr.ip}:{c.laddr.port}' if c.laddr else str(c.laddr)
            pid = c.pid
            pname = ''
            try:
                pname = psutil.Process(pid).name() if pid else ''
            except Exception:
                pass
            print(f'PID={pid} name={pname} addr={laddr}')
except Exception as e:
    print('psutil listing failed:', e)

if server:
    print('\nStopping server...')
    try:
        server.stop()
        print('Server stopped')
    except Exception as e:
        print('Error stopping server:', e)

print('\nDone')
