import time
import traceback
from opcua import Client

# start OPC server from project module
try:
    import os, sys
    # ensure project root is on sys.path
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if root not in sys.path:
        sys.path.insert(0, root)
    from OPC_UA import OPCServer
except Exception as e:
    print('Failed to import OPC_UA.OPCServer:', e)
    raise

cfg = {'application_Name': 'ModUA', 'host_name': '127.0.0.1', 'port': 4848, 'namespace': 'ModUA'}

print('Starting headless OPC UA server with config:', cfg)
server = None
try:
    server = OPCServer(cfg)
    server.start()
    print('Server.start() called; waiting for server to initialize...')
    time.sleep(1.0)
except Exception as e:
    print('Failed to start server:', e)
    traceback.print_exc()

# client tests
endpoints = [
    'opc.tcp://127.0.0.1:4848',
    'opc.tcp://localhost:4848',
]

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
            try:
                root = client.get_root_node()
                print('Root node found:', root)
            except Exception as e:
                print('Connected but failed to read root:', e)
            try:
                client.disconnect()
            except Exception:
                pass
        except Exception as e:
            print('Connect failed:', e)
    except Exception as e:
        print('Client init failed:', e)

# list listening TCP ports
print('\nListing listening TCP ports (psutil):')
try:
    import psutil
    conns = psutil.net_connections(kind='inet')
    listeners = [c for c in conns if c.status == 'LISTEN']
    for c in listeners:
        laddr = f'{c.laddr.ip}:{c.laddr.port}' if c.laddr else str(c.laddr)
        pid = c.pid
        try:
            pname = psutil.Process(pid).name() if pid else ''
        except Exception:
            pname = ''
        print(f'PID={pid} name={pname} addr={laddr} family={c.family} type={c.type}')
except Exception as e:
    print('psutil listing failed:', e)

# stop server
if server:
    print('\nStopping server...')
    try:
        server.stop()
        print('Server stopped')
    except Exception as e:
        print('Error stopping server:', e)

print('\nDone')
