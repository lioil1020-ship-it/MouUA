from opcua import Client

endpoints = [
    'opc.tcp://127.0.0.1:4848',
    'opc.tcp://localhost:4848',
    'opc.tcp://0.0.0.0:4848',
]

for ep in endpoints:
    print('---')
    print('Trying', ep)
    try:
        client = Client(ep)
        try:
            # some versions support set_timeout
            if hasattr(client, 'set_timeout'):
                try:
                    client.set_timeout(3)
                except Exception:
                    pass
            client.connect()
            print('Connected to', ep)
            try:
                root = client.get_root_node()
                print('Root node:', root)
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
