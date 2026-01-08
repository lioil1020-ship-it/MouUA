#!/usr/bin/env python3
"""簡單 OPC UA client，用於連接伺服器並列出 Objects 下的變數節點數量。

用法:
  python tests/opc_verify_client.py opc.tcp://127.0.0.1:48480
若未提供參數，預設連到 opc.tcp://127.0.0.1:48480
"""
import sys
import argparse
try:
    from opcua import Client, ua
except Exception as e:
    print('需要安裝 python-opcua: pip install opcua')
    raise


def walk(node, depth=0):
    try:
        children = node.get_children()
    except Exception:
        return []
    out = []
    for c in children:
        try:
            ntype = c.get_node_class()
        except Exception:
            ntype = None
        out.append((c, ntype))
        out.extend(walk(c, depth + 1))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('endpoint', nargs='?', default='opc.tcp://127.0.0.1:48480')
    args = p.parse_args()
    ep = args.endpoint
    print('Connecting to', ep)
    client = Client(ep)
    try:
        client.connect()
    except Exception as e:
        print('連線失敗:', e)
        sys.exit(2)
    try:
        root = client.get_objects_node()
        items = walk(root)
        var_count = 0
        names = []
        for node, ntype in items:
            try:
                if ntype == ua.NodeClass.Variable:
                    var_count += 1
                    try:
                        names.append(node.get_browse_name().Name)
                    except Exception:
                        names.append(str(node))
            except Exception:
                pass

        print('Variable nodes count under Objects:', var_count)
        if var_count <= 200:
            print('Sample variable names:')
            for n in names[:50]:
                print(' -', n)
        else:
            print('Too many to list; first 50:')
            for n in names[:50]:
                print(' -', n)
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


if __name__ == '__main__':
    main()
