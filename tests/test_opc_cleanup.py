#!/usr/bin/env python3
"""小測試：模擬 tree 並呼叫 setup_tags_from_tree 兩次，檢查 _nodes 是否重複疊加。"""
import sys
import os
import importlib.util

# 藉由檔案路徑載入 OPC_UA 模組，避免 Python path 問題
this_dir = os.path.dirname(os.path.dirname(__file__))
mod_path = os.path.join(this_dir, "OPC_UA.py")
spec = importlib.util.spec_from_file_location("OPC_UA", mod_path)
opc_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(opc_mod)
OPCServer = opc_mod.OPCServer


class MockItem:
    def __init__(self, text):
        self._text = text
        self._children = []
        self._parent = None

    def add_child(self, item):
        self._children.append(item)
        item._parent = self

    def childCount(self):
        return len(self._children)

    def child(self, i):
        return self._children[i]

    def text(self, col=0):
        return self._text

    def data(self, col, role):
        # minimal: return None for any data requests
        return None


class FakeVariable:
    def __init__(self, name):
        self.name = name

    def set_writable(self, v):
        pass

    def set_data_type(self, t):
        pass

    def set_value(self, v):
        pass


class FakeNode:
    def __init__(self, name):
        self.name = name
        self._children = []

    def add_object(self, nsidx, name):
        n = FakeNode(name)
        self._children.append(n)
        return n

    def add_variable(self, nsidx, name, val):
        v = FakeVariable(name)
        self._children.append(v)
        return v

    def get_children(self):
        return list(self._children)

    def delete(self):
        # simulate deletion
        self._children = []


class FakeObjects(FakeNode):
    def __init__(self):
        super().__init__("ObjectsRoot")


def build_tree():
    root = MockItem('Connectivity')
    ch = MockItem('Channel1')
    dev = MockItem('Device1')
    tag = MockItem('Tag1')
    dev.add_child(tag)
    ch.add_child(dev)
    root.add_child(ch)
    return root


def main():
    s = OPCServer({})
    # simulate started server and objects node
    s._server = True
    s._objects = FakeObjects()
    s._nsidx = 1

    tree = build_tree()

    print('Calling setup_tags_from_tree first time...')
    s.setup_tags_from_tree(tree)
    keys1 = sorted(list(s._nodes.keys()))
    print('nodes count after first call:', len(keys1))
    print(keys1)

    print('\nCalling setup_tags_from_tree second time...')
    s.setup_tags_from_tree(tree)
    keys2 = sorted(list(s._nodes.keys()))
    print('nodes count after second call:', len(keys2))
    print(keys2)

    if keys1 == keys2:
        print('\nRESULT: OK - no duplicate nodes')
        sys.exit(0)
    else:
        print('\nRESULT: FAIL - nodes changed between calls')
        sys.exit(2)


if __name__ == '__main__':
    main()
