from PyQt6.QtWidgets import QTreeWidgetItem, QMessageBox
from PyQt6.QtCore import Qt
import re
import traceback


class ClipboardManager:
    # 管理剪貼（序列化/反序列化）並處理衝突策略。
    # 與 UI 無關；序列化/反序列化時會呼叫宿主的 `AppController`（`save_*` / `calculate_next_*`），
    # 以將業務邏輯從 UI 分離。

    def __init__(self, app):
        self.app = app
        self.clipboard = None

    def copy(self, item):
        # Accept either a single QTreeWidgetItem or an iterable of items
        if not item:
            QMessageBox.warning(self.app, "Copy Failed", "沒有選取要複製的項目。")
            return

        try:
            def _serialize(it):
                node = {
                    "type": it.data(0, Qt.ItemDataRole.UserRole),
                    "name": it.text(0),
                    "data": {},
                    "children": [],
                }
                for i in range(1, 9):
                    try:
                        node["data"][f"d{i}"] = it.data(i, Qt.ItemDataRole.UserRole)
                    except Exception:
                        node["data"][f"d{i}"] = None
                for i in range(it.childCount()):
                    child = it.child(i)
                    if isinstance(child, QTreeWidgetItem):
                        node["children"].append(_serialize(child))
                return node

            # If an iterable of items is passed, store as Multi
            if isinstance(item, (list, tuple)):
                children = [_serialize(it) for it in item]
                self.clipboard = {"type": "Multi", "name": "Multiple", "children": children}
            else:
                self.clipboard = _serialize(item)
        except Exception as e:
            tb = traceback.format_exc()
            QMessageBox.critical(self.app, "Copy Error", f"複製時發生例外：{e}\n\n{tb}")
            self.clipboard = None

    def cut(self, item):
        # Accept single item or iterable
        if not item:
            return None
        self.copy(item)
        parent = None
        try:
            if isinstance(item, (list, tuple)):
                for it in item:
                    try:
                        parent = it.parent() or self.app.tree.invisibleRootItem()
                        parent.removeChild(it)
                    except Exception:
                        pass
            else:
                parent = item.parent() or self.app.tree.invisibleRootItem()
                parent.removeChild(item)
        except Exception:
            pass
        return parent

    def paste(self, target_item):
        if not self.clipboard or not target_item:
            return None

        clip = self.clipboard
        # Support Multi payloads: if clipboard type is Multi, infer the inner item type(s)
        copy_type = None
        if isinstance(clip, dict):
            if clip.get("type") == "Multi":
                children = clip.get("children", []) or []
                if not children:
                    QMessageBox.warning(self.app, "貼上失敗", f"剪貼簿為空的 Multi。")
                    return None
                child_types = set()
                for c in children:
                    try:
                        if isinstance(c, dict) and c.get("type"):
                            child_types.add(c.get("type"))
                    except Exception:
                        pass
                # If all children share the same type, treat copy_type as that type
                if len(child_types) == 1:
                    copy_type = next(iter(child_types))
                else:
                    # Mixed types - fall back to first child's type for target resolution
                    copy_type = children[0].get("type") if isinstance(children[0], dict) else None
            else:
                copy_type = clip.get("type")
        else:
            copy_type = None
        target_type = target_item.data(0, Qt.ItemDataRole.UserRole)

        # Special-case: pasted clipboard is a serialized Device but target is a Group.
        # Users often copy a Device (containing Groups/Tags) and want to paste the Tag(s)
        # into an existing Group. Support that by extracting Tag nodes and deserializing
        # them directly under the target Group.
        try:
            if isinstance(clip, dict) and clip.get('type') == 'Device' and target_type == 'Group':
                # recursively find Tag nodes and paste them under target_item
                def _walk_and_paste(node):
                    if not isinstance(node, dict):
                        return
                    ntype = node.get('type')
                    if ntype == 'Tag':
                        try:
                            self._deserialize_item(target_item, node)
                        except Exception:
                            pass
                    for c in node.get('children', []) or []:
                        _walk_and_paste(c)

                for child in clip.get('children', []) or []:
                    _walk_and_paste(child)
                try:
                    target_item.setExpanded(True)
                except Exception:
                    pass
                return target_item
        except Exception:
            pass
        # If the target has no explicit type (e.g. invisibleRootItem),
        # treat it as the top-level Connectivity container so Channels can be pasted.
        try:
            root_item = getattr(self.app, 'tree', None)
            if root_item is not None and hasattr(root_item, 'invisibleRootItem'):
                root_item = root_item.invisibleRootItem()
            else:
                root_item = None
        except Exception:
            root_item = None
        # If target has no explicit type but is a top-level node (no parent),
        # treat it as Connectivity so Channels can be pasted into it.
        try:
            if target_type is None and target_item is not None and getattr(target_item, 'parent', None)() is None:
                target_type = 'Connectivity'
        except Exception:
            pass

        parent_node = None
        if copy_type in ["Group", "Tag"] and target_type in ["Device", "Group"]:
            parent_node = target_item
        elif copy_type == "Device" and target_type == "Channel":
            parent_node = target_item
        elif copy_type == "Channel" and target_type == "Connectivity":
            parent_node = target_item
        elif copy_type == target_type:
            parent_node = target_item.parent()

        if not parent_node:
            QMessageBox.warning(
                self.app, "貼上失敗", f"無法將 {copy_type} 貼至 {target_type}。"
            )
            return None

        if isinstance(clip, dict) and "children" in clip:
            # If this is a Multi clipboard payload, paste each child individually
            if clip.get("type") == "Multi":
                for child in clip.get("children", []):
                    self._deserialize_item(parent_node, child)
            else:
                self._deserialize_item(parent_node, clip)
        else:
            # 舊版相容
            new_item = QTreeWidgetItem(parent_node)
            new_item.setData(0, Qt.ItemDataRole.UserRole, copy_type)
            new_name = f"{clip.get('name', 'Item')}_Copy"

            if copy_type == "Tag":
                new_item.setHidden(True)
                # Determine prefix based on access
                # Access is in data1 (d1), but we need to be careful with old clipboard format
                access_val = clip.get("data3")  # Old format had access in wrong position
                if not access_val or not isinstance(access_val, str):
                    access_val = "Read/Write"
                
                data_type = clip.get("data2")
                
                # Determine prefix
                if data_type and isinstance(data_type, str) and "Boolean" in data_type:
                    prefix = "0" if access_val == "Read/Write" else "1"
                else:
                    prefix = "4" if access_val == "Read/Write" else "3"
                
                new_addr = self.app.controller.calculate_next_address(parent_node, prefix=prefix, new_type=data_type)
                data = {
                    "general": {
                        "name": new_name,
                        "address": new_addr,
                        "data_type": data_type,  # d2
                        "description": clip.get("data1"),  # d1
                        "scan_rate": clip.get("data5"),  # d5
                        "access": access_val,  # d3
                    },
                    "scaling": clip.get("data6") or {"type": "None"},  # d6
                }
                # Create model via controller
                self.app.controller.save_tag(new_item, data)
            elif copy_type == "Device":
                flat = {
                    "name": new_name,
                    "device_id": self.app.controller.calculate_next_id(parent_node),
                    # new mapping: description -> d1, device_id -> d2, timing -> d3, data_access -> d4, encoding -> d5, block_sizes -> d6
                    "description": clip.get("data1"),
                    "timing": clip.get("data3"),
                    "data_access": clip.get("data4"),
                    "encoding": clip.get("data5"),
                    "block_sizes": clip.get("data6"),
                }
                nested = {"general": {"name": flat["name"], "description": flat.get("description"), "device_id": flat.get("device_id")},
                          "timing": flat.get("timing"), "data_access": flat.get("data_access"), "encoding": flat.get("encoding"), "block_sizes": flat.get("block_sizes")}
                out = {**flat, **nested}
                self.app.controller.save_device(new_item, out)
            elif copy_type == "Channel":
                flat = {
                    "name": new_name,
                    "driver": clip.get("data1"),
                    "params": clip.get("data2"),
                    "description": clip.get("data3"),
                }
                # normalize driver type if clipboard stored a dict/OrderedDict
                drv_raw = flat.get("driver")
                try:
                    if isinstance(drv_raw, dict):
                        drv_type = drv_raw.get("type") or str(drv_raw)
                    else:
                        drv_type = drv_raw
                except Exception:
                    drv_type = drv_raw
                from collections import OrderedDict as _OD
                nested = _OD([
                    ("general", {"channel_name": flat.get("name"), "description": flat.get("description")}),
                    ("driver", _OD([("type", drv_type), ("params", flat.get("params"))])),
                    ("communication", flat.get("params")),
                ])
                out = {**flat, **nested}
                self.app.controller.save_channel(new_item, out)
            elif copy_type == "Group":
                new_item.setText(0, new_name)
                # Group description now stored in d1 / role1
                new_item.setData(1, Qt.ItemDataRole.UserRole, clip.get("data1"))

        parent_node.setExpanded(True)
        return parent_node

    def _unique_name(self, parent_node, base_name, node_type):
        used = [
            parent_node.child(i).text(0)
            for i in range(parent_node.childCount())
            if parent_node.child(i).data(0, Qt.ItemDataRole.UserRole) == node_type
        ]

        # Normalize base_name by stripping repeated _Copy fragments
        # e.g., Tag_Copy1_Copy2 -> Tag
        clean = base_name
        # remove trailing _Copy or _Copy<number> fragments repeatedly
        while True:
            m = re.search(r"(_Copy\d+|_Copy)$", clean)
            if not m:
                break
            clean = clean[: m.start()]

        # If cleaned name ends with digits (e.g., Tag12), treat trailing digits as counter
        m_num = re.match(r"^(.*?)(\d+)$", clean)
        if m_num:
            root = m_num.group(1)
            base_idx = int(m_num.group(2))
            # find existing numeric suffixes for this root
            nums = []
            for u in used:
                mu = re.match(rf"^{re.escape(root)}(\d+)$", u)
                if mu:
                    nums.append(int(mu.group(1)))
            if nums:
                nxt = max(nums) + 1
            else:
                # if the exact cleaned base (with its number) is used, increment
                nxt = base_idx + 1 if clean in used else base_idx
            cand = f"{root}{nxt}"
            if cand not in used:
                return cand
            # fallback to incremental search
            i = nxt + 1
            while True:
                cand = f"{root}{i}"
                if cand not in used:
                    return cand
                i += 1

        # Otherwise, use cleaned base and append numeric suffix if needed
        if clean not in used:
            return clean

        # find numeric suffices for clean
        nums = []
        for u in used:
            mu = re.match(rf"^{re.escape(clean)}(\d+)$", u)
            if mu:
                nums.append(int(mu.group(1)))
        if nums:
            nxt = max(nums) + 1
        else:
            nxt = 1
        cand = f"{clean}{nxt}"
        while cand in used:
            nxt += 1
            cand = f"{clean}{nxt}"
        return cand

    # NOTE: next-id/next-address logic delegated to AppController

    def _deserialize_item(self, parent_node, node_dict):
        node_type = node_dict.get("type")
        base_name = node_dict.get("name") or f"{node_type}"

        new_item = QTreeWidgetItem(parent_node)
        new_item.setData(0, Qt.ItemDataRole.UserRole, node_type)

        data = node_dict.get("data", {})

        if node_type == "Tag":
            name = self._unique_name(parent_node, base_name, "Tag")
            addr = data.get("d4")
            used_addrs = [
                parent_node.child(i).data(4, Qt.ItemDataRole.UserRole)
                for i in range(parent_node.childCount())
                if parent_node.child(i).data(0, Qt.ItemDataRole.UserRole) == "Tag"
            ]

            if addr in used_addrs or addr is None:
                # determine prefix from data_type and client access
                new_type = data.get("d2")
                
                # 檢查是否為 Array 型別
                is_array_type = new_type and isinstance(new_type, str) and "Array" in new_type
                array_size = 10  # 預設陣列大小
                
                # 如果原始地址有 [n] 格式，提取陣列大小
                if is_array_type and addr and isinstance(addr, str):
                    import re
                    match = re.search(r'\[(\d+)\]', addr)
                    if match:
                        array_size = int(match.group(1))
                
                # scaling info may affect step size when scaling is Linear
                # Scaling is stored in role 6 (d6), NOT d5
                scaling = data.get("d6") if isinstance(data.get("d6"), dict) else None

                # Client Access is stored in role 3 (d3). Prefer that.
                access = data.get("d3")
                if not access or not isinstance(access, str):
                    access = "Read/Write"

                # Determine prefix based on data_type and access
                prefix = None
                try:
                    if new_type and isinstance(new_type, str) and "Boolean" in new_type:
                        prefix = "0" if access == "Read/Write" else "1"
                    else:
                        prefix = "4" if access == "Read/Write" else "3"
                except Exception as e:
                    # If anything goes wrong, default to "4"
                    prefix = "4"

                # If scaling is Linear and scaled_type consumes multiple registers,
                # pass scaled_type as the `new_type` so calculate_next_address uses that step.
                new_type_for_step = new_type
                try:
                    if isinstance(scaling, dict) and scaling.get('type') == 'Linear':
                        scaled_t = scaling.get('scaled_type')
                        # map of sizes is handled by controller.calculate_next_address
                        if scaled_t:
                            new_type_for_step = scaled_t
                except Exception:
                    pass

                # Calculate next address with prefix
                addr = self.app.controller.calculate_next_address(parent_node, prefix=prefix, new_type=new_type_for_step)
                
                # 如果是 Array 型別，添加 [size] 到地址後面
                if is_array_type:
                    addr = f"{addr} [{array_size}]"
                
                addr_used = addr
            else:
                # Use provided address
                addr_used = addr

            # Build tag_data regardless of whether address was auto-assigned
            tag_data = {
                "general": {
                    "name": name,
                    "address": addr_used,
                    "data_type": data.get("d2"),
                    "description": data.get("d1"),
                    "scan_rate": data.get("d5"),
                    # include client access if present (d3)
                    "access": data.get("d3") if data.get("d3") is not None else None,
                },
                "scaling": data.get("d6") or {"type": "None"},
            }
            new_item.setHidden(True)
            self.app.controller.save_tag(new_item, tag_data)

        elif node_type == "Device":
            name = self._unique_name(parent_node, base_name, "Device")
            dev_id = self.app.controller.calculate_next_id(parent_node)
            flat = {
                "name": name,
                "device_id": dev_id,
                # new mapping: description -> d1, device_id -> d2, timing -> d3, data_access -> d4, encoding -> d5, block_sizes -> d6
                "description": data.get("d1"),
                "driver": None,
                "timing": data.get("d3"),
                "data_access": data.get("d4"),
                "encoding": data.get("d5"),
                "block_sizes": data.get("d6"),
            }
            # try to inherit driver type from parent channel if present
            try:
                parent_drv = None
                if parent_node is not None:
                    parent_drv = parent_node.data(1, Qt.ItemDataRole.UserRole)
                # normalize parent driver to a string type if it's an OrderedDict or dict
                if isinstance(parent_drv, dict):
                    try:
                        ptype = parent_drv.get('type') or str(parent_drv)
                    except Exception:
                        ptype = str(parent_drv)
                else:
                    ptype = parent_drv
                flat["driver"] = ptype
            except Exception:
                flat["driver"] = None

            from collections import OrderedDict as _OD
            nested = _OD([
                ("general", {"name": flat.get("name"), "description": flat.get("description"), "device_id": flat.get("device_id")}),
                ("driver", _OD([("type", flat.get("driver")), ("params", {})])),
                ("timing", flat.get("timing")),
                ("data_access", flat.get("data_access")),
                ("encoding", flat.get("encoding")),
                ("block_sizes", flat.get("block_sizes")),
                ("ethernet", flat.get("ethernet")),
            ])
            dev_data = {**flat, **nested}
            self.app.controller.save_device(new_item, dev_data)

        elif node_type == "Channel":
            name = self._unique_name(parent_node, base_name, "Channel")
            # new mapping: d1=description, d2=driver, d3=communication
            flat = {
                "name": name,
                "driver": data.get("d2"),
                "params": data.get("d3"),
                "description": data.get("d1"),
            }
            # normalize raw driver into a string type if necessary
            drv_raw = flat.get("driver")
            try:
                if isinstance(drv_raw, dict):
                    drv_type = drv_raw.get("type") or str(drv_raw)
                else:
                    drv_type = drv_raw
            except Exception:
                drv_type = drv_raw
            from collections import OrderedDict as _OD
            nested = _OD([
                ("general", {"channel_name": flat.get("name"), "description": flat.get("description")}),
                ("driver", _OD([("type", drv_type), ("params", flat.get("params"))])),
                ("communication", flat.get("params")),
            ])
            ch_data = {**flat, **nested}
            self.app.controller.save_channel(new_item, ch_data)

        elif node_type == "Group":
            name = self._unique_name(parent_node, base_name, "Group")
            new_item.setText(0, name)
            # Group description now stored in d1
            new_item.setData(1, Qt.ItemDataRole.UserRole, data.get("d1"))

        else:
            new_item.setText(0, base_name)
            for i in range(1, 9):
                v = data.get(f"d{i}")
                if v is not None:
                    new_item.setData(i, Qt.ItemDataRole.UserRole, v)

        for child in node_dict.get("children", []):
            self._deserialize_item(new_item, child)
