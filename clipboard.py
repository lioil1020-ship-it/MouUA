from PyQt6.QtWidgets import QTreeWidgetItem, QMessageBox
from PyQt6.QtCore import Qt
import traceback


class ClipboardManager:
    """管理剪貼（序列化/反序列化）並處理衝突策略。
    與 UI 無關；序列化/反序列化時會呼叫宿主的 `AppController`（`save_*` / `calculate_next_*`），
    以將業務邏輯從 UI 分離。
    """

    def __init__(self, app):
        self.app = app
        self.clipboard = None

    def copy(self, item):
        if item is None:
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

            self.clipboard = _serialize(item)
        except Exception as e:
            tb = traceback.format_exc()
            QMessageBox.critical(self.app, "Copy Error", f"複製時發生例外：{e}\n\n{tb}")
            self.clipboard = None

    def cut(self, item):
        self.copy(item)
        parent = item.parent() or self.app.tree.invisibleRootItem()
        parent.removeChild(item)
        return parent

    def paste(self, target_item):
        if not self.clipboard or not target_item:
            return None

        clip = self.clipboard
        copy_type = clip.get("type") if isinstance(clip, dict) else None
        target_type = target_item.data(0, Qt.ItemDataRole.UserRole)

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
            self._deserialize_item(parent_node, clip)
        else:
            # 舊版相容
            new_item = QTreeWidgetItem(parent_node)
            new_item.setData(0, Qt.ItemDataRole.UserRole, copy_type)
            new_name = f"{clip.get('name', 'Item')}_Copy"

            if copy_type == "Tag":
                new_item.setHidden(True)
                new_addr = self.app.controller.calculate_next_address(parent_node)
                data = {
                    "general": {
                        "name": new_name,
                        "address": new_addr,
                        "data_type": clip.get("data2"),
                        "description": clip.get("data3"),
                        "scan_rate": clip.get("data4"),
                    },
                    "scaling": clip.get("data5"),
                }
                # Create model via controller
                self.app.controller.save_tag(new_item, data)
            elif copy_type == "Device":
                data = {
                    "name": new_name,
                    "device_id": self.app.controller.calculate_next_id(parent_node),
                    "description": clip.get("data3"),
                    "timing": clip.get("data4"),
                    "data_access": clip.get("data5"),
                    "encoding": clip.get("data6"),
                    "block_sizes": clip.get("data7"),
                    "ethernet": clip.get("data8"),
                }
                self.app.controller.save_device(new_item, data)
            elif copy_type == "Channel":
                data = {
                    "name": new_name,
                    "driver": clip.get("data1"),
                    "params": clip.get("data2"),
                    "description": clip.get("data3"),
                }
                self.app.controller.save_channel(new_item, data)
            elif copy_type == "Group":
                new_item.setText(0, new_name)
                new_item.setData(3, Qt.ItemDataRole.UserRole, clip.get("data3"))

        parent_node.setExpanded(True)
        return parent_node

    def _unique_name(self, parent_node, base_name, node_type):
        used = [
            parent_node.child(i).text(0)
            for i in range(parent_node.childCount())
            if parent_node.child(i).data(0, Qt.ItemDataRole.UserRole) == node_type
        ]
        if base_name not in used:
            return base_name
        idx = 1
        while True:
            cand = f"{base_name}_Copy{idx}"
            if cand not in used:
                return cand
            idx += 1

    # NOTE: next-id/next-address logic delegated to AppController

    def _deserialize_item(self, parent_node, node_dict):
        node_type = node_dict.get("type")
        base_name = node_dict.get("name") or f"{node_type}"

        new_item = QTreeWidgetItem(parent_node)
        new_item.setData(0, Qt.ItemDataRole.UserRole, node_type)

        data = node_dict.get("data", {})

        if node_type == "Tag":
            name = self._unique_name(parent_node, base_name, "Tag")
            addr = data.get("d1")
            used_addrs = [
                parent_node.child(i).data(1, Qt.ItemDataRole.UserRole)
                for i in range(parent_node.childCount())
                if parent_node.child(i).data(0, Qt.ItemDataRole.UserRole) == "Tag"
            ]
            if addr in used_addrs or addr is None:
                addr = self.app.controller.calculate_next_address(parent_node)

            tag_data = {
                "general": {
                    "name": name,
                    "address": addr,
                    "data_type": data.get("d2"),
                    "description": data.get("d3"),
                    "scan_rate": data.get("d4"),
                },
                "scaling": data.get("d5") or {"type": "None"},
            }
            new_item.setHidden(True)
            self.app.controller.save_tag(new_item, tag_data)

        elif node_type == "Device":
            name = self._unique_name(parent_node, base_name, "Device")
            dev_id = self.app.controller.calculate_next_id(parent_node)
            dev_data = {
                "name": name,
                "device_id": dev_id,
                "description": data.get("d3"),
                "timing": data.get("d4"),
                "data_access": data.get("d5"),
                "encoding": data.get("d6"),
                "block_sizes": data.get("d7"),
                "ethernet": data.get("d8"),
            }
            self.app.controller.save_device(new_item, dev_data)

        elif node_type == "Channel":
            name = self._unique_name(parent_node, base_name, "Channel")
            ch_data = {
                "name": name,
                "driver": data.get("d1"),
                "params": data.get("d2"),
                "description": data.get("d3"),
            }
            self.app.controller.save_channel(new_item, ch_data)

        elif node_type == "Group":
            name = self._unique_name(parent_node, base_name, "Group")
            new_item.setText(0, name)
            new_item.setData(3, Qt.ItemDataRole.UserRole, data.get("d3"))

        else:
            new_item.setText(0, base_name)
            for i in range(1, 9):
                v = data.get(f"d{i}")
                if v is not None:
                    new_item.setData(i, Qt.ItemDataRole.UserRole, v)

        for child in node_dict.get("children", []):
            self._deserialize_item(new_item, child)
