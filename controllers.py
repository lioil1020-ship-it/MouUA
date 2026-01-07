from PyQt6.QtCore import Qt
import csv
import os
import json

# QTreeWidgetItem is not required in this module; UI modules create items.


class AppController:
    """Controller 封裝與資料樹有關的商業邏輯：ID/位址分配、簡單建立輔助等。
    UI（app）仍保留顯示/事件，但會呼叫此 controller 以取得下一個可用的 ID/位址。
    """

    def __init__(self, app):
        self.app = app
        # transaction id counter for building Modbus TCP MBAP headers in diagnostics
        self._txid = 0

    def calculate_next_id(self, channel_item):
        used_ids = []
        for i in range(channel_item.childCount()):
            val = channel_item.child(i).data(2, Qt.ItemDataRole.UserRole)
            if val is not None:
                try:
                    used_ids.append(int(val))
                except Exception:
                    pass
        new_id = 1
        while new_id in used_ids:
            new_id += 1
        return new_id

    def calculate_next_address(self, parent_item):
        def _normalize_to_six(s):
            if s is None:
                return None
            s = str(s).strip()
            if not s:
                return None
            if not s.lstrip("+").lstrip("-").isdigit():
                return None
            # remove sign
            s_num = s.lstrip("+").lstrip("-")
            # rule: if starts with '4' and length==5 -> insert '0' after leading '4' (40001->400001)
            if len(s_num) == 5 and s_num.startswith("4"):
                return int(s_num[0] + "0" + s_num[1:])
            # otherwise pad to 6 digits on the left
            if len(s_num) < 6:
                return int(s_num.zfill(6))
            return int(s_num)

        used_addrs = []
        for i in range(parent_item.childCount()):
            child = parent_item.child(i)
            if child.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                addr_str = child.data(1, Qt.ItemDataRole.UserRole)
                n = _normalize_to_six(addr_str)
                if n is not None:
                    used_addrs.append(n)
        if not used_addrs:
            return "400001"
        return str(max(used_addrs) + 1)

    # --- Save/Load helpers so UI can delegate model creation/storage ---
    def save_channel(self, item, data):
        from models.channel import ChannelModel
        # update visible text and data roles (keep existing storage for compatibility)
        item.setText(0, data["name"])
        item.setData(1, Qt.ItemDataRole.UserRole, data.get("driver"))
        item.setData(2, Qt.ItemDataRole.UserRole, data.get("params"))
        item.setData(3, Qt.ItemDataRole.UserRole, data.get("description"))
        # attach model
        model = ChannelModel.from_dict(data)
        item.setData(0, Qt.ItemDataRole.UserRole + 1, model)
        return model

    def save_device(self, item, data):
        from models.device import DeviceModel
        # emit debug diagnostic about incoming device data structure
        try:
            self.app.append_diagnostic(
                f"SAVE_DEVICE_DATA_KEYS: {list(data.keys()) if isinstance(data, dict) else type(data)}"
            )
            self.app.append_diagnostic(
                f"SAVE_DEVICE_block_sizes_RAW: {data.get('block_sizes') if isinstance(data, dict) else None}"
            )
        except Exception:
            pass
        item.setText(0, data["name"])
        item.setData(1, Qt.ItemDataRole.UserRole, "Modbus")
        item.setData(2, Qt.ItemDataRole.UserRole, data.get("device_id"))
        item.setData(3, Qt.ItemDataRole.UserRole, data.get("description"))
        item.setData(4, Qt.ItemDataRole.UserRole, data.get("timing"))
        item.setData(5, Qt.ItemDataRole.UserRole, data.get("data_access"))
        item.setData(6, Qt.ItemDataRole.UserRole, data.get("encoding"))
        # Normalize block_sizes from various possible keys/locations the dialog might use
        def _extract_block_sizes(d):
            if d is None:
                return None
            # direct keys
            for k in ("block_sizes", "blockSizes", "blocks", "block_sizes_map", "blockSizesMap"):
                if k in d and d.get(k) is not None:
                    return d.get(k)
            # nested under 'params' or similar
            if isinstance(d, dict):
                for cand in ("params", "settings", "extra", "data"):
                    sub = d.get(cand)
                    if isinstance(sub, dict):
                        for k in ("block_sizes", "blockSizes", "blocks"):
                            if k in sub and sub.get(k) is not None:
                                return sub.get(k)
            # if nothing found, return None
            return None

        bs = _extract_block_sizes(data)
        # if not found in top-level, try inside provided device params key
        if bs is None:
            try:
                # some callers may pass block_sizes inside data.get('ethernet') or other keys
                bs = _extract_block_sizes(data.get('ethernet') if isinstance(data, dict) else None)
            except Exception:
                bs = None
        item.setData(7, Qt.ItemDataRole.UserRole, bs)
        try:
            # emit diagnostic showing which tree item was written and its id
            self.app.append_diagnostic(f"SAVE_DEVICE_ITEM: {item.text(0)!r} id={id(item)}")
            self.app.append_diagnostic(f"SAVE_DEVICE_block_sizes_SET: {bs!r}")
        except Exception:
            pass
        if "ethernet" in data:
            item.setData(8, Qt.ItemDataRole.UserRole, data.get("ethernet"))
        model = DeviceModel.from_dict(data)
        item.setData(0, Qt.ItemDataRole.UserRole + 1, model)
        return model

    def save_tag(self, item, data):
        from models.tag import TagModel
        gen = data.get("general", {})
        sc = data.get("scaling", {})
        item.setText(0, gen.get("name"))
        item.setData(1, Qt.ItemDataRole.UserRole, gen.get("address"))
        item.setData(2, Qt.ItemDataRole.UserRole, gen.get("data_type"))
        item.setData(3, Qt.ItemDataRole.UserRole, gen.get("description"))
        item.setData(4, Qt.ItemDataRole.UserRole, gen.get("scan_rate"))
        # store Client Access (e.g. 'Read/Write' or 'Read Only') in a spare slot so
        # other UI logic can inspect it. Use slot 9 which is unused elsewhere.
        try:
            access_val = gen.get("access") if isinstance(gen, dict) else None
            if access_val is None:
                access_val = data.get("general", {}).get("access") if isinstance(data, dict) else None
            # normalize shorthand to canonical full text
            if access_val is not None:
                try:
                    s = str(access_val).strip()
                    sl = s.lower()
                    if sl in ("ro", "r/o") or ("read only" in sl and "write" not in sl):
                        store_access = "Read Only"
                    elif sl in ("r/w", "rw") or ("write" in sl and "read" in sl) or ("read/write" in sl):
                        store_access = "Read/Write"
                    else:
                        store_access = s
                except Exception:
                    store_access = access_val
                item.setData(9, Qt.ItemDataRole.UserRole, store_access)
        except Exception:
            pass
        item.setData(5, Qt.ItemDataRole.UserRole, sc)
        # ensure tags are hidden in the tree (tags displayed only in right-top table)
        try:
            item.setHidden(True)
        except Exception:
            pass
        model = TagModel.from_dict(data)
        item.setData(0, Qt.ItemDataRole.UserRole + 1, model)
        return model

    def load_model(self, item):
        # retrieve attached model if exists
        model = item.data(0, Qt.ItemDataRole.UserRole + 1)
        return model

    def export_device_to_csv(self, device_item, filepath, encoding="utf-8"):
        """Export all tags under a device (including groups) to CSV at filepath."""
        # header as in example
        # Standard export header
        header = [
            "Tag Name",
            "Address",
            "Data Type",
            "Respect Data Type",
            "Client Access",
            "Scan Rate",
            "Scaling",
            "Raw Low",
            "Raw High",
            "Scaled Low",
            "Scaled High",
            "Scaled Data Type",
            "Clamp Low",
            "Clamp High",
            "Eng Units",
            "Description",
            "Negate Value",
        ]

        def collect_tags(parent, prefix=None):
            """Collect tag entries in traversal order and return list of dicts.

            We do not compute raw_index here — that is computed after we know all array group sizes.
            """
            tags = []
            import re

            def _addr_to_six(s):
                if s is None:
                    return ""
                ss = str(s).strip()
                if not ss:
                    return ""
                if not ss.lstrip("+").lstrip("-").isdigit():
                    return ss
                sn = ss.lstrip("+").lstrip("-")
                if len(sn) == 5 and sn.startswith("4"):
                    return sn[0] + "0" + sn[1:]
                if len(sn) < 6:
                    return sn.zfill(6)
                return sn

            for i in range(parent.childCount()):
                child = parent.child(i)
                ttype = child.data(0, Qt.ItemDataRole.UserRole)
                if ttype == "Tag":
                    name = child.text(0)
                    full_name = f"{prefix}.{name}" if prefix else name
                    addr = child.data(1, Qt.ItemDataRole.UserRole) or ""
                    addr_out = _addr_to_six(addr)
                    dtype = child.data(2, Qt.ItemDataRole.UserRole) or "Word"
                    scan = child.data(4, Qt.ItemDataRole.UserRole) or "10"
                    desc = child.data(3, Qt.ItemDataRole.UserRole) or ""
                    sc = child.data(5, Qt.ItemDataRole.UserRole) or {}
                    sc_type = sc.get("type", "")

                    def fmt_float_six(v):
                        try:
                            fv = float(v)
                        except Exception:
                            return ""
                        # If the float has no fractional part, export as integer
                        try:
                            if float(fv).is_integer():
                                return str(int(fv))
                        except Exception:
                            pass
                        return f"{fv:.6f}"

                    def fmt_int(v):
                        try:
                            return str(int(float(v)))
                        except Exception:
                            return ""

                    if (
                        sc_type is None
                        or str(sc_type).strip().lower() == "none"
                        or str(sc_type).strip() == ""
                    ):
                        scaling = ""
                        raw_low = ""
                        raw_high = ""
                        scaled_low = ""
                        scaled_high = ""
                        scaled_type = ""
                        clamp_low = ""
                        clamp_high = ""
                        eng_units = ""
                        negate = ""
                    else:
                        scaling = sc.get("type", "")
                        raw_low = fmt_float_six(sc.get("raw_low", ""))
                        raw_high = fmt_float_six(sc.get("raw_high", ""))
                        scaled_low = fmt_float_six(sc.get("scaled_low", ""))
                        scaled_high = fmt_float_six(sc.get("scaled_high", ""))
                        scaled_type = sc.get("scaled_type", "") or ""
                        clamp_low = fmt_int(sc.get("clamp_low", ""))
                        clamp_high = fmt_int(sc.get("clamp_high", ""))
                        if clamp_low == "":
                            clamp_low = "0"
                        if clamp_high == "":
                            clamp_high = "0"
                        eng_units = sc.get("eng_units", "") or ""
                        negate = fmt_int(sc.get("negate", ""))
                    if negate == "":
                        negate = "0"

                    # map client access to export short form
                    def _map_access_for_export(ch):
                        try:
                            ca = ch.data(9, Qt.ItemDataRole.UserRole)
                            if isinstance(ca, str) and ca.strip():
                                s = ca.strip().lower()
                                if "write" in s or s in ("r/w", "rw"):
                                    return "R/W"
                                return "RO"
                            for slot in range(10):
                                ca = ch.data(slot, Qt.ItemDataRole.UserRole)
                                if not ca or not isinstance(ca, str):
                                    continue
                                s = ca.strip().lower()
                                if "write" in s or s in ("r/w", "rw"):
                                    return "R/W"
                                if s in ("ro", "r/o") or ("read" in s and "write" not in s):
                                    return "RO"
                        except Exception:
                            pass
                        return "R/W"

                    client_access_export = _map_access_for_export(child)

                    # detect array group by name pattern 'BaseName [index]'
                    mname = re.match(r"^(.*)\s*\[\s*(\d+)\s*\]\s*$", name)
                    base_name = mname.group(1).strip() if mname else None
                    index_in_group = int(mname.group(2)) if mname else None

                    tags.append({
                        "name": name,
                        "full_name": full_name,
                        "base_name": base_name,
                        "index_in_group": index_in_group,
                        "addr": addr_out,
                        "dtype": dtype,
                        "scan": scan,
                        "desc": desc,
                        "scaling": scaling,
                        "raw_low": raw_low,
                        "raw_high": raw_high,
                        "scaled_low": scaled_low,
                        "scaled_high": scaled_high,
                        "scaled_type": scaled_type,
                        "clamp_low": clamp_low,
                        "clamp_high": clamp_high,
                        "eng_units": eng_units,
                        "negate": negate,
                        "client": client_access_export,
                    })
                elif ttype == "Group":
                    grp = child.text(0)
                    new_prefix = f"{prefix}.{grp}" if prefix else grp
                    tags.extend(collect_tags(child, new_prefix))
                else:
                    pass
            return tags

        # collect rows in original simple format
        rows = []
        for t in collect_tags(device_item, None):
            # t is a dict-like entry from collect_tags
            row = [
                t.get("full_name"),
                t.get("addr"),
                t.get("dtype"),
                "1",
                t.get("client"),
                t.get("scan"),
                t.get("scaling"),
                t.get("raw_low"),
                t.get("raw_high"),
                t.get("scaled_low"),
                t.get("scaled_high"),
                t.get("scaled_type"),
                t.get("clamp_low"),
                t.get("clamp_high"),
                t.get("eng_units"),
                t.get("desc"),
                t.get("negate"),
            ]
            rows.append(row)
        # ensure directory exists
        d = os.path.dirname(filepath)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

        # Write CSV lines manually without extra quoting (respect UI casing)
        # map encoding name to Python encoding; use 'mbcs' for ANSI on Windows
        # ensure UTF-8 exports include a BOM by using 'utf-8-sig'
        write_encoding = encoding
        try:
            if isinstance(write_encoding, str) and write_encoding.lower().startswith("utf-8"):
                write_encoding = "utf-8-sig"
        except Exception:
            pass
        with open(filepath, "w", newline="", encoding=write_encoding) as f:
            # write header
            f.write(",".join(header) + "\n")
            for r in rows:
                # r indices: 0:Tag Name,1:Address,2:Data Type,3:Respect,4:Client Access,5:Scan,
                # 6:scaling,7:raw_low,8:raw_high,9:scaled_low,10:scaled_high,11:scaled_type,
                # 12:clamp_low,13:clamp_high,14:eng_units,15:description,16:negate
                tag = r[0]
                addr = r[1]
                dtype = r[2]
                respect = r[3]
                client = r[4]
                scan = r[5]
                scaling = r[6]
                raw_low = r[7]
                raw_high = r[8]
                scaled_low = r[9]
                scaled_high = r[10]
                scaled_type = r[11]
                clamp_low = r[12]
                clamp_high = r[13]
                eng_units = r[14]
                description = r[15]
                negate = r[16]

                # helper to quote only when necessary
                def q(s):
                    return f'"{str(s).replace("\"", "\"\"")}"'

                def fmt_out(s):
                    if s is None:
                        return ""
                    ss = str(s)
                    if ss == "":
                        return ""
                    if "," in ss or '"' in ss:
                        return q(ss)
                    return ss

                # Ensure numeric clamp fields are strings
                clamp_low_s = str(clamp_low) if clamp_low is not None else ""
                clamp_high_s = str(clamp_high) if clamp_high is not None else ""

                if not scaling:
                    # if Eng Units is empty but Description looks like a unit (short, no spaces),
                    # treat Description as Eng Units (fixes imported files that put units in Description)
                    try:
                        if (not eng_units) and description:
                            ds = str(description).strip()
                            if ds and " " not in ds and len(ds) <= 6:
                                eng_units = ds
                                description = ""
                    except Exception:
                        pass
                    parts = [
                        str(tag),
                        str(addr),
                        dtype,
                        respect,
                        client,
                        scan,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        fmt_out(eng_units),
                        fmt_out(description),
                        "",
                    ]
                else:
                    # if Eng Units is empty but Description looks like a unit (short, no spaces),
                    # treat Description as Eng Units
                    try:
                        if (not eng_units) and description:
                            ds = str(description).strip()
                            if ds and " " not in ds and len(ds) <= 6:
                                eng_units = ds
                                description = ""
                    except Exception:
                        pass
                    parts = [
                        str(tag),
                        str(addr),
                        dtype,
                        respect,
                        client,
                        scan,
                        scaling,
                        raw_low or "",
                        raw_high or "",
                        scaled_low or "",
                        scaled_high or "",
                        scaled_type or "",
                        clamp_low_s,
                        clamp_high_s,
                        fmt_out(eng_units),
                        fmt_out(description),
                        negate or "0",
                    ]

                line = ",".join(parts) + "\n"
                f.write(line)

    def import_device_from_csv(self, device_item, filepath):
        """Import tags from CSV into the given device_item. Create groups as needed."""
        if not os.path.exists(filepath):
            return
        from PyQt6.QtWidgets import QTreeWidgetItem

        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # build header mapping to handle BOMs or slight header name variations
            fieldnames = reader.fieldnames or []

            def find_field(key_parts):
                for fn in fieldnames:
                    if not fn:
                        continue
                    low = fn.lower()
                    if all(p in low for p in key_parts):
                        return fn
                return None

            tag_field = find_field(["tag", "name"]) or find_field(["tag"])
            addr_field = find_field(["address"]) or find_field(["addr"])
            dtype_field = find_field(["data", "type"]) or find_field(["datatype"])
            scan_field = find_field(["scan"]) or find_field(["rate"])
            scaling_field = find_field(["scaling"])
            desc_field = find_field(["description"]) or find_field(["desc"])
            client_field = find_field(["client", "access"]) or find_field(["access"])

            for row in reader:
                tag_name = (row.get(tag_field) if tag_field else None) or ""
                # defensive cleanup: strip whitespace and surrounding quotes
                tag_name = tag_name.strip().strip('"').strip("'")
                # split groups by '.' and clean each part
                parts = [
                    p.strip().strip('"').strip("'") for p in tag_name.split(".") if p.strip()
                ]
                groups = parts[:-1]
                tag_simple = parts[-1] if parts else ""

                parent = device_item
                # ensure groups exist under device
                for g in groups:
                    # find existing group
                    found = None
                    for i in range(parent.childCount()):
                        c = parent.child(i)
                        if c.data(0, Qt.ItemDataRole.UserRole) == "Group" and c.text(0) == g:
                            found = c
                            break
                    if found:
                        parent = found
                    else:
                        new_group = QTreeWidgetItem(parent)
                        new_group.setText(0, g)
                        new_group.setData(0, Qt.ItemDataRole.UserRole, "Group")
                        parent = new_group

                # create tag item and hide in left tree (tags not shown there)
                titem = QTreeWidgetItem(parent)
                titem.setData(0, Qt.ItemDataRole.UserRole, "Tag")
                titem.setHidden(True)
                # build data dict for save_tag
                gen = {
                    "name": tag_simple,
                    "address": (row.get(addr_field) if addr_field else None)
                    or self.calculate_next_address(parent),
                    "data_type": (row.get(dtype_field) if dtype_field else None) or "Word",
                    "description": (row.get(desc_field) if desc_field else None) or "",
                    "scan_rate": (row.get(scan_field) if scan_field else None) or "10",
                    # map imported Client Access variants back to dialog-friendly text
                    "access": (lambda v: (
                        "Read Only" if (v is not None and str(v).strip().lower() in ("ro", "r/o", "read only")) else (
                            "Read/Write" if (v is not None and str(v).strip().lower() in ("r/w", "rw", "read/write", "read write")) else (
                                ("Read/Write" if (v is None or str(v).strip() == "") else ("Read Only" if "read only" in str(v).lower() and "write" not in str(v).lower() else "Read/Write"))
                            )
                        )
                    ))(row.get(client_field) if client_field else None),
                }
                sc = {
                    "type": (row.get(scaling_field) if scaling_field else None) or "None",
                    "raw_low": (row.get("Raw Low") or row.get("RawLow") or ""),
                    "raw_high": (row.get("Raw High") or row.get("RawHigh") or ""),
                    "scaled_low": (row.get("Scaled Low") or row.get("ScaledLow") or ""),
                    "scaled_high": (row.get("Scaled High") or row.get("ScaledHigh") or ""),
                    "scaled_type": (row.get("Scaled Data Type") or row.get("ScaledDataType") or ""),
                    "clamp_low": (row.get("Clamp Low") or ""),
                    "clamp_high": (row.get("Clamp High") or ""),
                    "eng_units": (row.get("Eng Units") or row.get("EngUnits") or ""),
                    "negate": (row.get("Negate Value") or ""),
                }
                data = {"general": gen, "scaling": sc}
                # delegate save and ensure visibility
                self.save_tag(titem, data)
                parent.setExpanded(True)
                # ensure device node expanded as well
                device_item.setExpanded(True)

    # export_project_to_csv removed per user request

    def import_project_from_csv(self, filepath, encoding="utf-8"):
        """Import a whole project CSV and create channels/devices/groups/tags accordingly."""
        if not os.path.exists(filepath):
            return
        from PyQt6.QtWidgets import QTreeWidgetItem

        with open(filepath, newline="", encoding=encoding) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []

            def find_field(key_parts):
                for fn in fieldnames:
                    if not fn:
                        continue
                    low = fn.lower()
                    if all(p in low for p in key_parts):
                        return fn
                return None

            ch_field = find_field(["channel"]) or "Channel"
            dev_field = find_field(["device"]) or "Device"
            tag_field = find_field(["tag", "name"]) or find_field(["tag"]) or "Tag Name"
            addr_field = find_field(["address"]) or "Address"
            dtype_field = find_field(["data", "type"]) or "Data Type"
            scan_field = find_field(["scan"]) or find_field(["rate"]) or "Scan Rate"
            scaling_field = find_field(["scaling"]) or "Scaling"
            desc_field = find_field(["description"]) or find_field(["desc"]) or "Description"
            client_field = find_field(["client", "access"]) or find_field(["access"]) or "Client Access"

            conn = self.app.tree.conn_node
            for row in reader:
                ch_name = (row.get(ch_field) or "").strip().strip('"').strip("'")
                dev_name = (row.get(dev_field) or "").strip().strip('"').strip("'")
                tag_name = (row.get(tag_field) or "").strip().strip('"').strip("'")
                addr = (row.get(addr_field) or "").strip().strip('"').strip("'")
                # optional: channel/device params stored as JSON in CSV columns
                ch_params_raw = None
                dev_params_raw = None
                # find potential param columns by common names
                for cand in ("Channel Params", "ChannelParams", "channel_params", "channelparams"):
                    if cand in row:
                        ch_params_raw = row.get(cand)
                        break
                for cand in ("Device Params", "DeviceParams", "device_params", "deviceparams"):
                    if cand in row:
                        dev_params_raw = row.get(cand)
                        break

                # find or create channel
                channel_item = None
                for i in range(conn.childCount()):
                    c = conn.child(i)
                    if c.data(0, Qt.ItemDataRole.UserRole) == "Channel" and c.text(0) == ch_name:
                        channel_item = c
                        break
                if not channel_item:
                    channel_item = QTreeWidgetItem(conn)
                    channel_item.setText(0, ch_name or "Channel")
                    channel_item.setData(0, Qt.ItemDataRole.UserRole, "Channel")
                    # restore channel params if present
                    try:
                        params_dict = {}
                        if ch_params_raw:
                            try:
                                params_dict = json.loads(ch_params_raw)
                            except Exception:
                                params_dict = {}
                        self.save_channel(channel_item, {"name": channel_item.text(0), "params": params_dict})
                    except Exception:
                        self.save_channel(channel_item, {"name": channel_item.text(0)})

                # find or create device under channel
                device_item = None
                for i in range(channel_item.childCount()):
                    d = channel_item.child(i)
                    if d.data(0, Qt.ItemDataRole.UserRole) == "Device" and d.text(0) == dev_name:
                        device_item = d
                        break
                if not device_item:
                    device_item = QTreeWidgetItem(channel_item)
                    device_item.setText(0, dev_name or "Device")
                    device_item.setData(0, Qt.ItemDataRole.UserRole, "Device")
                    # restore device params if present
                    try:
                        dev_params_dict = {}
                        if dev_params_raw:
                            try:
                                dev_params_dict = json.loads(dev_params_raw)
                            except Exception:
                                dev_params_dict = {}
                        # merge with required keys
                        dev_data = {
                            "name": device_item.text(0),
                            "device_id": self.calculate_next_id(channel_item),
                            "description": dev_params_dict.get("description", ""),
                            "timing": dev_params_dict.get("timing", {}),
                            "data_access": dev_params_dict.get("data_access", {}),
                            "encoding": dev_params_dict.get("encoding", {}),
                            "block_sizes": dev_params_dict.get("block_sizes", {}),
                            "ethernet": dev_params_dict.get("ethernet", {}),
                            "params": dev_params_dict.get("params", {}),
                        }
                        self.save_device(device_item, dev_data)
                    except Exception:
                        self.save_device(
                            device_item,
                            {
                                "name": device_item.text(0),
                                "device_id": self.calculate_next_id(channel_item),
                            },
                        )

                # handle group path inside tag name if present (dot separated)
                parts = [p.strip() for p in tag_name.split(".") if p.strip()]
                groups = parts[:-1]
                tag_simple = parts[-1] if parts else ""

                parent = device_item
                for g in groups:
                    found = None
                    for i in range(parent.childCount()):
                        c = parent.child(i)
                        if c.data(0, Qt.ItemDataRole.UserRole) == "Group" and c.text(0) == g:
                            found = c
                            break
                    if found:
                        parent = found
                    else:
                        new_group = QTreeWidgetItem(parent)
                        new_group.setText(0, g)
                        new_group.setData(0, Qt.ItemDataRole.UserRole, "Group")
                        parent = new_group

                # create tag
                titem = QTreeWidgetItem(parent)
                titem.setData(0, Qt.ItemDataRole.UserRole, "Tag")
                titem.setHidden(True)
                gen = {
                    "name": tag_simple,
                    "address": addr or self.calculate_next_address(parent),
                    "data_type": (row.get(dtype_field) if dtype_field else None) or "Word",
                    "description": (row.get(desc_field) if desc_field else None) or "",
                    "scan_rate": (row.get(scan_field) if scan_field else None) or "10",
                    "access": (lambda v: (
                        "Read Only" if (v is not None and str(v).strip().lower() in ("ro", "r/o", "read only")) else (
                            "Read/Write" if (v is not None and str(v).strip().lower() in ("r/w", "rw", "read/write", "read write")) else (
                                ("Read/Write" if (v is None or str(v).strip() == "") else ("Read Only" if "read only" in str(v).lower() and "write" not in str(v).lower() else "Read/Write"))
                            )
                        )
                    ))(row.get(client_field) if client_field else None),
                }
                sc = {
                    "type": (row.get(scaling_field) if scaling_field else None) or "None",
                    "raw_low": (row.get("Raw Low") or row.get("RawLow") or ""),
                    "raw_high": (row.get("Raw High") or row.get("RawHigh") or ""),
                    "scaled_low": (row.get("Scaled Low") or row.get("ScaledLow") or ""),
                    "scaled_high": (row.get("Scaled High") or row.get("ScaledHigh") or ""),
                    "scaled_type": (row.get("Scaled Data Type") or row.get("ScaledDataType") or ""),
                    "clamp_low": (row.get("Clamp Low") or ""),
                    "clamp_high": (row.get("Clamp High") or ""),
                    "eng_units": (row.get("Eng Units") or row.get("EngUnits") or ""),
                    "negate": (row.get("Negate Value") or ""),
                }
                self.save_tag(titem, {"general": gen, "scaling": sc})
                parent.setExpanded(True)
                conn.setExpanded(True)

    def export_project_to_json(self, filepath):
        """Export the entire project (channels/devices/groups/tags) to a JSON file."""
        import json

        def serialize_item(item):
            t = item.data(0, Qt.ItemDataRole.UserRole)
            node = {"type": t, "text": item.text(0)}
            if t == "Channel":
                node.update({
                    "driver": item.data(1, Qt.ItemDataRole.UserRole),
                    "params": item.data(2, Qt.ItemDataRole.UserRole) or {},
                    "description": item.data(3, Qt.ItemDataRole.UserRole) or "",
                })
            elif t == "Device":
                node.update({
                    "device_id": item.data(2, Qt.ItemDataRole.UserRole),
                    "description": item.data(3, Qt.ItemDataRole.UserRole) or "",
                    "timing": item.data(4, Qt.ItemDataRole.UserRole) or {},
                    "data_access": item.data(5, Qt.ItemDataRole.UserRole) or {},
                    "encoding": item.data(6, Qt.ItemDataRole.UserRole) or {},
                    "block_sizes": item.data(7, Qt.ItemDataRole.UserRole) or {},
                    "ethernet": item.data(8, Qt.ItemDataRole.UserRole) or {},
                })
            elif t == "Tag":
                # preserve stored roles for tag
                # Prefer any attached model's access value; fall back to spare column slot 9
                access_val = None
                try:
                    mdl = item.data(0, Qt.ItemDataRole.UserRole + 1)
                    if mdl is not None and hasattr(mdl, "access"):
                        access_val = getattr(mdl, "access")
                except Exception:
                    access_val = None
                if not access_val:
                    try:
                        access_val = item.data(9, Qt.ItemDataRole.UserRole)
                    except Exception:
                        access_val = None
                if not access_val:
                    access_val = "Read/Write"
                node.update({
                    "address": item.data(1, Qt.ItemDataRole.UserRole),
                    "data_type": item.data(2, Qt.ItemDataRole.UserRole),
                    "description": item.data(3, Qt.ItemDataRole.UserRole),
                    "scan_rate": item.data(4, Qt.ItemDataRole.UserRole),
                    # store client access explicitly so JSON round-trips preserve it
                    "access": access_val,
                    "scaling": item.data(5, Qt.ItemDataRole.UserRole) or {},
                })
            # recursively serialize children
            children = []
            for i in range(item.childCount()):
                children.append(serialize_item(item.child(i)))
            if children:
                node["children"] = children
            return node

        root = self.app.tree.conn_node
        doc = {"type": "Project", "channels": []}
        for i in range(root.childCount()):
            ch = root.child(i)
            if ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                doc["channels"].append(serialize_item(ch))

        d = os.path.dirname(filepath)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

    def import_project_from_json(self, filepath):
        """Import project JSON previously saved with `export_project_to_json`.

        This replaces the current Connectivity tree content with the loaded project.
        """
        import json
        from PyQt6.QtWidgets import QTreeWidgetItem

        if not os.path.exists(filepath):
            return
        with open(filepath, "r", encoding="utf-8") as f:
            doc = json.load(f)

        # clear existing connectivity children
        root = self.app.tree.conn_node
        while root.childCount() > 0:
            root.removeChild(root.child(0))

        def build_node(parent, node):
            t = node.get("type")
            txt = node.get("text") or ""
            item = QTreeWidgetItem(parent)
            item.setText(0, txt)
            item.setData(0, Qt.ItemDataRole.UserRole, t)
            if t == "Channel":
                item.setData(1, Qt.ItemDataRole.UserRole, node.get("driver"))
                item.setData(2, Qt.ItemDataRole.UserRole, node.get("params"))
                item.setData(3, Qt.ItemDataRole.UserRole, node.get("description"))
            elif t == "Device":
                item.setData(2, Qt.ItemDataRole.UserRole, node.get("device_id"))
                item.setData(3, Qt.ItemDataRole.UserRole, node.get("description"))
                item.setData(4, Qt.ItemDataRole.UserRole, node.get("timing"))
                item.setData(5, Qt.ItemDataRole.UserRole, node.get("data_access"))
                item.setData(6, Qt.ItemDataRole.UserRole, node.get("encoding"))
                item.setData(7, Qt.ItemDataRole.UserRole, node.get("block_sizes"))
                item.setData(8, Qt.ItemDataRole.UserRole, node.get("ethernet"))
            elif t == "Tag":
                gen = {
                    "name": txt,
                    "address": node.get("address"),
                    "data_type": node.get("data_type"),
                    "description": node.get("description"),
                    "scan_rate": node.get("scan_rate"),
                    "access": node.get("access", "Read/Write"),
                }
                sc = node.get("scaling") or {"type": "None"}
                self.save_tag(item, {"general": gen, "scaling": sc})

            # recurse children
            for c in node.get("children", []):
                build_node(item, c)

        for ch in doc.get("channels", []):
            build_node(root, ch)
        root.setExpanded(True)

    def read_tag_value(self, tag_item, host: str = None, port: int = 502, unit: int = 1, timeout: float = 3.0, connect_timeout: float | None = None, client_mode: str = "tcp", client_params: dict | None = None, diag_callback=None):
        """Read a single tag value via Modbus (synchronous wrapper).

        This method wraps the async implementation `read_tag_value_async` via
        `asyncio.run` for backward compatibility with synchronous callers.
        """
        import asyncio

        return asyncio.run(self.read_tag_value_async(tag_item, host=host, port=port, unit=unit, timeout=timeout, connect_timeout=connect_timeout, client_mode=client_mode, client_params=client_params, diag_callback=diag_callback))

    async def read_tag_value_async(self, tag_item, host: str = None, port: int = 502, unit: int = 1, timeout: float = 3.0, connect_timeout: float | None = None, client_mode: str = "tcp", client_params: dict | None = None, diag_callback=None):
        """Async read for a single tag. Returns the raw pymodbus result.

        Emits TX/RX diagnostics via `diag_callback` if provided.
        """
        from modbus_client import ModbusClient
        import asyncio

        # (suppressed BEGIN READ diagnostic to keep UI compact)

        # gather info from tag item
        addr_raw = tag_item.data(1, Qt.ItemDataRole.UserRole)
        dtype = (tag_item.data(2, Qt.ItemDataRole.UserRole) or "Word")

        def _digits(s):
            if s is None:
                return ""
            return "".join(ch for ch in str(s) if ch.isdigit())

        # Extract array length from address if present (e.g. `40095[3]`)
        import re
        array_len_from_addr = None
        try:
            m_addr = re.search(r"\[\s*(\d+)\s*\]", str(addr_raw))
            if m_addr:
                array_len_from_addr = int(m_addr.group(1))
                # remove bracketed part before extracting digits
                addr_no_brackets = re.sub(r"\[\s*\d+\s*\]", "", str(addr_raw))
            else:
                addr_no_brackets = str(addr_raw)
        except Exception:
            addr_no_brackets = str(addr_raw)

        nums = _digits(addr_no_brackets)
        if len(nums) == 5 and nums.startswith("4"):
            nums = nums[0] + "0" + nums[1:]
        nums = nums.zfill(6)
        lead = nums[0]

        # Determine zero-based addressing flags from the Device (if present).
        # Use explicit string comparison to match Kepware naming exactly.
        zero_based = False
        zero_based_bit = True
        try:
            dev = tag_item.parent()
            while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                dev = dev.parent()
            if dev is not None:
                da = dev.data(5, Qt.ItemDataRole.UserRole) or {}
                try:
                    zb_raw = da.get("zero_based", "Disable")
                    zb_s = str(zb_raw).strip().lower()
                    # Kepware: when UI shows 'Disable' -> send raw_index (0x5F for 40095)
                    # when UI shows 'Enable'  -> send raw_index + 1 (0x60 for 40095)
                    if zb_s == "enable":
                        zero_based = True
                    else:
                        zero_based = False
                except Exception:
                    zero_based = False
                try:
                    zb_bit_raw = da.get("zero_based_bit", "Enable")
                    zero_based_bit = str(zb_bit_raw).strip().lower() == "enable"
                except Exception:
                    zero_based_bit = True
        except Exception:
            pass

        # Compute offset from the 5-digit index portion of the address string.
        try:
            raw_index = int(nums[1:])
        except Exception:
            raw_index = 0
        # Interpret the UI 'Zero-Based Addressing' per requested rule:
        # - UI 'Enable' -> send raw_index - 1 (apply -1)
        # - UI 'Disable' -> send raw_index unchanged
        try:
            if zero_based:
                offset = max(0, raw_index - 1)
            else:
                offset = max(0, raw_index)
        except Exception:
            offset = max(0, raw_index)

        # Support array syntax both in data type and address. Examples:
        #   data_type = "Float[3]"  or address = "40095 [3]"
        import re

        # parse array length from dtype like Float[3] or Float(Array)
        array_len = 1
        base_dtype = dtype
        try:
            m = re.match(r"^\s*([A-Za-z0-9_]+)\s*\[\s*(\d+)\s*\]\s*$", str(dtype))
            if m:
                base_dtype = m.group(1)
                array_len = max(1, int(m.group(2)))
            else:
                # support forms like: Float(Array) or Float (Array) or FLOAT(array)
                m2 = re.match(r"^\s*([A-Za-z0-9_]+)\s*\(\s*array\s*\)\s*$", str(dtype), flags=re.IGNORECASE)
                if m2:
                    base_dtype = m2.group(1)
                    # length may come from address bracket; keep array_len=1 until address parsing
        except Exception:
            base_dtype = dtype

        # prefer array length from address bracket if present
        try:
            if array_len_from_addr is not None:
                array_len = max(1, int(array_len_from_addr))
        except Exception:
            pass

        # determine function code and per-element register count
        if "Boolean" in base_dtype or base_dtype.lower().startswith("bool"):
            per_elem_regs = 1
            fc = 1 if lead == "0" else 2
        else:
            dt = base_dtype.lower()
            if any(k in dt for k in ("double", "qword", "llong")):
                per_elem_regs = 4
            elif any(k in dt for k in ("long", "dword", "float")):
                per_elem_regs = 2
            else:
                per_elem_regs = 1
            # If the original address text begins with '3', prefer FC=4 (Input Registers).
            try:
                if str(addr_no_brackets).strip().startswith("3") or lead == "3":
                    fc = 4
                elif lead == "4":
                    fc = 3
                else:
                    fc = 3
            except Exception:
                if lead == "3":
                    fc = 4
                else:
                    fc = 3

        # total registers to read
        count = per_elem_regs * max(1, int(array_len))

        # Adjust offset semantics for bit vs register addressing
        try:
            if "Boolean" in dtype or dtype.lower().startswith("bool"):
                # for bit (coil/discrete) addresses, preserve existing bit flag semantics
                if zero_based_bit:
                    offset = max(0, raw_index)
                else:
                    offset = max(0, raw_index - 1)
            else:
                # for registers use requested zero-based semantics:
                # UI 'Enable' -> send raw_index - 1; UI 'Disable' -> send raw_index unchanged
                if zero_based:
                    offset = max(0, raw_index - 1)
                else:
                    offset = max(0, raw_index)
        except Exception:
            pass

        # TX diagnostics are emitted by the ModbusClient framer (compact TX/RX lines)

        client_params = client_params or {}
        # read encoding and timing settings from Device if present so we can pass to client
        encoding = {}
        device_timing = {}
        try:
            dev = tag_item.parent()
            while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                dev = dev.parent()
            if dev is not None:
                encoding = dev.data(6, Qt.ItemDataRole.UserRole) or {}
                device_timing = dev.data(4, Qt.ItemDataRole.UserRole) or {}
        except Exception:
            pass

        # Map device timing fields to pymodbus parameters.
        # Device: 'connect_timeout' is seconds; 'req_timeout' is milliseconds.
        try:
            # prefer explicit connect_timeout param, then device timing, then function timeout
            if connect_timeout is not None:
                mapped_connect_timeout = float(connect_timeout)
            else:
                mapped_connect_timeout = float(device_timing.get("connect_timeout", timeout))
        except Exception:
            mapped_connect_timeout = float(connect_timeout if connect_timeout is not None else timeout)

        try:
            # device 'req_timeout' provided in milliseconds -> convert to seconds
            if "req_timeout" in device_timing and device_timing.get("req_timeout") is not None:
                mapped_request_timeout = max(0.0, float(device_timing.get("req_timeout")) / 1000.0)
            else:
                # fallback: caller `timeout` treated as seconds
                mapped_request_timeout = float(timeout)
        except Exception:
            mapped_request_timeout = float(timeout)

        # mapped timing exists but keep UI minimal; avoid verbose timing diag

        # mapped timing exists but keep UI minimal; avoid verbose timing diag

        # Emit compact TX diagnostic before sending. Historically the
        # controller built a synthetic TX line; however when a diag_callback
        # is provided the `ModbusClient` will register pymodbus' trace_packet
        # and emit the actual wire TX bytes. To avoid duplicate TX lines we
        # only emit this synthetic TX when the caller explicitly requests it
        # via client_params['emit_synthetic_tx']=True. Default is False.
        try:
            emit_synthetic = False
            try:
                if client_params and isinstance(client_params, dict):
                    emit_synthetic = bool(client_params.get('emit_synthetic_tx', False))
            except Exception:
                emit_synthetic = False

            if diag_callback and emit_synthetic:
                try:
                    self._txid = (self._txid + 1) & 0xFFFF
                except Exception:
                    self._txid = 0
                txid = self._txid
                proto = 0
                pdu_tx = bytes([fc]) + offset.to_bytes(2, "big") + int(count).to_bytes(2, "big")

                try:
                    mode_s = (client_mode or "tcp").strip().lower()
                except Exception:
                    mode_s = "tcp"

                if mode_s == "rtu":
                    # Build RTU ADU: unit + PDU + CRC16 (little-endian)
                    try:
                        adu = bytes([int(unit)]) + pdu_tx

                        def _crc16(data: bytes) -> int:
                            crc = 0xFFFF
                            for b in data:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            return crc & 0xFFFF

                        crc = _crc16(adu)
                        crc_bytes = crc.to_bytes(2, "little")
                        adu_rt = adu + crc_bytes
                        hex_tx = " ".join(f"{b:02X}" for b in adu_rt)
                    except Exception:
                        # fallback to MBAP-like display if RTU formatting fails
                        mbap_len_tx = len(pdu_tx) + 1
                        mbap_tx = txid.to_bytes(2, "big") + proto.to_bytes(2, "big") + mbap_len_tx.to_bytes(2, "big") + int(unit).to_bytes(1, "big")
                        adu_tx = mbap_tx + pdu_tx
                        hex_tx = " ".join(f"{b:02X}" for b in adu_tx)
                else:
                    mbap_len_tx = len(pdu_tx) + 1
                    mbap_tx = txid.to_bytes(2, "big") + proto.to_bytes(2, "big") + mbap_len_tx.to_bytes(2, "big") + int(unit).to_bytes(1, "big")
                    adu_tx = mbap_tx + pdu_tx
                    hex_tx = " ".join(f"{b:02X}" for b in adu_tx)

                try:
                    diag_callback(f"TX: | {hex_tx} |")
                except Exception:
                    pass
        except Exception:
            pass

        # honor attempts/inter-request delay when provided in client_params or Device timing
        attempts = 1
        inter_delay_sec = 0.0
        try:
            if client_params:
                attempts = int(client_params.get('attempts', client_params.get('retries', int(device_timing.get('attempts', 1)))))
                inter_ms = int(client_params.get('inter_req_delay', int(device_timing.get('inter_req_delay', 0))))
            else:
                attempts = int(device_timing.get('attempts', 1))
                inter_ms = int(device_timing.get('inter_req_delay', 0))
            inter_delay_sec = max(0.0, float(inter_ms) / 1000.0)
        except Exception:
            attempts = 1
            inter_delay_sec = 0.0

        client = ModbusClient(
            mode=client_mode or "tcp",
            host=host,
            port=port,
            unit=unit,
            connect_timeout=mapped_connect_timeout,
            request_timeout=mapped_request_timeout,
            diag_callback=diag_callback,
            **(client_params or {}),
        )
        try:
            try:
                await client.connect_async()
                last_exc = None
                result = None
                for attempt_no in range(1, max(1, attempts) + 1):
                    try:
                        if diag_callback:
                            diag_callback(f"READ ATTEMPT: {attempt_no}/{attempts} unit={unit} offset={offset} count={count} fc={fc}")
                        result = await client.read_async(offset, count, fc, encoding=encoding)
                        # treat explicit error responses as failures and potentially retry
                        is_error = False
                        try:
                            is_error = bool(result and getattr(result, 'isError', lambda: False)())
                        except Exception:
                            is_error = False
                        if result is not None and not is_error:
                            break
                        else:
                            last_exc = Exception("Modbus read returned error or no response")
                    except Exception as e:
                        last_exc = e
                    # if more attempts remain, wait inter-request delay
                    if attempt_no < attempts:
                        try:
                            if diag_callback:
                                diag_callback(f"RETRY WAIT: {inter_delay_sec}s before next attempt")
                        except Exception:
                            pass
                        try:
                            await asyncio.sleep(inter_delay_sec)
                        except Exception:
                            pass
                # after attempts loop, if result still None or error -> raise
                if result is None or (hasattr(result, 'isError') and result.isError()):
                    if last_exc is not None:
                        raise last_exc
            except Exception as e:
                # emit traceback to diagnostics so user can see why read failed
                if diag_callback:
                    try:
                        import traceback

                        tb = traceback.format_exc()
                        diag_callback(f"ERR READ EXCEPTION: {e}\n{tb}")
                    except Exception:
                        pass
                raise
        finally:
            try:
                await client.close_async()
            except Exception:
                pass

        # diagnostics RX (build MBAP + PDU)
        if diag_callback:
            try:
                proto = 0
                txid = self._txid
                # (verbose diagnostics suppressed to keep UI compact)
                # Diagnostics are forwarded to UI via diag_callback; no file logging.

                if hasattr(result, "bits") and result.bits is not None:
                    bits = result.bits
                    bdata = bytearray()
                    byte_val = 0
                    for i, bit in enumerate(bits):
                        if bit:
                            byte_val |= (1 << (i % 8))
                        if (i % 8) == 7:
                            bdata.append(byte_val)
                            byte_val = 0
                    if len(bits) % 8:
                        bdata.append(byte_val)
                    pdu = bytes([fc, len(bdata)]) + bytes(bdata)
                elif hasattr(result, "registers") and result.registers is not None:
                    regs = result.registers
                    # prefer data_bytes attached to result (client normalized using encoding)
                    data_bytes = getattr(result, "data_bytes", None)
                    # attach array metadata so poller can decode multiple elements
                    try:
                        setattr(result, 'array_len', int(array_len))
                        setattr(result, 'array_base', str(base_dtype))
                    except Exception:
                        pass
                    # debug: show identity and content of data_bytes to troubleshoot empty payload
                    # (suppressed DBG output)
                    if data_bytes is None:
                        # defensive: ensure regs is a sequence of ints
                        try:
                            data_bytes = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs)
                        except Exception:
                            try:
                                regs_list = list(regs)
                                data_bytes = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs_list)
                            except Exception:
                                data_bytes = b""
                    # (suppressed register/data bytes diagnostics)

                    # Always prefer normalized data_bytes when present and non-empty
                    if data_bytes:
                        pdu = bytes([fc, len(data_bytes)]) + data_bytes
                        # (suppressed FORCED PDU diagnostic)
                    else:
                        # fallback to pymodbus encode() if available and seems valid
                        enc = None
                        try:
                            if hasattr(result, 'encode'):
                                try:
                                    enc = result.encode()
                                except Exception:
                                    enc = None
                        except Exception:
                            enc = None

                        used_enc = False
                        if enc and isinstance(enc, (bytes, bytearray)):
                            try:
                                if len(enc) >= 2 and enc[0] == fc and enc[1] > 0:
                                    pdu = bytes(enc)
                                    used_enc = True
                                    # (suppressed USING ENCODE diagnostic)
                            except Exception:
                                pass

                        if not used_enc:
                            # final fallback: empty data
                            pdu = bytes([fc, 0])
                            # (suppressed FALLBACK PDU diagnostic)
                else:
                    pdu = str(result).encode("utf-8")
                # RX diagnostics are produced by the client's diag_callback
                # and the canonical read payload is attached as `result.data_bytes`.
            except Exception:
                # emit only a concise error for UI
                try:
                    if diag_callback:
                        diag_callback("ERR RX")
                except Exception:
                    pass

        return result

    def write_tag_value(self, tag_item, value, host: str = None, port: int = 502, unit: int = 1, timeout: float = 3.0, connect_timeout: float | None = None, client_mode: str = "tcp", client_params: dict | None = None, diag_callback=None):
        """Synchronous wrapper for writing a tag value."""
        import asyncio

        return asyncio.run(self.write_tag_value_async(tag_item, value, host=host, port=port, unit=unit, timeout=timeout, connect_timeout=connect_timeout, client_mode=client_mode, client_params=client_params, diag_callback=diag_callback))

    async def write_tag_value_async(self, tag_item, value, host: str = None, port: int = 502, unit: int = 1, timeout: float = 3.0, connect_timeout: float | None = None, client_mode: str = "tcp", client_params: dict | None = None, diag_callback=None):
        """Async write for a single tag value.

        Supports:
        - Coil writes (FC5/FC15)
        - Single-register writes (FC6) and multiple-register writes (FC16)
        - Bit-in-register writes via Mask Write Register (FC22) or read-modify-write fallback
        Respects device `encoding` flags (`modicon_bit_order`, `treat_longs_as_decimals`, word/dword ordering).
        """
        from modbus_client import ModbusClient
        import struct
        import re
        import asyncio

        # reuse parsing logic from read_tag_value_async for address and dtype
        addr_raw = tag_item.data(1, Qt.ItemDataRole.UserRole)
        dtype = (tag_item.data(2, Qt.ItemDataRole.UserRole) or "Word")

        def _digits(s):
            if s is None:
                return ""
            return "".join(ch for ch in str(s) if ch.isdigit())

        # detect bracketed array length and strip
        array_len_from_addr = None
        try:
            m_addr = re.search(r"\[\s*(\d+)\s*\]", str(addr_raw))
            if m_addr:
                array_len_from_addr = int(m_addr.group(1))
                addr_no_brackets = re.sub(r"\[\s*\d+\s*\]", "", str(addr_raw))
            else:
                addr_no_brackets = str(addr_raw)
        except Exception:
            addr_no_brackets = str(addr_raw)

        # detect bit suffix like 40001.3 -> base addr and bit index
        bit_index = None
        try:
            if "." in addr_no_brackets:
                parts = addr_no_brackets.split(".")
                if len(parts) >= 2 and parts[-1].strip().isdigit():
                    bit_index = int(parts[-1].strip())
                    addr_no_brackets = ".".join(parts[:-1])
        except Exception:
            bit_index = None

        nums = _digits(addr_no_brackets)
        if len(nums) == 5 and nums.startswith("4"):
            nums = nums[0] + "0" + nums[1:]
        nums = nums.zfill(6)
        lead = nums[0]

        # Determine zero-based addressing flags from the Device (if present).
        zero_based = False
        zero_based_bit = True
        encoding = {}
        device_timing = {}
        try:
            dev = tag_item.parent()
            while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                dev = dev.parent()
            if dev is not None:
                da = dev.data(5, Qt.ItemDataRole.UserRole) or {}
                try:
                    zb_raw = da.get("zero_based", "Disable")
                    zb_s = str(zb_raw).strip().lower()
                    if zb_s == "enable":
                        zero_based = True
                    else:
                        zero_based = False
                except Exception:
                    zero_based = False
                try:
                    zb_bit_raw = da.get("zero_based_bit", "Enable")
                    zero_based_bit = str(zb_bit_raw).strip().lower() == "enable"
                except Exception:
                    zero_based_bit = True
                encoding = dev.data(6, Qt.ItemDataRole.UserRole) or {}
                device_timing = dev.data(4, Qt.ItemDataRole.UserRole) or {}
        except Exception:
            pass

        try:
            raw_index = int(nums[1:])
        except Exception:
            raw_index = 0

        # compute offset based on bit vs register
        if "Boolean" in dtype or dtype.lower().startswith("bool"):
            if zero_based_bit:
                offset = max(0, raw_index)
            else:
                offset = max(0, raw_index - 1)
        else:
            # Apply zero-based mapping per requested rule:
            # zero_based Enable -> send raw_index - 1
            # zero_based Disable -> send raw_index unchanged
            if zero_based:
                offset = max(0, raw_index - 1)
            else:
                offset = max(0, raw_index)

        # compute function codes and per-element regs (mirror read logic)
        if "Boolean" in dtype or dtype.lower().startswith("bool"):
            per_elem_regs = 1
            fc_read = 1 if lead == "0" else 2
            is_bit_coil = True
        else:
            dt = dtype.lower()
            if any(k in dt for k in ("double", "qword", "llong")):
                per_elem_regs = 4
            elif any(k in dt for k in ("long", "dword", "float")):
                per_elem_regs = 2
            else:
                per_elem_regs = 1
            fc_read = 3 if lead == "4" else (4 if lead == "3" else 3)
            is_bit_coil = False

        # device encoding flags helper
        def _enc_flag(enc, *keys, default="Enable"):
            try:
                for k in keys:
                    if k in enc and enc.get(k) is not None:
                        return str(enc.get(k)).strip().lower() in ("enable", "true", "1", "yes")
            except Exception:
                pass
            return str(default).strip().lower() in ("enable", "true", "1", "yes")

        modicon_bit_order = _enc_flag(encoding, "bit_order", "modicon_bit_order", "modiconBitOrder", default="Disable")
        treat_longs = _enc_flag(encoding, "treat_longs_as_decimals", "treat_longs", "treatLongs", default="Disable")
        byte_order_enable = _enc_flag(encoding, "byte_order", "modbus_byte_order", "byteOrder", default="Enable")
        first_word_low = _enc_flag(encoding, "word_low", "first_word_low", "firstWordLow", default="Enable")
        first_dword_low = _enc_flag(encoding, "dword_low", "first_dword_low", "firstDwordLow", default="Enable")

        if diag_callback:
            try:
                diag_callback(f"WRITE: target={addr_raw} offset={offset} dtype={dtype} bit_index={bit_index} modicon_bit_order={modicon_bit_order} treat_longs={treat_longs}")
            except Exception:
                pass

        # helper: build registers list from raw big-endian bytes according to encoding
        def _regs_from_bytes(b_all):
            chunks = [b_all[i : i + 2] for i in range(0, len(b_all), 2)]
            # reverse the reordering applied by _normalize_register_bytes
            try:
                if not first_dword_low:
                    for i in range(0, len(chunks) - 3, 4):
                        chunks[i : i + 4] = chunks[i + 2 : i + 4] + chunks[i : i + 2]
            except Exception:
                pass
            try:
                if not first_word_low:
                    for i in range(0, len(chunks) - 1, 2):
                        chunks[i], chunks[i + 1] = chunks[i + 1], chunks[i]
            except Exception:
                pass
            try:
                if not byte_order_enable:
                    chunks = [c[::-1] for c in chunks]
            except Exception:
                pass
            regs_out = []
            for c in chunks:
                try:
                    regs_out.append(int.from_bytes(c, "big"))
                except Exception:
                    regs_out.append(0)
            return regs_out

        # helper: encode a numeric value into register list
        def _encode_value_to_regs(val, dtype_str):
            dt = dtype_str.lower()
            try:
                if any(k in dt for k in ("double", "qword", "llong", "int64")):
                    b_all = struct.pack(">d", float(val))
                elif "float" in dt or dt == "f":
                    b_all = struct.pack(">f", float(val))
                elif any(k in dt for k in ("long", "dword", "int32")):
                    if treat_longs:
                        # encode as High*10000 + Low
                        v = int(float(val))
                        v = max(0, min(99999999, v))
                        high = v // 10000
                        low = v % 10000
                        b_all = high.to_bytes(2, "big") + low.to_bytes(2, "big")
                    else:
                        v = int(float(val)) & 0xFFFFFFFF
                        b_all = v.to_bytes(4, "big")
                else:
                    # default: single register
                    v = int(float(val)) & 0xFFFF
                    b_all = v.to_bytes(2, "big")
                # expand to full 2-byte-aligned length
                # ensure even number of bytes
                if len(b_all) % 2 != 0:
                    b_all = b"\x00" + b_all
            except Exception:
                # fallback: zero
                b_all = b"\x00\x00"
            return _regs_from_bytes(b_all)

        client_params = client_params or {}
        # map timing similar to read
        try:
            if connect_timeout is not None:
                mapped_connect_timeout = float(connect_timeout)
            else:
                mapped_connect_timeout = float(device_timing.get("connect_timeout", timeout))
        except Exception:
            mapped_connect_timeout = float(connect_timeout if connect_timeout is not None else timeout)

        try:
            if "req_timeout" in device_timing and device_timing.get("req_timeout") is not None:
                mapped_request_timeout = max(0.0, float(device_timing.get("req_timeout")) / 1000.0)
            else:
                mapped_request_timeout = float(timeout)
        except Exception:
            mapped_request_timeout = float(timeout)

        client = ModbusClient(
            mode=client_mode or "tcp",
            host=host,
            port=port,
            unit=unit,
            connect_timeout=mapped_connect_timeout,
            request_timeout=mapped_request_timeout,
            diag_callback=diag_callback,
            **client_params,
        )

        try:
            await client.connect_async()

            # helpers for synthetic TX/RX emission
            def _crc16(data: bytes) -> int:
                crc = 0xFFFF
                for b in data:
                    crc ^= b
                    for _ in range(8):
                        if crc & 1:
                            crc = (crc >> 1) ^ 0xA001
                        else:
                            crc >>= 1
                return crc & 0xFFFF

            def _crc_bytes(data: bytes) -> bytes:
                try:
                    return _crc16(data).to_bytes(2, "little")
                except Exception:
                    return b"\x00\x00"

            def _hex(b: bytes) -> str:
                try:
                    return " ".join(f"{x:02X}" for x in b)
                except Exception:
                    try:
                        return str(b)
                    except Exception:
                        return ""

            def _format_adu(pdu: bytes, use_mode: str):
                try:
                    m = (use_mode or "tcp").strip().lower()
                except Exception:
                    m = "tcp"
                if m in ("rtu", "overtcp"):
                    # RTU ADU: unit + pdu + crc16(unit+pdu)
                    try:
                        adu = bytes([int(unit)]) + pdu
                        crc = _crc_bytes(bytes([int(unit)]) + pdu)
                        return adu + crc
                    except Exception:
                        return bytes([int(unit)]) + pdu
                else:
                    # MBAP (TCP): txid(2) proto(2)=0 len(2) unit(1) + pdu
                    try:
                        self._txid = (self._txid + 1) & 0xFFFF
                    except Exception:
                        self._txid = 0
                    txid = int(getattr(self, '_txid', 0))
                    proto = 0
                    mbap_len = len(pdu) + 1
                    try:
                        mbap = txid.to_bytes(2, "big") + proto.to_bytes(2, "big") + mbap_len.to_bytes(2, "big") + int(unit).to_bytes(1, "big")
                        return mbap + pdu
                    except Exception:
                        return pdu

            def _emit_tx(pdu: bytes):
                if not diag_callback:
                    return
                try:
                    adu = _format_adu(pdu, client_mode)
                    diag_callback(f"TX: | {_hex(adu)} |")
                    return adu
                except Exception:
                    return None

            def _emit_rx(pdu: bytes):
                # synthetic RX emission disabled (rely on pymodbus logging for real RX)
                return

            # Bit/coil writes
            if is_bit_coil:
                # for coils use write_coil (single) or write_coils (multiple)
                try:
                    # build FC5 pdu for single coil
                    try:
                        fc = 5
                        val_word = 0xFF00 if bool(value) else 0x0000
                        pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(val_word).to_bytes(2, "big")
                        _emit_tx(pdu_tx)
                    except Exception:
                        pdu_tx = None

                    if hasattr(client, "write_coil_async"):
                        res = await client.write_coil_async(offset, bool(value))
                    else:
                        # try underlying sync client in thread
                        underlying = getattr(client, "_client", None)
                        if underlying is not None and hasattr(underlying, "write_coil"):
                            res = await asyncio.to_thread(getattr(underlying, "write_coil"), offset, bool(value))
                        elif hasattr(client, "write_coil"):
                            # last resort: call ModbusClient.sync wrapper in a thread
                            res = await asyncio.to_thread(getattr(client, "write_coil"), offset, bool(value))
                        else:
                            raise NotImplementedError("write_coil API not available on ModbusClient")
                    # emit synthetic RX (try to use response encode if available)
                    try:
                        enc = None
                        if res is not None and hasattr(res, 'encode'):
                            try:
                                enc = res.encode()
                            except Exception:
                                enc = None
                        if enc:
                            _emit_rx(bytes(enc))
                        else:
                            # echo response for FC5
                            resp_pdu = bytes([5]) + int(offset).to_bytes(2, "big") + int(val_word).to_bytes(2, "big")
                            _emit_rx(resp_pdu)
                    except Exception:
                        pass
                    return True
                except Exception:
                    # try multiple coils
                    try:
                        # for multiple coils FC15 - build minimal pdu for single coil list
                        try:
                            fc = 15
                            qty = 1
                            coil_bytes = b"\x01" if bool(value) else b"\x00"
                            pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(qty).to_bytes(2, "big") + int(len(coil_bytes)).to_bytes(1, "big") + coil_bytes
                            _emit_tx(pdu_tx)
                        except Exception:
                            pdu_tx = None

                        if hasattr(client, "write_coils_async"):
                            res = await client.write_coils_async(offset, [bool(value)])
                        else:
                            underlying = getattr(client, "_client", None)
                            if underlying is not None and hasattr(underlying, "write_coils"):
                                res = await asyncio.to_thread(getattr(underlying, "write_coils"), offset, [bool(value)])
                            elif hasattr(client, "write_coils"):
                                res = await asyncio.to_thread(getattr(client, "write_coils"), offset, [bool(value)])
                            else:
                                res = None
                        try:
                            enc = None
                            if res is not None and hasattr(res, 'encode'):
                                try:
                                    enc = res.encode()
                                except Exception:
                                    enc = None
                            if enc:
                                _emit_rx(bytes(enc))
                            else:
                                # echo response for FC15
                                resp_pdu = bytes([15]) + int(offset).to_bytes(2, "big") + int(qty).to_bytes(2, "big")
                                _emit_rx(resp_pdu)
                        except Exception:
                            pass
                        return True
                    except Exception:
                        pass
                    raise

            # If writing a bit inside a register (bit_index not None), attempt mask write
            if bit_index is not None:
                # compute bit position according to modicon ordering
                try:
                    if modicon_bit_order:
                        pos = 15 - int(bit_index)
                    else:
                        pos = int(bit_index)
                except Exception:
                    pos = int(bit_index or 0)
                bit_mask = 1 << (pos & 0x0F)
                # desired bit value
                bit_val = 1 if bool(value) else 0
                # AND/OR masks
                if bit_val:
                    and_mask = 0xFFFF
                    or_mask = bit_mask
                else:
                    and_mask = (~bit_mask) & 0xFFFF
                    or_mask = 0
                # try mask write register (FC22)
                try:
                    # build FC22 pdu
                    try:
                        fc = 22
                        pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(and_mask).to_bytes(2, "big") + int(or_mask).to_bytes(2, "big")
                        _emit_tx(pdu_tx)
                    except Exception:
                        pdu_tx = None

                    if hasattr(client, "mask_write_register_async"):
                        res = await client.mask_write_register_async(offset, and_mask, or_mask)
                    else:
                        underlying = getattr(client, "_client", None)
                        if underlying is not None and hasattr(underlying, "mask_write_register"):
                            res = await asyncio.to_thread(getattr(underlying, "mask_write_register"), offset, and_mask, or_mask)
                        elif hasattr(client, "mask_write_register"):
                            res = await asyncio.to_thread(getattr(client, "mask_write_register"), offset, and_mask, or_mask)
                        else:
                            res = None
                    try:
                        enc = None
                        if res is not None and hasattr(res, 'encode'):
                            try:
                                enc = res.encode()
                            except Exception:
                                enc = None
                        if enc:
                            _emit_rx(bytes(enc))
                        else:
                            resp_pdu = bytes([22]) + int(offset).to_bytes(2, "big") + int(and_mask).to_bytes(2, "big") + int(or_mask).to_bytes(2, "big")
                            _emit_rx(resp_pdu)
                    except Exception:
                        pass
                    return True
                except Exception:
                    # fallback: read register, modify and write back
                    try:
                        # read single register
                        rres = await client.read_async(offset, 1, 3, encoding=encoding)
                        regs = getattr(rres, "registers", None) or []
                        cur = int(regs[0]) if regs else 0
                        if bit_val:
                            new = cur | bit_mask
                        else:
                            new = cur & (~bit_mask & 0xFFFF)
                        # write back single register (emit TX/RX)
                        try:
                            fc = 6
                            pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(new).to_bytes(2, "big")
                            _emit_tx(pdu_tx)
                        except Exception:
                            pdu_tx = None
                        if hasattr(client, "write_register_async"):
                            res = await client.write_register_async(offset, new)
                        else:
                            underlying = getattr(client, "_client", None)
                            if underlying is not None and hasattr(underlying, "write_register"):
                                res = await asyncio.to_thread(getattr(underlying, "write_register"), offset, new)
                            elif hasattr(client, "write_register"):
                                res = await asyncio.to_thread(getattr(client, "write_register"), offset, new)
                            else:
                                raise NotImplementedError("write_register API not available")
                        try:
                            enc = None
                            if res is not None and hasattr(res, 'encode'):
                                try:
                                    enc = res.encode()
                                except Exception:
                                    enc = None
                            if enc:
                                _emit_rx(bytes(enc))
                            else:
                                resp_pdu = bytes([6]) + int(offset).to_bytes(2, "big") + int(new).to_bytes(2, "big")
                                _emit_rx(resp_pdu)
                        except Exception:
                            pass
                        return True
                    except Exception:
                        raise

            # Register-level write (whole register or multi-register)
            regs_to_write = _encode_value_to_regs(value, dtype)
            if len(regs_to_write) == 1:
                # single register -> FC6
                try:
                    try:
                        fc = 6
                        pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(regs_to_write[0]).to_bytes(2, "big")
                        _emit_tx(pdu_tx)
                    except Exception:
                        pdu_tx = None
                    if hasattr(client, "write_register_async"):
                        res = await client.write_register_async(offset, regs_to_write[0])
                    else:
                        underlying = getattr(client, "_client", None)
                        if underlying is not None and hasattr(underlying, "write_register"):
                            res = await asyncio.to_thread(getattr(underlying, "write_register"), offset, regs_to_write[0])
                        elif hasattr(client, "write_register"):
                            res = await asyncio.to_thread(getattr(client, "write_register"), offset, regs_to_write[0])
                        else:
                            raise NotImplementedError("write_register API not available")
                    try:
                        enc = None
                        if res is not None and hasattr(res, 'encode'):
                            try:
                                enc = res.encode()
                            except Exception:
                                enc = None
                        if enc:
                            _emit_rx(bytes(enc))
                        else:
                            resp_pdu = bytes([6]) + int(offset).to_bytes(2, "big") + int(regs_to_write[0]).to_bytes(2, "big")
                            _emit_rx(resp_pdu)
                    except Exception:
                        pass
                    return True
                except Exception:
                    raise
            else:
                # multiple registers -> FC16
                try:
                    try:
                        fc = 16
                        qty = len(regs_to_write)
                        data_bytes = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs_to_write)
                        pdu_tx = bytes([fc]) + int(offset).to_bytes(2, "big") + int(qty).to_bytes(2, "big") + int(len(data_bytes)).to_bytes(1, "big") + data_bytes
                        _emit_tx(pdu_tx)
                    except Exception:
                        pdu_tx = None
                    if hasattr(client, "write_registers_async"):
                        res = await client.write_registers_async(offset, regs_to_write)
                    else:
                        underlying = getattr(client, "_client", None)
                        if underlying is not None and hasattr(underlying, "write_registers"):
                            res = await asyncio.to_thread(getattr(underlying, "write_registers"), offset, regs_to_write)
                        elif hasattr(client, "write_registers"):
                            res = await asyncio.to_thread(getattr(client, "write_registers"), offset, regs_to_write)
                        else:
                            raise NotImplementedError("write_registers API not available")
                    try:
                        enc = None
                        if res is not None and hasattr(res, 'encode'):
                            try:
                                enc = res.encode()
                            except Exception:
                                enc = None
                        if enc:
                            _emit_rx(bytes(enc))
                        else:
                            # FC16 response echoes address+qty
                            resp_pdu = bytes([16]) + int(offset).to_bytes(2, "big") + int(qty).to_bytes(2, "big")
                            _emit_rx(resp_pdu)
                    except Exception:
                        pass
                    return True
                except Exception:
                    raise

        finally:
            try:
                await client.close_async()
            except Exception:
                pass
