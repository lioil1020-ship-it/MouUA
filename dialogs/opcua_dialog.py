import sys, os
# Allow running this dialog file directly from the project folder.
# When executed directly, ensure the project root is on sys.path so
# imports like `ui.widgets.form_builder` resolve correctly.
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from PyQt6.QtWidgets import (
    QDialog, QDialogButtonBox, QVBoxLayout, QLabel, QTabWidget, 
    QWidget, QHBoxLayout, QCheckBox, QFormLayout, QLineEdit
)
import socket
from PyQt6.QtCore import Qt
from ui.widgets.form_builder import FormBuilder
import psutil

class OPCUADialog(QDialog):
    def __init__(self, parent=None, initial=None):
        super().__init__(parent)
        self.setWindowTitle("OPC UA Server")
        self.resize(640, 560)

        # 一致化參數
        _row_spacing = 12
        _h_margin = 20
        _v_margin = 18
        _form_max_width = 600

        # 建立主分頁控制項
        self.tabs = QTabWidget(self)

        # --- 1. Settings Tab (已移除多餘的 Description 欄位) ---
        self.settings_tab = QWidget()
        s_layout = QVBoxLayout(self.settings_tab)
        s_layout.setSpacing(_row_spacing)
        s_layout.setContentsMargins(_h_margin, _v_margin, _h_margin, _v_margin)
        
        self.settings_form = FormBuilder(self.settings_tab)
        self.settings_form.layout.setSpacing(_row_spacing)
        try:
            self.settings_form.setMaximumWidth(_form_max_width)
        except: pass
        self.settings_form.add_field('application_Name', 'Application Name')
        self.settings_form.add_field('host_name', 'Host Name')
        self.settings_form.add_field('namespace', 'Namespace')
        self.settings_form.add_field('port', 'Port')
        self.settings_form.add_field('product_uri', 'Product URI(End Point)')
        # 將 Product URI 顯示為靜態文字（用 QLabel 替換輸入框）
        self._setup_product_uri_display()
        # Network adapter selector (combo). We'll populate with adapter names and IPv4 addresses
        self.settings_form.add_field('network_adapter', 'Network Adapter', field_type='combo', options=[])
        # keep a hidden field for adapter ip so values() returns it
        from PyQt6.QtWidgets import QLineEdit
        self._adapter_ip_hidden = QLineEdit()
        self._adapter_ip_hidden.setVisible(False)
        self.settings_form.fields['network_adapter_ip'] = self._adapter_ip_hidden
        # 已移除「Auto Start OPC UA Server on application startup」選項
        self.settings_form.add_field('max_sessions', 'Max Sessions')
        self.settings_form.add_field('publish_interval', 'Publish Interval (ms)')
        
        s_layout.addWidget(self.settings_form)
        s_layout.addStretch()

        # --- 2. Authentication Tab (具備 Username/Password 動態顯示邏輯) ---
        self.auth_tab = QWidget()
        a_layout = QVBoxLayout(self.auth_tab)
        a_layout.setSpacing(_row_spacing)
        a_layout.setContentsMargins(_h_margin, _v_margin, _h_margin, _v_margin)
        
        self.auth_form = FormBuilder(self.auth_tab)
        self.auth_form.layout.setSpacing(_row_spacing)
        try:
            self.auth_form.setMaximumWidth(_form_max_width)
        except: pass
        self.auth_form.add_field('authentication', 'Authentication', field_type='combo', 
                                 options=['Anonymous', 'Username/Password'], 
                                 default='Anonymous')
        self.auth_form.add_field('username', 'Username')
        self.auth_form.add_field('password', 'Password')
        
        a_layout.addWidget(self.auth_form)
        a_layout.addStretch()

        # --- 3. Security Policies Tab (僅 7 個勾選框，無框框，與 Form 同步行距) ---
        self.sec_tab = QWidget()
        sec_layout = QVBoxLayout(self.sec_tab)
        sec_layout.setSpacing(_row_spacing) # 保持一致行距
        sec_layout.setContentsMargins(_h_margin, _v_margin, _h_margin, _v_margin)

        self.sec_checkboxes = {
            'policy_none': QCheckBox('None'),
            'policy_sign_aes128': QCheckBox('Sign - Aes128'),
            'policy_sign_aes256': QCheckBox('Sign - Aes256'),
            'policy_sign_basic256sha256': QCheckBox('Sign - Basic256Sha256'),
            'policy_encrypt_aes128': QCheckBox('Sign & Encrypt - Aes128'),
            'policy_encrypt_aes256': QCheckBox('Sign & Encrypt - Aes256'),
            'policy_encrypt_basic256sha256': QCheckBox('Sign & Encrypt - Basic256Sha256')
        }

        for cb in self.sec_checkboxes.values():
            sec_layout.addWidget(cb)
        sec_layout.addStretch()

        # --- 4. Certificate Tab (根據圖例更新，已移除最下方 Checkbox) ---
        self.cert_tab = QWidget()
        cert_layout = QVBoxLayout(self.cert_tab)
        cert_layout.setSpacing(_row_spacing)
        cert_layout.setContentsMargins(_h_margin, _v_margin, _h_margin, _v_margin)
        
        self.auto_generate = QCheckBox('Auto Generate Certificate')
        cert_layout.addWidget(self.auto_generate)

        self.cert_form = FormBuilder(self.cert_tab)
        self.cert_form.layout.setSpacing(_row_spacing)
        try:
            self.cert_form.setMaximumWidth(_form_max_width)
        except: pass
        
        self.common_name_label = QLabel('ModUA@ModUA')
        self.common_name_label.setStyleSheet("color: #888;")
        self.cert_form.layout.addRow("Common Name", self.common_name_label)
        
        self.cert_form.add_field('organization', 'Organization')
        self.cert_form.add_field('organization_unit', 'Organization Unit')
        self.cert_form.add_field('locality', 'Locality')
        self.cert_form.add_field('state', 'State')
        
        # Country + 提示說明
        country_h = QHBoxLayout()
        self.country_input = QLineEdit()
        country_h.addWidget(self.country_input)
        country_h.addWidget(QLabel("(Two letter code, e.g. DE, US, ...)"))
        self.cert_form.layout.addRow("Country", country_h)
        self.cert_form.fields['country'] = self.country_input

        # Validity + 提示說明
        validity_h = QHBoxLayout()
        self.validity_input = QLineEdit()
        validity_h.addWidget(self.validity_input)
        validity_h.addWidget(QLabel("(Years, 1 - 20)"))
        self.cert_form.layout.addRow("Certificate Validity", validity_h)
        self.cert_form.fields['cert_validity'] = self.validity_input

        cert_layout.addWidget(self.cert_form)
        cert_layout.addStretch()

        # --- 組裝主視圖 ---
        self.tabs.addTab(self.settings_tab, 'Settings')
        self.tabs.addTab(self.auth_tab, 'Authentication')
        self.tabs.addTab(self.sec_tab, 'Security Policies')
        self.tabs.addTab(self.cert_tab, 'Certificate')

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(_h_margin, _v_margin, _h_margin, _v_margin)
        main_layout.addWidget(QLabel('Configure OPC UA Server parameters below:'))
        main_layout.addWidget(self.tabs)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)

        # 初始初始化
        self._apply_defaults(initial)
        self._connect_endpoint_updaters()
        self._setup_auth_visibility()
        self._update_endpoint_label()

    # --- 邏輯功能輔助方法 ---

    def _setup_auth_visibility(self):
        """控制 Username 與 Password 欄位的動態顯示與隱藏"""
        combo = self.auth_form.fields.get('authentication')
        def toggle():
            is_up = combo.currentText() == 'Username/Password'
            for key in ['username', 'password']:
                widget = self.auth_form.fields.get(key)
                if widget:
                    widget.setVisible(is_up)
                    label = self.auth_form.layout.labelForField(widget)
                    if label: label.setVisible(is_up)
        if combo:
            combo.currentTextChanged.connect(toggle)
            toggle()

    def _setup_product_uri_display(self):
        """將 URI 欄位替換為可選取的藍色文字標籤"""
        try:
            old_w = self.settings_form.fields.get('product_uri')
            if old_w:
                # 使用原來的 QLineEdit，改為唯讀並調整樣式成靜態藍色文字，避免變動 layout
                try:
                    # 保留引用，讓其他程式可以用 endpoint_display 存取
                    self.endpoint_display = old_w
                    old_w.setReadOnly(True)
                    old_w.setStyleSheet("font-weight: bold; color: #0066cc; background: transparent; border: none;")
                    # 確保字可以被選取
                    try:
                        old_w.setCursorPosition(0)
                    except: pass
                except Exception:
                    pass
        except:
            pass

    def _update_endpoint_label(self):
        """即時計算並顯示 opc.tcp 連線字串，優先使用選取的 network adapter IP。"""
        try:
            vals = self.settings_form.get_values()
            # prefer selected network adapter IP; fall back to host_name or auto-detect
            host = ''
            try:
                na_widget = self.settings_form.fields.get('network_adapter')
                if na_widget and hasattr(na_widget, 'currentData'):
                    ipdata = na_widget.currentData()
                    if ipdata:
                        host = str(ipdata).strip()
                if not host:
                    host = (vals.get('network_adapter_ip') or '').strip()
            except Exception:
                host = ''
            if not host:
                host = (vals.get('host_name') or '').strip()
            port = (vals.get('port') or '').strip()

            def _get_ip():
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.settimeout(0.2); s.connect(('8.8.8.8', 80))
                    ip = s.getsockname()[0]; s.close(); return ip
                except: return '127.0.0.1'

            # if host resolves to loopback or is empty, auto-detect a LAN IP
            try:
                if not host or host.lower() in ('localhost', '127.0.0.1', 'modua'):
                    h = _get_ip()
                else:
                    h = host
            except Exception:
                h = _get_ip()

            # expose chosen adapter ip in hidden field for persistence
            try:
                if hasattr(self, '_adapter_ip_hidden'):
                    self._adapter_ip_hidden.setText(h)
            except Exception:
                pass

            if hasattr(self, 'endpoint_display'):
                self.endpoint_display.setText(f"opc.tcp://{h}:{port if port else '48480'}")
        except: pass

    def _connect_endpoint_updaters(self):
        # update when port changes or network adapter selection changes
        for k in ['host_name', 'port', 'network_adapter']:
            w = self.settings_form.fields.get(k)
            try:
                if hasattr(w, 'textChanged'):
                    w.textChanged.connect(self._update_endpoint_label)
                elif hasattr(w, 'currentIndexChanged'):
                    w.currentIndexChanged.connect(self._on_adapter_changed)
            except Exception:
                pass
        # initially populate adapters
        try:
            self._populate_adapters()
        except Exception:
            pass

    def _on_adapter_changed(self, idx=None):
        try:
            na = self.settings_form.fields.get('network_adapter')
            if na and hasattr(na, 'currentData'):
                ip = na.currentData()
                try:
                    self._adapter_ip_hidden.setText(str(ip or ''))
                except Exception:
                    pass
            self._update_endpoint_label()
        except Exception:
            pass

    def _populate_adapters(self):
        """Populate the network adapter combo with available IPv4 addresses using psutil."""
        try:
            na_widget = self.settings_form.fields.get('network_adapter')
            if na_widget is None:
                return
            try:
                na_widget.clear()
            except Exception:
                pass
            infos = psutil.net_if_addrs()
            for ifname, addrs in infos.items():
                for a in addrs:
                    try:
                        fam = getattr(a, 'family', None)
                        if fam and (getattr(fam, 'name', '').endswith('AF_INET') or fam == 2):
                            ip = a.address
                            if ip and not ip.startswith('127.'):
                                display = f"{ifname} - {ip}"
                                na_widget.addItem(display, ip)
                    except Exception:
                        try:
                            ip = getattr(a, 'address', None) or str(a)
                            if ip and ':' not in ip and not ip.startswith('127.'):
                                display = f"{ifname} - {ip}"
                                na_widget.addItem(display, ip)
                        except Exception:
                            pass
            if na_widget.count() == 0:
                detected = None
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.settimeout(0.2); s.connect(('8.8.8.8', 80)); detected = s.getsockname()[0]; s.close()
                except Exception:
                    detected = '127.0.0.1'
                na_widget.addItem(f"Auto - {detected}", detected)
            try:
                vals = self.settings_form.get_values()
                target = vals.get('network_adapter_ip') or None
                if target:
                    idx = na_widget.findData(target)
                    if idx >= 0:
                        na_widget.setCurrentIndex(idx)
                        self._adapter_ip_hidden.setText(str(target))
            except Exception:
                pass
        except Exception:
            pass

    def _apply_defaults(self, initial):
        defaults = {
            'application_Name':'ModUA','host_name': 'ModUA', 'namespace': 'ModUA', 'port': '48480',
            'policy_none': True, 'policy_sign_aes128': False, 'policy_sign_aes256': False,
            'policy_sign_basic256sha256': False, 'policy_encrypt_aes128': False,
            'policy_encrypt_aes256': False, 'policy_encrypt_basic256sha256': False,
            'auto_generate': True,
            # 預設憑證欄位
            'common_name': 'ModUA@lioil',
            'organization': 'Organization',
            'organization_unit': 'Unit',
            'locality': 'Locality',
            'state': 'State',
            'country': 'tw',
            'cert_validity': '20',
            # 其他預設
            'max_sessions': '4096', 'publish_interval': '1000'
        }
        self.set_values(defaults)
        if initial: self.set_values(initial)

    def set_values(self, data: dict):
        if not data: return
        for f in [self.settings_form, self.auth_form, self.cert_form]:
            f.set_values(data)
        for k, cb in self.sec_checkboxes.items():
            cb.setChecked(bool(data.get(k, False)))
        self.auto_generate.setChecked(bool(data.get('auto_generate', True)))
        # 支援設置 Common Name（在表單中為 QLabel）
        if 'common_name' in data and hasattr(self, 'common_name_label'):
            try:
                self.common_name_label.setText(str(data.get('common_name') or ''))
            except: pass
        self._update_endpoint_label()

    def values(self) -> dict:
        out = {}
        for f in [self.settings_form, self.auth_form, self.cert_form]:
            out.update(f.get_values())
        for k, cb in self.sec_checkboxes.items():
            out[k] = cb.isChecked()
        out['auto_generate'] = self.auto_generate.isChecked()
        if hasattr(self, 'endpoint_display'):
            out['product_uri'] = self.endpoint_display.text()
        return out