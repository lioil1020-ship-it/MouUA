from PyQt6.QtWidgets import QWidget, QFormLayout, QLineEdit, QComboBox, QLabel, QSpinBox

class FormBuilder(QWidget):
    ROW_HEIGHT = 22

    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QFormLayout(self)
        # reduce spacing so rows align closely with table rows
        self.layout.setVerticalSpacing(2)
        self.fields = {} # 儲存 ID 與元件的對應字典 📥

        # enforce child widget heights via stylesheet as fallback
        self.setStyleSheet(
            f"QLineEdit, QComboBox, QSpinBox {{ min-height: {self.ROW_HEIGHT}px; max-height: {self.ROW_HEIGHT}px; }} QLabel {{ min-height: {self.ROW_HEIGHT}px; max-height: {self.ROW_HEIGHT}px; }}"
        )

    def add_field(self, field_id, label_text, field_type="text", options=None, default=""):
        """新增欄位"""
        label = QLabel(label_text)
        if field_type == "text":
            widget = QLineEdit()
            widget.setText(str(default))
        elif field_type == "combo":
            widget = QComboBox()
            if options:
                widget.addItems(options)
            if default:
                widget.setCurrentText(str(default))
        else:
            # fallback to line edit for unknown types
            widget = QLineEdit()
            widget.setText(str(default))

        # enforce fixed height to match table row height
        try:
            label.setFixedHeight(self.ROW_HEIGHT)
            widget.setFixedHeight(self.ROW_HEIGHT)
        except Exception:
            pass

        self.layout.addRow(label, widget)
        self.fields[field_id] = widget

    def clear_form(self):
        """徹底清空佈局，解決 Ethernet 畫面殘留 Serial 選單的問題"""
        while self.layout.count() > 0:
            self.layout.removeRow(0)
        self.fields = {} 

    def get_values(self):
        """獲取欄位數值"""
        values = {}
        for fid, widget in self.fields.items():
            if isinstance(widget, QLineEdit):
                values[fid] = widget.text()
            elif isinstance(widget, QComboBox):
                values[fid] = widget.currentText()
            else:
                # generic handling
                try:
                    values[fid] = widget.text()
                except Exception:
                    values[fid] = None
        return values

    def set_values(self, data):
        """填回數值 (編輯內容時使用)"""
        if not data: return
        for fid, value in data.items():
            if fid in self.fields:
                widget = self.fields[fid]
                if isinstance(widget, QLineEdit):
                    widget.setText(str(value))
                elif isinstance(widget, QComboBox):
                    idx = widget.findText(str(value))
                    if idx >= 0: widget.setCurrentIndex(idx)