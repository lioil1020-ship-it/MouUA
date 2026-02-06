# -*- mode: python ; coding: utf-8 -*-
import os
import sys

# 直接將資源資料夾整合進來，避免在 YAML 下複雜指令
datas_list = [
    ('ui', 'ui'),
    ('images', 'images'),
    ('core', 'core'),
    ('certs', 'certs')
]

# 根據平台設定名稱
app_name = 'ModUA-macos' if sys.platform == 'darwin' else 'ModUA-onedir'

# 圖示邏輯
icon_param = None
if sys.platform == 'darwin':
    if os.path.exists('lioil.icns'):
        datas_list.insert(0, ('lioil.icns', '.'))
        icon_param = 'lioil.icns'
else:
    if os.path.exists('lioil.ico'):
        datas_list.insert(0, ('lioil.ico', '.'))
        icon_param = 'lioil.ico'

a = Analysis(
    ['ModUA.py'],
    pathex=[],
    binaries=[],
    datas=datas_list,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon=icon_param,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name=app_name, # 決定 dist 底下資料夾名稱
)