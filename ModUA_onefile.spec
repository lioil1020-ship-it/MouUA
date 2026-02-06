# -*- mode: python ; coding: utf-8 -*-
import os
import sys

datas_list = [
    ('ui', 'ui'),
    ('images', 'images'),
    ('core', 'core'),
    ('certs', 'certs')
]

app_name = 'ModUA-macos-onefile' if sys.platform == 'darwin' else 'ModUA-onefile'

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
    a.binaries,
    a.datas,
    [],
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False,
    icon=icon_param,
)