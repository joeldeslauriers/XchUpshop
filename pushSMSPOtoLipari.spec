# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['src\\drivers\\push_sms_po_to_lipari\\main.py'],
    pathex=['src'],
    binaries=[],
    datas=[('src\\drivers\\push_sms_po_to_lipari\\config.ini', '.'), ('src\\drivers\\push_sms_po_to_lipari\\version_info.txt', '.')],
    hiddenimports=['ui_send_PO_Lipari'],
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
    name='pushSMSPOtoLipari',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
