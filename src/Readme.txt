========================================
XchUpshop – Aide-mémoire Build PyInstaller
========================================

Dossier de travail
------------------
cd C:\XchUpshop


Règles rapides
--------------
ONEFILE
- 1 seul .exe
- Démarrage plus lent
- Debug plus difficile

ONEDIR (Utilisé avec nouvelle version)
- Démarrage rapide
- Plus stable
- Dossier avec _internal
- Meilleur pour la prod

⚠ config.ini
- Un seul config.ini partagé
- Placé dans le dossier PARENT du dossier EXE
- Ne PAS utiliser --add-data pour config.ini (sauf legacy)


========================================
ImportOrdersIntoSMS
========================================

ONEFILE
pyinstaller --noconfirm --clean --onefile --windowed `
  --name ImportOrdersIntoSMS `
  --paths src `
  --hidden-import shared `
  --hidden-import shared.http_errors `
  src\drivers\import_orders_into_sms\main.py

ONEDIR
pyinstaller --noconfirm --clean --onedir --windowed `
  --name ImportOrdersIntoSMS `
  --paths src `
  --hidden-import shared `
  --hidden-import shared.http_errors `
  --version-file src\drivers\import_orders_into_sms\version_info.txt `
  src\drivers\import_orders_into_sms\main.py



========================================
PushSMSPOToMaceri
========================================

ONEFILE
pyinstaller --noconfirm --clean --onefile --windowed `
  --name PushSMSPOToMaceri `
  --paths src `
  --version-file src\drivers\push_sms_po_to_maceri\version_info.txt `
  src\drivers\push_sms_po_to_maceri\main.py

ONEDIR
pyinstaller --noconfirm --clean --onedir --windowed `
  --name PushSMSPOToMaceri `
  --paths src `
  --version-file src\drivers\push_sms_po_to_maceri\version_info.txt `
  src\drivers\push_sms_po_to_maceri\main.py

========================================
pushSMSPOtoUNFI
========================================

ONEFILE
pyinstaller --noconfirm --clean --onefile --windowed `
  --name pushSMSPOtoUNFI `
  --paths src `
  --version-file src\drivers\push_sms_po_to_unfi\version_info.txt `
  src\drivers\push_sms_po_to_unfi\main.py

ONEDIR
pyinstaller --noconfirm --clean --onedir --windowed `
  --name PushSMSPOtoUNFI `
  --paths src `
  --version-file src\drivers\push_sms_po_to_unfi\version_info.txt `
  --collect-all tkinter `
  src\drivers\push_sms_po_to_unfi\main.py



========================================
pushSMSPOtoLipari
========================================

ONEFILE (legacy – config embarqué)
pyinstaller --noconfirm --clean --onefile --windowed `
  --name pushSMSPOtoLipari `
  --paths src `
  --hidden-import ui_send_PO_Lipari `
  --add-data "src\drivers\push_sms_po_to_lipari\config.ini;." `
  --add-data "src\drivers\push_sms_po_to_lipari\version_info.txt;." `
  src\drivers\push_sms_po_to_lipari\main.py

ONEDIR (RECOMMANDÉ)
pyinstaller --noconfirm --clean --onedir --windowed `
  --name PushSMSPOtoLipari `
  --paths src `
  --hidden-import ui_send_PO_Lipari `
  --version-file src\drivers\push_sms_po_to_lipari\version_info.txt `
  src\drivers\push_sms_po_to_lipari\main.py

========================================
GetSPSInvoices
========================================

ONEFILE (legacy – config embarqué)
pyinstaller --noconfirm --clean --onefile --windowed `
  --name GetSPSInvoices `
  --paths src `
  --hidden-import ui_getSPSInvoices `
  --add-data "src\drivers\get_sps_invoices\config.ini;." `
  --add-data "src\drivers\get_sps_invoices\version_info.txt;." `
  src\drivers\get_sps_invoices\main.py


ONEDIR
pyinstaller --noconfirm --clean --onedir --windowed `
  --name GetSPSInvoices `
  --paths src `
  --version-file src\drivers\get_sps_invoices\version_info.txt `
  src\drivers\get_sps_invoices\main.py




