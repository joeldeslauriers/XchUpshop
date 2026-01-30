cd C:\XchUpshop

pyinstaller --noconfirm --clean --onefile --windowed `
  --name ImportOrdersIntoSMS `
  --paths src `
  --hidden-import shared `
  --hidden-import shared.http_errors `
  src\drivers\import_orders_into_sms\main.py





cd C:\XchUpshop

pyinstaller --noconfirm --clean --onefile --windowed `
  --name PushSMSPOToMaceri `
  --paths src `
  --version-file src\drivers\push_sms_po_to_maceri\version_info.txt `
  src\drivers\push_sms_po_to_maceri\main.py



pyinstaller --noconfirm --clean --onefile --windowed `
  --name pushSMSPOtoUNFI `
  --paths src `
  --version-file src\drivers\push_sms_po_to_unfi\version_info.txt `
  src\drivers\push_sms_po_to_unfi\main.py
