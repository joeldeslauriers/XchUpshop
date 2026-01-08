cd C:\XchUpshop

pyinstaller --noconfirm --clean --onefile --windowed `
  --name ImportOrdersIntoSMS `
  --paths src `
  --hidden-import shared `
  --hidden-import shared.http_errors `
  src\drivers\import_orders_into_sms\main.py
