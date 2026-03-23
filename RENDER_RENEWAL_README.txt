Render deployment notes for the subscription renewal notifier
============================================================

What it does
------------
- Reads your Excel workbook
- Uses the sheet: Subscription Details-Line Level
- Keeps only rows where Status = ACTIVE
- Finds rows where Renewal Date is exactly 60 days from the run date
- Sends only End Customer, Subscription ID, and Renewal Date to your Webex space
- Stores sent items in a JSON state file so the same renewal is not posted twice

Recommended Render setup
------------------------
Use a Render Cron Job.

Files to put in your repo
-------------------------
- render_renewal_notifier.py
- requirements_render_renewal.txt
- render.yaml
- subscriptions.xlsx

Environment variables in Render
-------------------------------
Required:
- WEBEX_ACCESS_TOKEN
- WEBEX_ROOM_ID

Defaults already included in render.yaml:
- EXCEL_FILE=subscriptions.xlsx
- SHEET_NAME=Subscription Details-Line Level
- STATUS_FILTER=ACTIVE
- NOTIFY_DAYS=60
- PER_MESSAGE=15
- INCLUDE_HEADER=true
- ALLOW_DUPLICATES=false
- STATE_FILE=/var/data/sent_notifications.json

Important for the Excel file
----------------------------
You have two good options:
1) Keep subscriptions.xlsx in the repo and redeploy whenever you update it
2) Better: attach a Render persistent disk and store the live workbook there, then set:
   EXCEL_FILE=/var/data/subscriptions.xlsx

Persistent disk recommendation
------------------------------
Attach a persistent disk to the cron job and mount it at /var/data.
Then put these files there:
- /var/data/subscriptions.xlsx
- /var/data/sent_notifications.json

Why the disk matters:
- sent_notifications.json survives restarts and deploys
- your updated workbook can live outside the repo

Suggested schedule
------------------
The sample render.yaml uses:
- 0 13 * * *
That runs daily at 13:00 UTC.

For Eastern Time:
- 13:00 UTC is 9:00 AM EDT
- 14:00 UTC is 9:00 AM EST
Render cron schedules are in UTC, so you may need to adjust for daylight saving time.

Testing locally
---------------
PowerShell example:
$env:WEBEX_ACCESS_TOKEN="your_token"
$env:WEBEX_ROOM_ID="your_room_id"
$env:RUN_DATE="2026-03-23"
python .\render_renewal_notifier.py

RUN_DATE lets you simulate a specific day.
For example, if RUN_DATE is 2026-03-23 and NOTIFY_DAYS is 60,
the script will notify renewals on 2026-05-22.

How duplicate prevention works
------------------------------
Each sent item is keyed by:
- End Customer
- Subscription ID
- Renewal Date
- Status

If the same row already exists in sent_notifications.json, it will not be posted again.

Common changes you may want later
---------------------------------
- Change NOTIFY_DAYS from 60 to 90 or 30
- Add a second reminder window
- Send a summary count first, then the detail rows
- Include only one Webex message with a CSV attachment instead of markdown rows
