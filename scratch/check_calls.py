import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

api_key = os.getenv("EXOTEL_API_KEY")
api_token = os.getenv("EXOTEL_API_TOKEN")
sid = os.getenv("EXOTEL_SID")
subdomain = os.getenv("EXOTEL_SUBDOMAIN", "api.exotel.com")

if not all([api_key, api_token, sid]):
    print("Error: Missing credentials in .env")
    exit(1)

# List call records
url = f"https://api.exotel.com/v1/Accounts/{sid}/Calls.json"
params = {
    "Limit": 10
}

print(f"Fetching calls from: {url}")
resp = requests.get(url, params=params, auth=(api_key, api_token))
print(f"Status: {resp.status_code}")

if resp.status_code == 200:
    data = resp.json()
    calls = data.get("Calls", [])
    print(f"Retrieved {len(calls)} calls:")
    for c in calls:
         print(f"Sid: {c.get('Sid')} | Date: {c.get('DateCreated')} | From: {c.get('From')} | To: {c.get('To')} | Status: {c.get('Status')} | CustomField: {c.get('CustomField')}")
else:
    print(f"Failed to fetch calls: {resp.text}")




