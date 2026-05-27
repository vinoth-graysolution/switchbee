import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

api_key = os.getenv("EXOTEL_API_KEY")
api_token = os.getenv("EXOTEL_API_TOKEN")
sid = os.getenv("EXOTEL_SID")

if not all([api_key, api_token, sid]):
    print("Error: Missing credentials in .env")
    exit(1)

# Fetch details for the failed call
call_sid = "1373dd83a10b8af4bc2ca0caac8a1a5m"
url = f"https://api.exotel.com/v1/Accounts/{sid}/Calls/{call_sid}.json"

print(f"Fetching call details for {call_sid} from: {url}")
resp = requests.get(url, auth=(api_key, api_token))
print(f"Status: {resp.status_code}")

if resp.status_code == 200:
    import json
    print(json.dumps(resp.json(), indent=2))
else:
    print(f"Failed to fetch call details: {resp.text}")
