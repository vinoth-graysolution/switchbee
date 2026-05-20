"""
test_exotel_auth.py
Run this to verify your Exotel credentials are correct.
Usage: uv run src/test_exotel_auth.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# ── Load credentials ─────────────────────────────────────────
api_key   = os.getenv("EXOTEL_API_KEY", "")
api_token = os.getenv("EXOTEL_API_TOKEN", "")
sid       = os.getenv("EXOTEL_SID", "")
from_num  = os.getenv("EXOTEL_PHONE_NUMBER", "")

print("=" * 60)
print("  EXOTEL CREDENTIAL CHECK")
print("=" * 60)
print(f"  SID        : {sid!r}")
print(f"  API_KEY    : {api_key[:8]}…{api_key[-4:] if len(api_key) > 12 else '(short)'}")
print(f"  API_TOKEN  : {api_token[:8]}…{api_token[-4:] if len(api_token) > 12 else '(short)'}")
print(f"  FROM NUMBER: {from_num!r}")
print()

if not all([api_key, api_token, sid]):
    print("❌  One or more credentials are MISSING in .env!")
    raise SystemExit(1)

# ── Build the URL exactly as service.py does ─────────────────
url = f"https://api.exotel.com/v1/Accounts/{sid}/Calls/connect"
print(f"  URL: {url}")
print()

# ── Test with a dummy call (will fail on 'To' validation but confirms auth) ─
data = {
    "From":     from_num,
    "To":       from_num,   # calling yourself — safe test
    "CallerId": from_num,
    "CallType": "trans",
}

print("  Sending test request…")
resp = requests.post(url, data=data, auth=(api_key, api_token), timeout=15)
print(f"  HTTP Status : {resp.status_code}")
print(f"  Response    : {resp.text[:500]}")
print()

if resp.status_code == 401:
    print("❌  401 Unauthorized — credentials are WRONG or EXPIRED.")
    print()
    print("  Fix options:")
    print("  1. Log into https://my.exotel.com → Settings → API")
    print("     and copy the CURRENT API Key & Token into .env")
    print("  2. Make sure EXOTEL_SID matches your Account SID exactly")
    print("     (check top-right corner of the Exotel dashboard)")
    print("  3. Some accounts need subdomain — try:")
    print("     url = f'https://{api_key}:{api_token}@api.exotel.com/...'")
elif resp.status_code in (200, 201, 400, 422):
    print("✅  Auth OK — credentials are valid! (4xx may be a payload issue, not auth)")
else:
    print(f"⚠   Unexpected status {resp.status_code}")
