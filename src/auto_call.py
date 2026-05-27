import csv
import requests
import time
import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()


# Read the outbound server URL from .env (auto-updates when ngrok restarts)
OUTBOUND_SERVER_URL = os.getenv("OUTBOUND_SERVER_URL", "")
NGROK_URL = OUTBOUND_SERVER_URL if OUTBOUND_SERVER_URL else ""

# Exotel credentials
ACCOUNT_SID = os.getenv("EXOTEL_SID")
API_KEY = os.getenv("EXOTEL_API_KEY")
API_TOKEN = os.getenv("EXOTEL_API_TOKEN")
EXOTEL_SUBDOMAIN = os.getenv("EXOTEL_SUBDOMAIN", "api.exotel.com")

# Exotel call details API (uses correct subdomain from .env)
CALL_DETAILS_URL = f"https://{EXOTEL_SUBDOMAIN}/v1/Accounts/{ACCOUNT_SID}/Calls/{{call_sid}}"


def wait_until_call_completed(call_sid):
    error_count = 0
    while True:
        try:
            response = requests.get(
                CALL_DETAILS_URL.format(call_sid=call_sid),
                auth=(API_KEY, API_TOKEN)
            )

            print("Status Code:", response.status_code)
            print("Raw Response:", response.text)

            # Reset error count on successful HTTP request
            error_count = 0

            # Parse XML response safely using ElementTree
            root = ET.fromstring(response.text)
            status_elem = root.find(".//Status")
            if status_elem is not None and status_elem.text:
                call_status = status_elem.text.strip()
            else:
                raise ValueError("Status tag not found or empty in response XML")

            print(f"Current Call Status: {call_status}")

            if call_status.lower() in [
                "completed",
                "busy",
                "failed",
                "no-answer",
                "canceled"
            ]:
                print("Call finished.")
                return call_status.lower()

        except Exception as e:
            error_count += 1
            print(f"Status Check Error (attempt {error_count}/10):", str(e))
            if error_count >= 10:
                print("Too many consecutive status check errors. Moving to next candidate.")
                return "failed"

        time.sleep(10)


UNANSWERED_CSV = r"D:\conversation_ai\src\unanswered.csv"


def write_unanswered(name, phone, role, status):
    file_exists = os.path.exists(UNANSWERED_CSV)
    with open(UNANSWERED_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["name", "phone", "role", "status"])
        writer.writerow([name, phone, role, status])


with open(r"D:\conversation_ai\src\candidates.csv", "r", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    for row in reader:

        candidate_name = row["name"]
        phone_number = row["phone"]
        candidate_role = row.get("role", "")

        payload = {
            "dialout_settings": {
                "phone_number": phone_number
            },
            "candidate_data": {
                "name": candidate_name,
                "role": candidate_role
            }
        }

        print(f"\nCalling {candidate_name} -> {phone_number}")

        try:

            response = requests.post(
                NGROK_URL,
                json=payload,
                timeout=30
            )

            print("Start API Status:", response.status_code)

            response_data = response.json()

            # IMPORTANT:
            # Your backend must return Exotel Call SID
            call_sid = response_data["call_sid"]

            print("Call SID:", call_sid)

            # Wait until call completed
            final_status = wait_until_call_completed(call_sid)

            if final_status != "completed":
                print(f"Call not answered (status: {final_status}). Saving to unanswered.csv")
                write_unanswered(candidate_name, phone_number, candidate_role, final_status)

        except Exception as e:
            print("Error:", str(e))
            write_unanswered(candidate_name, phone_number, candidate_role, "failed")