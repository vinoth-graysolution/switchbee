import csv
import requests
import time


NGROK_URL = "https://psychologically-nonprecious-vonnie.ngrok-free.dev/start"


with open("D:\conversation_ai\src\candidates.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:

        candidate_name = row["name"]
        phone_number = row["phone"]
        # role = row["role"]

        payload = {
            "dialout_settings": {
                "phone_number": phone_number
            },
            "candidate_data": {
                "name": candidate_name,
                # "role": role
            }
        }

        print(f"\nCalling {candidate_name} -> {phone_number}")

        response = requests.post(
            NGROK_URL,
            json=payload
        )

        print("Status:", response.status_code)
        print("Response:", response.text)

        # Delay between calls
        time.sleep(5)