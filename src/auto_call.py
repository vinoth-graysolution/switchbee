"""
auto_call.py

Concurrent outbound AI call trigger system.

Features:
- Async concurrent outbound calling
- Configurable concurrency limit
- Exotel-friendly throttling
- Better error handling
- Production-ready structure
"""

import asyncio
import csv
from typing import Dict, List

import aiohttp

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

NGROK_URL = "https://psychologically-nonprecious-vonnie.ngrok-free.dev/start"

CANDIDATES_CSV = r"D:\conversation_ai\src\candidates.csv"

# IMPORTANT:
# Set this based on your Exotel concurrent limit.
#
# Example:
# If Exotel allows 10 concurrent slots
# and each AI conversation uses 2 call legs:
#
# MAX_CONCURRENT_CALLS = 5
#
MAX_CONCURRENT_CALLS = 5

# Small delay between scheduling calls
# Prevents sudden API bursts
CALL_SPAWN_DELAY = 1

# HTTP timeout
REQUEST_TIMEOUT = 30

# ─────────────────────────────────────────────────────────────
# Semaphore
# ─────────────────────────────────────────────────────────────

semaphore = asyncio.Semaphore(MAX_CONCURRENT_CALLS)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def load_candidates() -> List[Dict]:
    """Load candidates from CSV."""
    candidates = []

    with open(CANDIDATES_CSV, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            phone = row.get("phone", "").strip()

            if not phone:
                continue

            candidates.append(
                {
                    "name": row.get("name", "").strip(),
                    "phone": phone,
                    "role": row.get("role", "").strip(),
                }
            )

    return candidates


# ─────────────────────────────────────────────────────────────
# Call Logic
# ─────────────────────────────────────────────────────────────

async def trigger_call(
    session: aiohttp.ClientSession,
    candidate: Dict,
):
    """
    Trigger one outbound AI call.
    """

    async with semaphore:

        name = candidate["name"]
        phone = candidate["phone"]
        role = candidate["role"]

        payload = {
            "dialout_settings": {
                "phone_number": phone
            },
            "candidate_data": {
                "name": name,
                "role": role,
            },
        }

        print("=" * 60)
        print(f"📞 Starting Call")
        print(f"👤 Candidate : {name}")
        print(f"💼 Role      : {role}")
        print(f"📱 Phone     : {phone}")
        print("=" * 60)

        try:
            async with session.post(
                NGROK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:

                response_text = await response.text()

                print(f"✅ Status : {response.status}")

                if response.status == 200:
                    print(f"🚀 Call initiated successfully")
                else:
                    print(f"❌ Failed to initiate call")

                print(f"📄 Response : {response_text[:300]}")

        except asyncio.TimeoutError:
            print(f"⏰ Timeout while calling {phone}")

        except aiohttp.ClientError as e:
            print(f"🌐 Network error for {phone}: {e}")

        except Exception as e:
            print(f"⚠ Unexpected error for {phone}: {e}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main():

    print("\n" + "=" * 70)
    print("🤖 CONCURRENT AI CALLING SYSTEM")
    print("=" * 70)

    print(f"📊 Max Concurrent Calls : {MAX_CONCURRENT_CALLS}")

    candidates = load_candidates()

    print(f"📂 Loaded Candidates    : {len(candidates)}")

    if not candidates:
        print("❌ No valid candidates found")
        return

    connector = aiohttp.TCPConnector(limit=100)

    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = []

        for candidate in candidates:

            task = asyncio.create_task(
                trigger_call(session, candidate)
            )

            tasks.append(task)

            # Small stagger delay
            await asyncio.sleep(CALL_SPAWN_DELAY)

        print("\n🚀 All call tasks scheduled\n")

        await asyncio.gather(*tasks)

    print("\n" + "=" * 70)
    print("✅ ALL CALLS COMPLETED")
    print("=" * 70)


# ─────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(main())