#
# Copyright (c) 2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""service.py

Webhook server to handle outbound call requests, initiate calls via Exotel API,
handle subsequent WebSocket connections for Media Streams, and automatically
separate candidates into interested.csv / not_interested.csv.
"""

import csv
import os
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv(override=True)


# ─────────────────────────────────────────────────────────────
# CSV output paths  (same folder as candidates.csv)
# ─────────────────────────────────────────────────────────────

_SRC_DIR = Path(__file__).parent
INTERESTED_CSV = _SRC_DIR / "interested.csv"
NOT_INTERESTED_CSV = _SRC_DIR / "not_interested.csv"

CSV_FIELDNAMES = ["name", "phone", "role", "outcome"]


def _ensure_csv(path: Path):
    """Create CSV file with header if it does not exist yet."""
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()


def write_outcome(name: str, phone: str, role: str, outcome: str):
    """
    Append a candidate row to the appropriate CSV file.

    Args:
        name:    Candidate name.
        phone:   Candidate phone number.
        role:    Job role applied for.
        outcome: 'interested' or 'not_interested'.
    """
    csv_path = INTERESTED_CSV if outcome == "interested" else NOT_INTERESTED_CSV
    _ensure_csv(csv_path)

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writerow(
            {"name": name, "phone": phone, "role": role, "outcome": outcome}
        )

    print(
        f"[CSV] {outcome.upper()}: {name} ({phone}) → "
        f"{'interested.csv' if outcome == 'interested' else 'not_interested.csv'}"
    )


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────


async def make_exotel_call(session: aiohttp.ClientSession, to_number: str, from_number: str):
    """Make an outbound call using Exotel's Connect API."""
    api_key = os.getenv("EXOTEL_API_KEY")
    api_token = os.getenv("EXOTEL_API_TOKEN")
    sid = os.getenv("EXOTEL_SID")

    if not all([api_key, api_token, sid]):
        raise ValueError("Missing Exotel credentials: EXOTEL_API_KEY, EXOTEL_API_TOKEN, EXOTEL_SID")

    url = f"https://api.exotel.com/v1/Accounts/{sid}/Calls/connect"

    data = {
        "From": from_number,
        "To": to_number,
        "CallerId": from_number,
        "CallType": "trans",
    }

    auth = aiohttp.BasicAuth(api_key, api_token)

    async with session.post(url, data=data, auth=auth) as response:
        if response.status != 200:
            error_text = await response.text()
            raise Exception(f"Exotel API error ({response.status}): {error_text}")

        result_text = await response.text()

        call_sid = "unknown"
        if "<Sid>" in result_text:
            start = result_text.find("<Sid>") + 5
            end = result_text.find("</Sid>")
            if end > start:
                call_sid = result_text[start:end]

        return {"status": "call_initiated", "call_sid": call_sid}


# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session = aiohttp.ClientSession()
    app.state.candidate_names = {}   # call_sid  → name
    app.state.candidate_phones = {}  # call_sid  → phone
    app.state.candidate_roles = {}   # call_sid  → role

    # Ensure output CSVs exist with headers
    _ensure_csv(INTERESTED_CSV)
    _ensure_csv(NOT_INTERESTED_CSV)

    yield
    await app.state.session.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/start")
async def initiate_outbound_call(request: Request) -> JSONResponse:
    """Handle outbound call request and initiate call via Exotel."""
    print("Received outbound call request")

    try:
        data = await request.json()

        if not data.get("dialout_settings"):
            raise HTTPException(
                status_code=400, detail="Missing 'dialout_settings' in the request body"
            )
        if not data["dialout_settings"].get("phone_number"):
            raise HTTPException(
                status_code=400, detail="Missing 'phone_number' in dialout_settings"
            )

        phone_number = str(data["dialout_settings"]["phone_number"])
        candidate_data = data.get("candidate_data", {})
        candidate_name = candidate_data.get("name", "Candidate")
        candidate_role = candidate_data.get("role", "")

        print(f"Processing outbound call to {phone_number} for {candidate_name}")

        try:
            call_result = await make_exotel_call(
                session=request.app.state.session,
                to_number=phone_number,
                from_number=os.getenv("EXOTEL_PHONE_NUMBER"),
            )

            call_sid = call_result.get("call_sid", "unknown")

            # Store candidate metadata keyed by call_sid for later CSV write
            if call_sid != "unknown":
                request.app.state.candidate_names[call_sid] = candidate_name
                request.app.state.candidate_phones[call_sid] = phone_number
                request.app.state.candidate_roles[call_sid] = candidate_role

            # Fallback for cases where call_sid is unknown
            request.app.state.latest_candidate_name = candidate_name
            request.app.state.latest_candidate_phone = phone_number
            request.app.state.latest_candidate_role = candidate_role

        except Exception as e:
            print(f"Error initiating Exotel call: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to initiate call: {str(e)}")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

    return JSONResponse(
        {
            "call_sid": call_sid,
            "status": "call_initiated",
            "phone_number": phone_number,
        }
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle WebSocket connection from Exotel Media Streams."""
    await websocket.accept()
    print("WebSocket connection accepted for outbound call")

    try:
        from bot import bot
        from pipecat.runner.types import WebSocketRunnerArguments

        runner_args = WebSocketRunnerArguments(websocket=websocket)
        runner_args.handle_sigint = False

        candidate_names_map = getattr(websocket.app.state, "candidate_names", {})
        candidate_phones_map = getattr(websocket.app.state, "candidate_phones", {})
        candidate_roles_map = getattr(websocket.app.state, "candidate_roles", {})
        latest_name = getattr(websocket.app.state, "latest_candidate_name", "Candidate")
        latest_phone = getattr(websocket.app.state, "latest_candidate_phone", "")
        latest_role = getattr(websocket.app.state, "latest_candidate_role", "")

        def on_outcome(name: str, outcome: str):
            """
            Callback fired by the bot when candidate interest is detected.
            Looks up phone & role from the shared state maps.
            """
            # Try to resolve phone/role from map; fall back to latest values
            phone = latest_phone
            role = latest_role
            for sid, n in candidate_names_map.items():
                if n == name:
                    phone = candidate_phones_map.get(sid, latest_phone)
                    role = candidate_roles_map.get(sid, latest_role)
                    break

            write_outcome(name=name, phone=phone, role=role, outcome=outcome)

        await bot(
            runner_args,
            candidate_names_map,
            latest_name,
            on_outcome=on_outcome,
        )

    except Exception as e:
        print(f"Error in WebSocket endpoint: {e}")
        await websocket.close()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)