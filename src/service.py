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

import asyncio
import csv
import io
import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import aiohttp
import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Request, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Add src folder to sys.path so bot.py can be imported successfully
_SRC_DIR = Path(__file__).parent
if str(_SRC_DIR) not in sys.path:
    sys.path.append(str(_SRC_DIR))

from bot import bot
from pipecat.runner.types import WebSocketRunnerArguments

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

    """Make an outbound call using Exotel's Flow Connect API."""
    api_key = os.getenv("EXOTEL_API_KEY")
    api_token = os.getenv("EXOTEL_API_TOKEN")
    sid = os.getenv("EXOTEL_SID")
    subdomain = os.getenv("EXOTEL_SUBDOMAIN", "api.exotel.com")
    app_id = os.getenv("EXOTEL_APP_ID")
 
    if not all([api_key, api_token, sid, app_id]):
        raise ValueError("Missing Exotel credentials or EXOTEL_APP_ID in .env")
 
    domain = "my.exotel.in" if "in.exotel.com" in subdomain else "my.exotel.com"
    url = f"https://{subdomain}/v1/Accounts/{sid}/Calls/connect"
 
    # Dynamically construct the WebSocket URL from OUTBOUND_SERVER_URL
    outbound_url = os.getenv("OUTBOUND_SERVER_URL", "")
    if outbound_url.startswith("http"):
        ws_url = outbound_url.replace("https://", "wss://").replace("http://", "ws://")
        if ws_url.endswith("/start"):
            ws_url = ws_url[:-6] + "/ws"
        elif not ws_url.endswith("/ws"):
            ws_url = ws_url.rstrip("/") + "/ws"
    else:
        ws_url = ""

     # Dial the candidate directly, and route them to the Flow on answer
    data = {
        "From": to_number,
        "CallerId": from_number,
        "CallType": "trans",
        "Url": f"http://{domain}/exoml/start/{app_id}",
        "CustomField": ws_url,
    }
 
    print(f"[Exotel API] Initiating call to {to_number} using Flow {app_id} (URL: {data['Url']})")
    print(f"[Exotel API] Initiating call to {to_number} using Flow {app_id} (URL: {data['Url']}, WS: {ws_url})")

    # print(f"[Exotel API] Initiating call to {to_number} using Flow {app_id} (URL: {data['Url']})")

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


# ─────────────────────────────────────────────────────────────
# Pydantic models for new API endpoints
# ─────────────────────────────────────────────────────────────

class CandidateIn(BaseModel):
    name: str
    phone: str
    role: Optional[str] = ""


class BulkCallRequest(BaseModel):
    candidates: List[CandidateIn]


# ─────────────────────────────────────────────────────────────
# Exotel call status polling (async version of auto_call.py)
# ─────────────────────────────────────────────────────────────

CALL_DETAILS_URL = (
    "https://{subdomain}/v1/Accounts/{sid}/Calls/{call_sid}"
)


async def wait_until_call_completed_async(call_sid: str) -> str:
    """
    Async version of wait_until_call_completed from auto_call.py.
    Polls Exotel every 10 s until the call reaches a terminal state.
    Returns the final call status string.
    """
    api_key = os.getenv("EXOTEL_API_KEY")
    api_token = os.getenv("EXOTEL_API_TOKEN")
    sid = os.getenv("EXOTEL_SID")
    subdomain = os.getenv("EXOTEL_SUBDOMAIN", "api.exotel.com")
    url = CALL_DETAILS_URL.format(subdomain=subdomain, sid=sid, call_sid=call_sid)

    import xml.etree.ElementTree as ET

    error_count = 0
    while True:
        try:
            # Run the blocking requests.get in a thread so we don't block the event loop
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(url, auth=(api_key, api_token), timeout=15),
            )
            error_count = 0
            root = ET.fromstring(response.text)
            status_elem = root.find(".//Status")
            if status_elem is None or not status_elem.text:
                raise ValueError("Status tag missing in Exotel response")
            call_status = status_elem.text.strip().lower()
            print(f"[Poll] {call_sid} → {call_status}")
            if call_status in {"completed", "busy", "failed", "no-answer", "canceled"}:
                return call_status
        except Exception as exc:
            error_count += 1
            print(f"[Poll] Error ({error_count}/10): {exc}")
            if error_count >= 10:
                return "failed"
        await asyncio.sleep(10)


# ─────────────────────────────────────────────────────────────
# Sequential bulk-call background runner
# ─────────────────────────────────────────────────────────────


async def _run_sequential_calls(job_id: str, candidates: List[dict], app_state):
    """
    Calls each candidate ONE BY ONE — waits for each call to complete
    before dialling the next, matching the original auto_call.py behaviour.
    """
    job = app_state.jobs[job_id]
    job["status"] = "running"

    from_number = os.getenv("EXOTEL_PHONE_NUMBER")

    for candidate in candidates:
        name = candidate["name"]
        phone = candidate["phone"]
        role = candidate.get("role", "")

        print(f"\n[BulkCall] Calling {name} → {phone}")
        result_entry = {"name": name, "phone": phone, "role": role, "call_sid": None, "final_status": "pending"}
        job["results"].append(result_entry)

        try:
            async with app_state.session.request.__self__.__class__() as _tmp:
                pass
        except Exception:
            pass

        try:
            call_result = await make_exotel_call(
                session=app_state.session,
                to_number=phone,
                from_number=from_number,
            )
            call_sid = call_result.get("call_sid", "unknown")
            result_entry["call_sid"] = call_sid

            # Store metadata so the WS bot can pick up the candidate name
            if call_sid != "unknown":
                app_state.candidate_names[call_sid] = name
                app_state.candidate_phones[call_sid] = phone
                app_state.candidate_roles[call_sid] = role
            app_state.latest_candidate_name = name
            app_state.latest_candidate_phone = phone
            app_state.latest_candidate_role = role

            # ── WAIT for this call to finish before dialling next ──
            final_status = await wait_until_call_completed_async(call_sid)
            result_entry["final_status"] = final_status
            job["completed"] += 1

            if final_status != "completed":
                print(f"[BulkCall] {name} not answered ({final_status}) — logging to unanswered.csv")
                _write_unanswered(name, phone, role, final_status)

        except Exception as exc:
            print(f"[BulkCall] Error calling {name}: {exc}")
            result_entry["final_status"] = "error"
            result_entry["error"] = str(exc)
            job["completed"] += 1
            _write_unanswered(name, phone, role, "failed")

    job["status"] = "done"
    print(f"[BulkCall] Job {job_id} finished — {job['completed']}/{job['total']} calls processed.")


def _write_unanswered(name: str, phone: str, role: str, status: str):
    """Mirror of auto_call.py's write_unanswered — writes to src/unanswered.csv."""
    unanswered_csv = _SRC_DIR / "unanswered.csv"
    file_exists = unanswered_csv.exists()
    with open(unanswered_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["name", "phone", "role", "status"])
        writer.writerow([name, phone, role, status])


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session = aiohttp.ClientSession()
    app.state.candidate_names = {}   # call_sid  → name
    app.state.candidate_phones = {}  # call_sid  → phone
    app.state.candidate_roles = {}   # call_sid  → role
    app.state.jobs = {}              # job_id    → job dict

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
        runner_args = WebSocketRunnerArguments(websocket=websocket)
        runner_args.handle_sigint = False

        candidate_names_map = getattr(websocket.app.state, "candidate_names", {})
        latest_name = getattr(websocket.app.state, "latest_candidate_name", "Candidate")

        def on_outcome(name: str, phone: str, role: str, outcome: str):
            """
            Callback fired by the bot when candidate interest is detected.
            """
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
# ── Single call endpoint
# ─────────────────────────────────────────────────────────────


@app.post("/call")
async def trigger_single_call(candidate: CandidateIn, request: Request) -> JSONResponse:
    """
    Trigger an outbound AI call to a single candidate immediately.

    Request body:
        { "name": "Alice", "phone": "+919876543210", "role": "developer" }
    """
    print(f"[/call] Dialling {candidate.name} → {candidate.phone}")
    try:
        call_result = await make_exotel_call(
            session=request.app.state.session,
            to_number=candidate.phone,
            from_number=os.getenv("EXOTEL_PHONE_NUMBER"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    call_sid = call_result.get("call_sid", "unknown")
    if call_sid != "unknown":
        request.app.state.candidate_names[call_sid] = candidate.name
        request.app.state.candidate_phones[call_sid] = candidate.phone
        request.app.state.candidate_roles[call_sid] = candidate.role
    request.app.state.latest_candidate_name = candidate.name
    request.app.state.latest_candidate_phone = candidate.phone
    request.app.state.latest_candidate_role = candidate.role

    return JSONResponse({"call_sid": call_sid, "status": "call_initiated", "phone": candidate.phone})


# ─────────────────────────────────────────────────────────────
# ── Bulk call endpoint  (sequential — one call at a time)
# ─────────────────────────────────────────────────────────────


@app.post("/bulk-call")
async def trigger_bulk_calls(
    request: Request,
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(default=None),
) -> JSONResponse:
    """
    Trigger outbound AI calls for multiple candidates, one by one.

    Accepts EITHER:
      • Multipart form upload: field `file` = CSV with columns name,phone,role
      • JSON body:             { "candidates": [{"name":…,"phone":…,"role":…}] }

    Returns immediately with a job_id.  Poll GET /jobs/{job_id} for progress.
    Calls are made SEQUENTIALLY — each call must finish before the next starts.
    """
    candidates: List[dict] = []

    # ── Parse input ──────────────────────────────────────────
    if file is not None:
        # CSV upload
        content = await file.read()
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            name = row.get("name", "").strip()
            phone = row.get("phone", "").strip()
            role = row.get("role", "").strip()
            if name and phone:
                candidates.append({"name": name, "phone": phone, "role": role})
    else:
        # JSON body
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Send JSON body with 'candidates' list or upload a CSV file.")

        raw_list = body.get("candidates", [])
        if not raw_list:
            raise HTTPException(status_code=400, detail="'candidates' list is empty or missing.")
        for c in raw_list:
            name = str(c.get("name", "")).strip()
            phone = str(c.get("phone", "")).strip()
            role = str(c.get("role", "")).strip()
            if name and phone:
                candidates.append({"name": name, "phone": phone, "role": role})

    if not candidates:
        raise HTTPException(status_code=400, detail="No valid candidates found (each row needs at least name + phone).")

    # ── Create job record ─────────────────────────────────────
    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "total": len(candidates),
        "completed": 0,
        "results": [],
    }
    request.app.state.jobs[job_id] = job

    # ── Launch sequential background task ─────────────────────
    asyncio.get_event_loop().create_task(
        _run_sequential_calls(job_id, candidates, request.app.state)
    )

    print(f"[/bulk-call] Job {job_id} queued — {len(candidates)} candidates (sequential)")
    return JSONResponse({"job_id": job_id, "status": "queued", "total": len(candidates)})


# ─────────────────────────────────────────────────────────────
# ── Job status endpoint
# ─────────────────────────────────────────────────────────────


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str, request: Request) -> JSONResponse:
    """
    Poll the progress of a bulk-call job.

    Response:
        { "job_id": "…", "status": "running"|"done"|"queued",
          "total": 3, "completed": 1, "results": […] }
    """
    job = request.app.state.jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return JSONResponse(job)


# ─────────────────────────────────────────────────────────────
# ── Candidates CSV endpoints
# ─────────────────────────────────────────────────────────────

CANDIDATES_CSV = _SRC_DIR / "candidates.csv"


@app.get("/candidates")
async def list_candidates() -> JSONResponse:
    """Return all candidates from candidates.csv as JSON."""
    if not CANDIDATES_CSV.exists():
        return JSONResponse([])
    with open(CANDIDATES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if row.get("name") or row.get("phone")]
    return JSONResponse(rows)


@app.post("/candidates", status_code=201)
async def add_candidate(candidate: CandidateIn) -> JSONResponse:
    """Append a new candidate to candidates.csv."""
    file_exists = CANDIDATES_CSV.exists()
    with open(CANDIDATES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "phone", "role"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({"name": candidate.name, "phone": candidate.phone, "role": candidate.role})
    return JSONResponse({"added": True, "candidate": candidate.model_dump()})


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)