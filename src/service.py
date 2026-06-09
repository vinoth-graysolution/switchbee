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
from typing import Dict, List, Optional

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
from campaign_runner import CampaignRunner, load_campaigns_from_disk
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


def update_candidate_call_outcome(phone: str, outcome: str):
    """
    Increment candidate calls, set lastContact, and set lastintent to the outcome.
    Also update tags if needed (e.g., 'Interested' or 'Not Interested').
    """
    if not CANDIDATES_CSV.exists():
        return
    import datetime
    now_str = datetime.datetime.now().strftime("%b %d, %I:%M %p")
    rows = []
    updated = False
    try:
        with open(CANDIDATES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames) if reader.fieldnames else ["name", "phone", "role", "lang", "tags", "calls", "lastContact", "lastintent"]
            for row in reader:
                db_phone = row.get("phone", "").strip()
                match_phone = phone.strip()
                db_digits = "".join(filter(str.isdigit, db_phone))
                match_digits = "".join(filter(str.isdigit, match_phone))
                if db_phone == match_phone or (db_digits and match_digits and db_digits == match_digits):
                    calls = int(row.get("calls", 0) or 0) + 1
                    row["calls"] = str(calls)
                    row["lastContact"] = now_str
                    outcome_label = "Interested" if outcome == "interested" else "Not Interested"
                    row["lastintent"] = outcome_label
                    
                    # Update tags to preserve only User, New User, Interested, Not Interested
                    tags = [t.strip() for t in row.get("tags", "").split(",") if t.strip()]
                    tags = [t for t in tags if t not in ["Interested", "Not Interested", "interest", "not interest", "User", "New User", "new user", "user"]]
                    tags.append(outcome_label)
                    row["tags"] = ", ".join(tags)
                    updated = True
                rows.append(row)
        if updated:
            with open(CANDIDATES_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"[CSV] Updated candidate {phone} stats in candidates.csv: lastintent={outcome}")
    except Exception as e:
        print(f"[CSV] Error updating candidate {phone}: {e}")



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
    lang: Optional[str] = "English"
    tags: Optional[str] = ""
    calls: Optional[int] = 0
    lastContact: Optional[str] = "—"
    lastintent: Optional[str] = "Unknown"


class BulkCallRequest(BaseModel):
    candidates: List[CandidateIn]


# ─────────────────────────────────────────────────────────────
# Campaign Pydantic models
# ─────────────────────────────────────────────────────────────

class CampaignSettings(BaseModel):
    max_retries: int = 2
    retry_delay_seconds: int = 60
    scheduled_at: Optional[str] = None


class CampaignCreateRequest(BaseModel):
    name: str
    role: str
    settings: CampaignSettings = CampaignSettings()


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



def _upgrade_candidates_csv():
    """Ensure candidates.csv has the full headers and migrate old columns if needed."""
    if not CANDIDATES_CSV.exists():
        return
    rows = []
    try:
        with open(CANDIDATES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and all(h in reader.fieldnames for h in ["lang", "tags", "calls", "lastContact", "lastintent"]):
                return
            for row in reader:
                if row.get("name") or row.get("phone"):
                    rows.append({
                        "name": row.get("name") or "",
                        "phone": row.get("phone") or "",
                        "role": row.get("role") or "",
                        "lang": row.get("lang") or "English",
                        "tags": row.get("tags") or "",
                        "calls": row.get("calls") or "0",
                        "lastContact": row.get("lastContact") or "—",
                        "lastintent": row.get("lastintent") or "Unknown"
                    })
        with open(CANDIDATES_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "phone", "role", "lang", "tags", "calls", "lastContact", "lastintent"])
            writer.writeheader()
            writer.writerows(rows)
        print("[CSV] Upgraded candidates.csv to new 8-column schema.")
    except Exception as e:
        print(f"[CSV] Error upgrading candidates.csv: {e}")


async def _check_scheduled_campaigns(app: FastAPI):
    """Periodically check for scheduled campaigns whose time has come."""
    from datetime import datetime
    while True:
        try:
            await asyncio.sleep(5)
            now_str = datetime.utcnow().isoformat() + "Z"
            for campaign_id in list(app.state.campaigns.keys()):
                runner = app.state.campaigns.get(campaign_id)
                if runner and runner.status == "scheduled" and runner.scheduled_at:
                    if runner.scheduled_at <= now_str:
                        print(f"[Scheduler] Starting scheduled campaign {runner.id} ({runner.name})")
                        runner._pause_event.set()
                        asyncio.create_task(runner.start(app.state))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[Scheduler] Error checking campaigns: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session = aiohttp.ClientSession()
    app.state.candidate_names = {}   # call_sid  → name
    app.state.candidate_phones = {}  # call_sid  → phone
    app.state.candidate_roles = {}   # call_sid  → role
    app.state.jobs = {}              # job_id    → job dict
    app.state.campaigns: Dict[str, CampaignRunner] = load_campaigns_from_disk()

    # Ensure CSVs and schemas are correct on startup
    _upgrade_candidates_csv()
    _ensure_csv(INTERESTED_CSV)
    _ensure_csv(NOT_INTERESTED_CSV)

    # Start background scheduler task
    scheduler_task = asyncio.create_task(_check_scheduled_campaigns(app))

    yield
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
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
            update_candidate_call_outcome(phone=phone, outcome=outcome)

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
        rows = []
        for row in reader:
            if row.get("name") or row.get("phone"):
                rows.append({
                    "name": row.get("name", ""),
                    "phone": row.get("phone", ""),
                    "role": row.get("role", ""),
                    "lang": row.get("lang", "English"),
                    "tags": row.get("tags", ""),
                    "calls": int(row.get("calls", 0) or 0),
                    "lastContact": row.get("lastContact", "—"),
                    "lastintent": row.get("lastintent", "Unknown")
                })
    return JSONResponse(rows)


@app.post("/candidates", status_code=201)
async def add_candidate(candidate: CandidateIn) -> JSONResponse:
    """Append a new candidate to candidates.csv."""
    file_exists = CANDIDATES_CSV.exists()
    with open(CANDIDATES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "phone", "role", "lang", "tags", "calls", "lastContact", "lastintent"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "name": candidate.name,
            "phone": candidate.phone,
            "role": candidate.role,
            "lang": candidate.lang,
            "tags": candidate.tags,
            "calls": candidate.calls,
            "lastContact": candidate.lastContact,
            "lastintent": candidate.lastintent
        })
    return JSONResponse({"added": True, "candidate": candidate.model_dump()})


@app.put("/candidates/{phone}")
async def update_candidate(phone: str, candidate: CandidateIn) -> JSONResponse:
    """Update an existing candidate in candidates.csv."""
    if not CANDIDATES_CSV.exists():
        raise HTTPException(status_code=404, detail="No candidates found")
    
    updated = False
    rows = []
    fieldnames = ["name", "phone", "role", "lang", "tags", "calls", "lastContact", "lastintent"]
    with open(CANDIDATES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = {k: v for k, v in row.items() if k in fieldnames}
            if clean_row.get("phone") == phone:
                clean_row["name"] = candidate.name
                clean_row["role"] = candidate.role
                clean_row["phone"] = candidate.phone
                clean_row["lang"] = candidate.lang
                clean_row["tags"] = candidate.tags
                clean_row["calls"] = str(candidate.calls)
                clean_row["lastContact"] = candidate.lastContact
                clean_row["lastintent"] = candidate.lastintent
                updated = True
            rows.append(clean_row)
            
    if not updated:
        raise HTTPException(status_code=404, detail=f"Candidate with phone {phone} not found")
        
    with open(CANDIDATES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    return JSONResponse({"updated": True, "candidate": candidate.model_dump()})


@app.delete("/candidates/{phone}")
async def delete_candidate(phone: str) -> JSONResponse:
    """Delete a candidate from candidates.csv."""
    if not CANDIDATES_CSV.exists():
        raise HTTPException(status_code=404, detail="No candidates found")
        
    deleted = False
    rows = []
    fieldnames = ["name", "phone", "role", "lang", "tags", "calls", "lastContact", "lastintent"]
    with open(CANDIDATES_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = {k: v for k, v in row.items() if k in fieldnames}
            if clean_row.get("phone") == phone:
                deleted = True
                continue
            rows.append(clean_row)
            
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Candidate with phone {phone} not found")
        
    with open(CANDIDATES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    return JSONResponse({"deleted": True})


# ─────────────────────────────────────────────────────────────
# ── Campaign endpoints
# ─────────────────────────────────────────────────────────────


@app.post("/campaigns", status_code=201)
async def create_campaign(body: CampaignCreateRequest, request: Request) -> JSONResponse:
    """
    Create a new campaign.

    Request body:
        {
            "name": "June Batch",
            "role": "Desktop Support Engineer",
            "settings": { "max_retries": 2, "retry_delay_seconds": 60 }
        }

    Returns the new campaign_id. Upload candidates next via
    POST /campaigns/{campaign_id}/candidates.
    """
    campaign_id = str(uuid.uuid4())
    runner = CampaignRunner(
        campaign_id=campaign_id,
        name=body.name,
        role=body.role,
        max_retries=body.settings.max_retries,
        retry_delay=body.settings.retry_delay_seconds,
        scheduled_at=body.settings.scheduled_at,
    )
    request.app.state.campaigns[campaign_id] = runner
    print(f"[/campaigns] Created campaign {campaign_id}: {body.name!r}")
    return JSONResponse(runner.get_summary(), status_code=201)


@app.post("/campaigns/{campaign_id}/candidates", status_code=200)
async def upload_campaign_candidates(
    campaign_id: str,
    request: Request,
    file: Optional[UploadFile] = File(default=None),
    append: bool = False,
) -> JSONResponse:
    """
    Upload a candidates CSV to a campaign.

    Accepts EITHER:
      • Multipart form upload: field `file` = CSV with columns name, phone, role
      • JSON body:             { "candidates": [{"name":…,"phone":…,"role":…}] }

    Must be called before starting the campaign.
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    if runner.status not in ("created", "scheduled", "paused"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot upload candidates to a campaign with status '{runner.status}'. "
                   "Only created, scheduled, or paused campaigns can be modified.",
        )

    from campaign_runner import _candidates_path
    import csv as _csv

    new_candidates: List[dict] = []

    if file is not None:
        content = await file.read()
        text = content.decode("utf-8")
        reader = _csv.DictReader(io.StringIO(text))
        for row in reader:
            name = row.get("name", "").strip()
            phone = row.get("phone", "").strip()
            role = row.get("role", runner.role).strip() or runner.role
            if name and phone:
                new_candidates.append({"name": name, "phone": phone, "role": role})
    else:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Send a multipart CSV upload (field 'file') or a JSON body with 'candidates' list.",
            )
        for c in body.get("candidates", []):
            name = str(c.get("name", "")).strip()
            phone = str(c.get("phone", "")).strip()
            role = str(c.get("role", runner.role)).strip() or runner.role
            if name and phone:
                new_candidates.append({"name": name, "phone": phone, "role": role})

    if not new_candidates and not append:
        raise HTTPException(status_code=400, detail="No valid candidates found (each row needs name + phone).")

    # Load existing candidates if append is requested
    final_candidates = []
    if append:
        final_candidates = runner.load_candidates()

    seen_phones = {c["phone"].strip() for c in final_candidates}
    for c in new_candidates:
        p_clean = c["phone"].strip()
        if p_clean not in seen_phones:
            final_candidates.append(c)
            seen_phones.add(p_clean)

    if not final_candidates:
        raise HTTPException(status_code=400, detail="No candidates to save.")

    # Write candidates.csv for the campaign
    cand_path = _candidates_path(campaign_id)
    with open(cand_path, "w", newline="", encoding="utf-8") as f:
        writer = _csv.DictWriter(f, fieldnames=["name", "phone", "role"])
        writer.writeheader()
        writer.writerows(final_candidates)

    runner._save_meta()
    print(f"[/campaigns/{campaign_id}/candidates] Saved {len(final_candidates)} total candidates (appended {len(new_candidates)}).")
    return JSONResponse({"campaign_id": campaign_id, "candidates_loaded": len(final_candidates)})


@app.post("/campaigns/{campaign_id}/start")
async def start_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """
    Start dialling candidates for the campaign.
    Candidates must be uploaded first.
    Returns immediately; the calling loop runs in the background.
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    if runner.status not in ("created",):
        raise HTTPException(
            status_code=400,
            detail=f"Campaign is already '{runner.status}'. Only 'created' campaigns can be started.",
        )
    if not runner.load_candidates():
        raise HTTPException(
            status_code=400,
            detail="No candidates found. Upload a candidates CSV first via POST /campaigns/{id}/candidates.",
        )

    asyncio.get_event_loop().create_task(runner.start(request.app.state))
    print(f"[/campaigns/{campaign_id}/start] Campaign started.")
    return JSONResponse({"campaign_id": campaign_id, "status": "running"})


@app.post("/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """
    Pause a running campaign. The current call will finish,
    then dialling will suspend until resumed.
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    if runner.status != "running":
        raise HTTPException(status_code=400, detail=f"Campaign is '{runner.status}', not 'running'.")
    await runner.pause()
    return JSONResponse({"campaign_id": campaign_id, "status": runner.status})


@app.post("/campaigns/{campaign_id}/resume")
async def resume_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """
    Resume a paused campaign.
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    if runner.status != "paused":
        raise HTTPException(status_code=400, detail=f"Campaign is '{runner.status}', not 'paused'.")
    await runner.resume(request.app.state)
    return JSONResponse({"campaign_id": campaign_id, "status": runner.status})


@app.post("/campaigns/{campaign_id}/cancel")
async def cancel_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """
    Cancel a campaign. The current call will finish but no further calls are made.
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    if runner.status in ("done", "cancelled"):
        raise HTTPException(status_code=400, detail=f"Campaign is already '{runner.status}'.")
    await runner.cancel()
    return JSONResponse({"campaign_id": campaign_id, "status": runner.status})


@app.get("/campaigns")
async def list_campaigns(request: Request) -> JSONResponse:
    """
    List all campaigns with summary stats.
    """
    summaries = [runner.get_summary() for runner in request.app.state.campaigns.values()]
    # Sort newest first
    summaries.sort(key=lambda s: s.get("created_at", ""), reverse=True)
    return JSONResponse(summaries)


@app.get("/campaigns/{campaign_id}")
async def get_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """
    Get detailed status and per-call results for a campaign.

    Response includes:
        - Campaign metadata (name, role, status, settings, stats)
        - Full list of per-call results from results.csv
    """
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
    summary = runner.get_summary()
    summary["results"] = runner.get_results()
    return JSONResponse(summary)


@app.put("/campaigns/{campaign_id}")
async def update_campaign(campaign_id: str, body: CampaignCreateRequest, request: Request) -> JSONResponse:
    """Update an existing campaign's settings."""
    runner = request.app.state.campaigns.get(campaign_id)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found.")
        
    runner.name = body.name
    runner.role = body.role
    runner.max_retries = body.settings.max_retries
    runner.retry_delay = body.settings.retry_delay_seconds
    runner._save_meta()
    
    return JSONResponse(runner.get_summary())


@app.delete("/campaigns/{campaign_id}")
async def delete_campaign(campaign_id: str, request: Request) -> JSONResponse:
    """Delete a campaign and its directory from disk."""
    import shutil
    request.app.state.campaigns.pop(campaign_id, None)
    
    from campaign_runner import _campaign_dir
    camp_dir = _campaign_dir(campaign_id)
    if camp_dir.exists() and camp_dir.is_dir():
        shutil.rmtree(camp_dir)
        return JSONResponse({"deleted": True})
        
    return JSONResponse({"deleted": True})


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("service:app", host="0.0.0.0", port=7860, reload=True, app_dir=str(_SRC_DIR))