"""campaign_runner.py

Manages the full lifecycle of a Campaign — a named, organised bulk-call drive.

Each campaign:
  - Has its own subdirectory under src/campaigns/<campaign_id>/
  - Stores metadata in meta.json (name, role, status, settings, stats)
  - Stores the original candidate list in candidates.csv
  - Writes per-call results to results.csv as calls complete

Lifecycle:
  created  → (upload candidates) → running → paused → running → done
                                             ↘ cancelled
"""

import asyncio
import csv
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Literal, Optional

# ─────────────────────────────────────────────────────────────
# Directory layout
# ─────────────────────────────────────────────────────────────

_SRC_DIR = Path(__file__).parent
CAMPAIGNS_DIR = _SRC_DIR / "campaigns"

CampaignStatus = Literal["created", "scheduled", "running", "paused", "cancelled", "done"]

RESULTS_FIELDNAMES = ["name", "phone", "role", "attempt", "call_sid", "final_status"]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _campaign_dir(campaign_id: str) -> Path:
    return CAMPAIGNS_DIR / campaign_id


def _meta_path(campaign_id: str) -> Path:
    return _campaign_dir(campaign_id) / "meta.json"


def _candidates_path(campaign_id: str) -> Path:
    return _campaign_dir(campaign_id) / "candidates.csv"


def _results_path(campaign_id: str) -> Path:
    return _campaign_dir(campaign_id) / "results.csv"


def _ensure_results_csv(campaign_id: str):
    path = _results_path(campaign_id)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDNAMES)
            writer.writeheader()


def _append_result_row(campaign_id: str, row: dict):
    path = _results_path(campaign_id)
    _ensure_results_csv(campaign_id)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDNAMES)
        writer.writerow({k: row.get(k, "") for k in RESULTS_FIELDNAMES})


# ─────────────────────────────────────────────────────────────
# CampaignRunner
# ─────────────────────────────────────────────────────────────

class CampaignRunner:
    """
    Encapsulates a single campaign's state and call-execution logic.

    Args:
        campaign_id:    Unique identifier (UUID).
        name:           Human-readable campaign name.
        role:           Job role for this campaign.
        max_retries:    Number of times to retry unanswered / failed calls.
        retry_delay:    Seconds to wait between retry rounds.
    """

    def __init__(
        self,
        campaign_id: str,
        name: str,
        role: str,
        max_retries: int = 2,
        retry_delay: int = 60,
        scheduled_at: Optional[str] = None,
    ):
        self.id = campaign_id
        self.name = name
        self.role = role
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.scheduled_at = scheduled_at

        self.status: CampaignStatus = "scheduled" if scheduled_at else "created"
        self.created_at: str = _now_iso()
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None

        # In-memory result tracking
        self.results: List[dict] = []

        # Pause / cancel control
        self._pause_event = asyncio.Event()
        self._pause_event.set()   # not paused by default
        self._cancelled = False

        # Ensure campaign directory exists
        _campaign_dir(self.id).mkdir(parents=True, exist_ok=True)
        self._save_meta()

    # ── Persistence ──────────────────────────────────────────

    def _save_meta(self):
        """Write current state to meta.json."""
        meta = {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "settings": {
                "max_retries": self.max_retries,
                "retry_delay": self.retry_delay,
                "scheduled_at": self.scheduled_at,
            },
            "stats": self._compute_stats(),
        }
        with open(_meta_path(self.id), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    # ── Stats helpers ────────────────────────────────────────

    def _compute_stats(self) -> dict:
        by_phone = {}
        for r in self.results:
            p = r.get("phone")
            if not p:
                continue
            if p not in by_phone:
                by_phone[p] = []
            by_phone[p].append(r)

        attempted = len(by_phone)
        completed = 0
        unanswered = 0
        failed = 0

        for p, attempts in by_phone.items():
            if any(att.get("final_status") == "completed" for att in attempts):
                completed += 1
            else:
                latest = max(attempts, key=lambda x: int(x.get("attempt") or 1))
                stat = latest.get("final_status")
                if stat in ("no-answer", "busy"):
                    unanswered += 1
                else:
                    failed += 1

        return {
            "total_candidates": self._candidate_count(),
            "total_call_attempts": attempted,
            "completed": completed,
            "unanswered": unanswered,
            "failed": failed,
        }

    def _candidate_count(self) -> int:
        path = _candidates_path(self.id)
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for row in csv.DictReader(f) if row.get("name") or row.get("phone"))

    def get_summary(self) -> dict:
        """Return a full JSON-serialisable summary of this campaign."""
        meta_path = _meta_path(self.id)
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"id": self.id, "status": self.status}

    def get_results(self) -> List[dict]:
        """Return all per-call result rows from results.csv."""
        path = _results_path(self.id)
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    # ── Candidate loading ────────────────────────────────────

    def load_candidates(self) -> List[dict]:
        """Read candidates.csv and return as a list of dicts."""
        path = _candidates_path(self.id)
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [
                {
                    "name": row.get("name", "").strip(),
                    "phone": row.get("phone", "").strip(),
                    "role": row.get("role", self.role).strip() or self.role,
                }
                for row in reader
                if (row.get("name") or "").strip() and (row.get("phone") or "").strip()
            ]

    # ── Lifecycle controls ────────────────────────────────────

    async def pause(self):
        if self.status == "running":
            self.status = "paused"
            self._pause_event.clear()
            self._save_meta()
            print(f"[Campaign {self.id}] Paused.")

    async def resume(self, app_state):
        if self.status == "paused":
            self.status = "running"
            self._pause_event.set()
            self._save_meta()
            print(f"[Campaign {self.id}] Resumed.")

    async def cancel(self):
        self._cancelled = True
        self._pause_event.set()   # unblock if paused so the loop can exit
        self.status = "cancelled"
        self.finished_at = _now_iso()
        self._save_meta()
        print(f"[Campaign {self.id}] Cancelled.")

    # ── Main call loop ───────────────────────────────────────

    async def start(self, app_state):
        """
        Begin dialling candidates sequentially.
        Automatically retries unanswered / failed calls up to max_retries times.
        """
        from service import make_exotel_call, wait_until_call_completed_async
        import os

        self.status = "running"
        self.started_at = _now_iso()
        self._save_meta()
        _ensure_results_csv(self.id)

        candidates = self.load_candidates()
        if not candidates:
            print(f"[Campaign {self.id}] No candidates found. Marking done.")
            self.status = "done"
            self.finished_at = _now_iso()
            self._save_meta()
            return

        from_number = os.getenv("EXOTEL_PHONE_NUMBER", "")

        # ── Retry rounds: attempt 0 = first pass, 1..N = retries ──
        to_dial = candidates[:]
        attempt = 0

        while to_dial and not self._cancelled:
            print(f"[Campaign {self.id}] Round {attempt + 1} — {len(to_dial)} candidate(s) to dial.")
            retry_queue: List[dict] = []

            for candidate in to_dial:
                # ── Check cancel ───────────────────────────────
                if self._cancelled:
                    break

                # ── Check pause (blocks here until resumed) ───
                await self._pause_event.wait()
                if self._cancelled:
                    break

                name = candidate["name"]
                phone = candidate["phone"]
                role = candidate["role"]

                print(f"[Campaign {self.id}] Calling {name} → {phone} (attempt {attempt + 1})")

                call_sid = "unknown"
                final_status = "error"

                try:
                    call_result = await make_exotel_call(
                        session=app_state.session,
                        to_number=phone,
                        from_number=from_number,
                    )
                    call_sid = call_result.get("call_sid", "unknown")

                    # Register candidate metadata so the WS bot can pick it up
                    if call_sid != "unknown":
                        app_state.candidate_names[call_sid] = name
                        app_state.candidate_phones[call_sid] = phone
                        app_state.candidate_roles[call_sid] = role
                    app_state.latest_candidate_name = name
                    app_state.latest_candidate_phone = phone
                    app_state.latest_candidate_role = role

                    final_status = await wait_until_call_completed_async(call_sid)

                except Exception as exc:
                    print(f"[Campaign {self.id}] Error calling {name}: {exc}")
                    final_status = "error"

                # ── Record result ──────────────────────────────
                row = {
                    "name": name,
                    "phone": phone,
                    "role": role,
                    "attempt": attempt + 1,
                    "call_sid": call_sid,
                    "final_status": final_status,
                }
                self.results.append(row)
                _append_result_row(self.id, row)
                self._save_meta()

                # ── Queue for retry if unanswered / failed ─────
                if final_status in ("no-answer", "busy", "failed", "error") and attempt < self.max_retries:
                    retry_queue.append(candidate)

            # ── Prepare next retry round ───────────────────────
            attempt += 1
            if retry_queue and attempt <= self.max_retries and not self._cancelled:
                print(
                    f"[Campaign {self.id}] Waiting {self.retry_delay}s before retry "
                    f"round {attempt + 1} ({len(retry_queue)} candidates)."
                )
                # Sleep in small chunks so pause/cancel is still responsive
                elapsed = 0
                while elapsed < self.retry_delay and not self._cancelled:
                    await asyncio.sleep(min(5, self.retry_delay - elapsed))
                    elapsed += 5
                    await self._pause_event.wait()   # respect pause during delay
                to_dial = retry_queue
            else:
                break

        if not self._cancelled:
            self.status = "done"
            self.finished_at = _now_iso()
            self._save_meta()
            print(f"[Campaign {self.id}] Done — {len(self.results)} total call attempts.")


# ─────────────────────────────────────────────────────────────
# Factory — restore campaigns from disk on server startup
# ─────────────────────────────────────────────────────────────

def load_campaigns_from_disk() -> Dict[str, CampaignRunner]:
    """
    Re-hydrate all campaigns from meta.json files on disk.
    Called at server startup so campaigns survive restarts.
    Campaigns that were 'running' or 'paused' when the server stopped
    are set back to 'paused' (safe, requires manual resume).
    """
    runners: Dict[str, CampaignRunner] = {}
    if not CAMPAIGNS_DIR.exists():
        return runners

    for entry in CAMPAIGNS_DIR.iterdir():
        if not entry.is_dir():
            continue
        meta_file = entry / "meta.json"
        if not meta_file.exists():
            continue
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)

            settings = meta.get("settings", {})
            runner = CampaignRunner(
                campaign_id=meta["id"],
                name=meta["name"],
                role=meta["role"],
                max_retries=settings.get("max_retries", 2),
                retry_delay=settings.get("retry_delay", 60),
                scheduled_at=settings.get("scheduled_at"),
            )
            runner.created_at = meta.get("created_at", runner.created_at)
            runner.started_at = meta.get("started_at")
            runner.finished_at = meta.get("finished_at")

            disk_status = meta.get("status", "created")
            # Mark mid-flight campaigns as paused — require explicit resume
            if disk_status in ("running",):
                runner.status = "paused"
                runner._pause_event.clear()
            else:
                runner.status = disk_status
                if disk_status == "paused" or disk_status == "scheduled":
                    runner._pause_event.clear()

            # Re-load in-memory results from results.csv
            runner.results = runner.get_results()

            runner._save_meta()
            runners[runner.id] = runner
            print(f"[Startup] Restored campaign {runner.id} ({runner.name}) → {runner.status}")
        except Exception as exc:
            print(f"[Startup] Failed to restore campaign from {entry}: {exc}")

    return runners
