"""
Cron job storage and management.

Jobs are stored in ~/.hermes/cron/jobs.json
Output is saved to ~/.hermes/cron/output/{job_id}/{timestamp}.md
"""

import copy
import errno
import json
import logging
import tempfile
import threading
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import Optional, Dict, List, Any, Union, Tuple

logger = logging.getLogger(__name__)

from hermes_time import now as _hermes_now
from utils import atomic_replace

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

# =============================================================================
# Configuration
# =============================================================================

HERMES_DIR = get_hermes_home().resolve()
CRON_DIR = HERMES_DIR / "cron"
JOBS_FILE = CRON_DIR / "jobs.json"

# In-process lock protecting load_jobs→modify→save_jobs cycles.
# Required when tick() runs jobs in parallel threads — without this,
# concurrent mark_job_run / advance_next_run calls can clobber each other.
_jobs_file_lock = threading.Lock()
OUTPUT_DIR = CRON_DIR / "output"
ONESHOT_GRACE_SECONDS = 120
IN_FLIGHT_TIMEOUT_GRACE_SECONDS = 5


def _normalize_skill_list(skill: Optional[str] = None, skills: Optional[Any] = None) -> List[str]:
    """Normalize legacy/single-skill and multi-skill inputs into a unique ordered list."""
    if skills is None:
        raw_items = [skill] if skill else []
    elif isinstance(skills, str):
        raw_items = [skills]
    else:
        raw_items = list(skills)

    normalized: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _apply_skill_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return a job dict with canonical `skills` and legacy `skill` fields aligned."""
    normalized = dict(job)
    skills = _normalize_skill_list(normalized.get("skill"), normalized.get("skills"))
    normalized["skills"] = skills
    normalized["skill"] = skills[0] if skills else None
    return normalized


def _secure_dir(path: Path):
    """Set directory to owner-only access (0700). No-op on Windows."""
    try:
        os.chmod(path, 0o700)
    except (OSError, NotImplementedError):
        pass  # Windows or other platforms where chmod is not supported


def _secure_file(path: Path):
    """Set file to owner-only read/write (0600). No-op on Windows."""
    try:
        if path.exists():
            os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


def ensure_dirs():
    """Ensure cron directories exist with secure permissions."""
    CRON_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _secure_dir(CRON_DIR)
    _secure_dir(OUTPUT_DIR)


# =============================================================================
# Schedule Parsing
# =============================================================================

def parse_duration(s: str) -> int:
    """
    Parse duration string into minutes.
    
    Examples:
        "30m" → 30
        "2h" → 120
        "1d" → 1440
    """
    s = s.strip().lower()
    match = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$', s)
    if not match:
        raise ValueError(f"Invalid duration: '{s}'. Use format like '30m', '2h', or '1d'")
    
    value = int(match.group(1))
    unit = match.group(2)[0]  # First char: m, h, or d
    
    multipliers = {'m': 1, 'h': 60, 'd': 1440}
    return value * multipliers[unit]


def parse_schedule(schedule: str) -> Dict[str, Any]:
    """
    Parse schedule string into structured format.
    
    Returns dict with:
        - kind: "once" | "interval" | "cron"
        - For "once": "run_at" (ISO timestamp)
        - For "interval": "minutes" (int)
        - For "cron": "expr" (cron expression)
    
    Examples:
        "30m"              → once in 30 minutes
        "2h"               → once in 2 hours
        "every 30m"        → recurring every 30 minutes
        "every 2h"         → recurring every 2 hours
        "0 9 * * *"        → cron expression
        "2026-02-03T14:00" → once at timestamp
    """
    schedule = schedule.strip()
    original = schedule
    schedule_lower = schedule.lower()
    
    # "every X" pattern → recurring interval
    if schedule_lower.startswith("every "):
        duration_str = schedule[6:].strip()
        minutes = parse_duration(duration_str)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m"
        }
    
    # Check for cron expression (5 or 6 space-separated fields)
    # Cron fields: minute hour day month weekday [year]
    parts = schedule.split()
    if len(parts) >= 5 and all(
        re.match(r'^[\d\*\-,/]+$', p) for p in parts[:5]
    ):
        if not HAS_CRONITER:
            raise ValueError("Cron expressions require 'croniter' package. Install with: pip install croniter")
        # Validate cron expression
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{schedule}': {e}")
        return {
            "kind": "cron",
            "expr": schedule,
            "display": schedule
        }
    
    # ISO timestamp (contains T or looks like date)
    if 'T' in schedule or re.match(r'^\d{4}-\d{2}-\d{2}', schedule):
        try:
            # Parse and validate
            dt = datetime.fromisoformat(schedule.replace('Z', '+00:00'))
            # Make naive timestamps timezone-aware at parse time so the stored
            # value doesn't depend on the system timezone matching at check time.
            if dt.tzinfo is None:
                dt = dt.astimezone()  # Interpret as local timezone
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}"
            }
        except ValueError as e:
            raise ValueError(f"Invalid timestamp '{schedule}': {e}")
    
    # Duration like "30m", "2h", "1d" → one-shot from now
    try:
        minutes = parse_duration(schedule)
        run_at = _hermes_now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {original}"
        }
    except ValueError:
        pass
    
    raise ValueError(
        f"Invalid schedule '{original}'. Use:\n"
        f"  - Duration: '30m', '2h', '1d' (one-shot)\n"
        f"  - Interval: 'every 30m', 'every 2h' (recurring)\n"
        f"  - Cron: '0 9 * * *' (cron expression)\n"
        f"  - Timestamp: '2026-02-03T14:00:00' (one-shot at time)"
    )


def _ensure_aware(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in Hermes configured timezone.

    Backward compatibility:
    - Older stored timestamps may be naive.
    - Naive values are interpreted as *system-local wall time* (the timezone
      `datetime.now()` used when they were created), then converted to the
      configured Hermes timezone.

    This preserves relative ordering for legacy naive timestamps across
    timezone changes and avoids false not-due results.
    """
    target_tz = _hermes_now().tzinfo
    if dt.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        return dt.replace(tzinfo=local_tz).astimezone(target_tz)
    return dt.astimezone(target_tz)


def _recoverable_oneshot_run_at(
    schedule: Dict[str, Any],
    now: datetime,
    *,
    last_run_at: Optional[str] = None,
) -> Optional[str]:
    """Return a one-shot run time if it is still eligible to fire.

    One-shot jobs get a small grace window so jobs created a few seconds after
    their requested minute still run on the next tick. Once a one-shot has
    already run, it is never eligible again.
    """
    if schedule.get("kind") != "once":
        return None
    if last_run_at:
        return None

    run_at = schedule.get("run_at")
    if not run_at:
        return None

    run_at_dt = _ensure_aware(datetime.fromisoformat(run_at))
    if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
        return run_at
    return None


def _compute_grace_seconds(schedule: dict) -> int:
    """Compute how late a job can be and still catch up instead of fast-forwarding.

    Uses half the schedule period, clamped between 120 seconds and 2 hours.
    This ensures daily jobs can catch up if missed by up to 2 hours,
    while frequent jobs (every 5-10 min) still fast-forward quickly.
    """
    MIN_GRACE = 120
    MAX_GRACE = 7200  # 2 hours

    kind = schedule.get("kind")

    if kind == "interval":
        period_seconds = schedule.get("minutes", 1) * 60
        grace = period_seconds // 2
        return max(MIN_GRACE, min(grace, MAX_GRACE))

    if kind == "cron" and HAS_CRONITER:
        try:
            now = _hermes_now()
            cron = croniter(schedule["expr"], now)
            first = cron.get_next(datetime)
            second = cron.get_next(datetime)
            period_seconds = int((second - first).total_seconds())
            grace = period_seconds // 2
            return max(MIN_GRACE, min(grace, MAX_GRACE))
        except Exception:
            pass

    return MIN_GRACE


def compute_next_run(schedule: Dict[str, Any], last_run_at: Optional[str] = None) -> Optional[str]:
    """
    Compute the next run time for a schedule.

    Returns ISO timestamp string, or None if no more runs.
    """
    now = _hermes_now()

    if schedule["kind"] == "once":
        return _recoverable_oneshot_run_at(schedule, now, last_run_at=last_run_at)

    elif schedule["kind"] == "interval":
        minutes = schedule["minutes"]
        if last_run_at:
            # Next run is last_run + interval
            last = _ensure_aware(datetime.fromisoformat(last_run_at))
            next_run = last + timedelta(minutes=minutes)
        else:
            # First run is now + interval
            next_run = now + timedelta(minutes=minutes)
        return next_run.isoformat()

    elif schedule["kind"] == "cron":
        if not HAS_CRONITER:
            logger.warning(
                "Cannot compute next run for cron schedule %r: 'croniter' is "
                "not installed. croniter is a core dependency as of v0.9.x; "
                "reinstall hermes-agent or run 'pip install croniter' in your "
                "runtime env.",
                schedule.get("expr"),
            )
            return None
        # Use last_run_at as the croniter base when available, consistent
        # with interval jobs.  This ensures that after a crash/restart,
        # the next run is anchored to the actual last execution time
        # rather than to an arbitrary restart time.
        base_time = now
        if last_run_at:
            base_time = _ensure_aware(datetime.fromisoformat(last_run_at))
        cron = croniter(schedule["expr"], base_time)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()

    return None


def _cron_timeout_seconds() -> float:
    """Return configured cron timeout in seconds, clamped to a sane minimum."""
    try:
        timeout = float(os.getenv("HERMES_CRON_TIMEOUT", 600))
    except (TypeError, ValueError):
        timeout = 600.0
    return max(timeout, 1.0)


def _orphan_recovery_grace_seconds() -> float:
    """Return the orphan-recovery grace window in seconds."""
    try:
        value = float(os.getenv("HERMES_CRON_ORPHAN_GRACE_SECONDS", 60))
    except (TypeError, ValueError):
        value = 60.0
    return max(value, 0.0)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return _ensure_aware(datetime.fromisoformat(value))
    except Exception:
        return None


def _parse_legacy_owner_pid(owner_instance_id: Optional[str]) -> Optional[int]:
    if not owner_instance_id:
        return None
    match = re.match(r"^(\d+)-", str(owner_instance_id).strip())
    if not match:
        return None
    try:
        pid = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _linux_boot_id() -> Optional[str]:
    if not sys.platform.startswith("linux"):
        return None
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    return value or None


def _darwin_boot_fingerprint() -> Optional[str]:
    if sys.platform != "darwin":
        return None
    try:
        result = subprocess.run(
            ["sysctl", "-n", "kern.boottime"],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None

    output = (result.stdout or "").strip()
    match = re.search(r"sec\s*=\s*(\d+)\s*,\s*usec\s*=\s*(\d+)", output)
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    compact = re.sub(r"\s+", " ", output)
    return compact or None


def _boot_fingerprint() -> Optional[str]:
    if sys.platform.startswith("linux"):
        return _linux_boot_id()
    if sys.platform == "darwin":
        return _darwin_boot_fingerprint()
    return None


def _linux_process_state(pid: int) -> Optional[str]:
    if not sys.platform.startswith("linux") or pid <= 0:
        return None
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None

    end_idx = stat_text.rfind(")")
    if end_idx == -1 or len(stat_text) <= end_idx + 2:
        return None
    state = stat_text[end_idx + 2 : end_idx + 3]
    return state or None


def _linux_process_start_fingerprint(pid: int) -> Optional[str]:
    if not sys.platform.startswith("linux") or pid <= 0:
        return None
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None

    end_idx = stat_text.rfind(")")
    if end_idx == -1:
        return None

    fields = stat_text[end_idx + 2 :].split()
    if len(fields) <= 19:
        return None
    return fields[19] or None


def _darwin_process_start_fingerprint(pid: int) -> Optional[str]:
    if sys.platform != "darwin" or pid <= 0:
        return None
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None

    output = re.sub(r"\s+", " ", (result.stdout or "").strip())
    return output or None


def _process_start_fingerprint(pid: int) -> Optional[str]:
    if sys.platform.startswith("linux"):
        return _linux_process_start_fingerprint(pid)
    if sys.platform == "darwin":
        return _darwin_process_start_fingerprint(pid)
    return None


def _pid_is_alive(pid: int) -> Optional[bool]:
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        alive = True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            alive = True
        else:
            return None
    else:
        alive = True

    if alive and sys.platform.startswith("linux"):
        state = _linux_process_state(pid)
        if state in {"Z", "X", "x"}:
            return False
    return alive


def _process_identity_matches(
    pid: int,
    *,
    boot_id: Optional[str],
    process_start: Optional[str],
) -> Optional[bool]:
    checked = False

    if boot_id:
        current_boot_id = _boot_fingerprint()
        if not current_boot_id:
            return None
        checked = True
        if current_boot_id != boot_id:
            return False

    if process_start:
        current_process_start = _process_start_fingerprint(pid)
        if not current_process_start:
            return None
        checked = True
        if current_process_start != process_start:
            return False

    if checked and process_start:
        return True
    return None


def _legacy_owner_pid_is_dead(pid: int) -> bool:
    return _pid_is_alive(pid) is False


def _get_inflight_owner_state(
    in_flight: Dict[str, Any],
    now_dt: Optional[datetime] = None,
) -> Tuple[str, str]:
    del now_dt

    owner_pid_raw = in_flight.get("owner_pid")
    try:
        owner_pid = int(owner_pid_raw) if owner_pid_raw is not None else None
    except (TypeError, ValueError):
        owner_pid = None

    if owner_pid and owner_pid > 0:
        alive = _pid_is_alive(owner_pid)
        if alive is False:
            return "dead", f"owner pid {owner_pid} not alive"
        if alive is True:
            identity = _process_identity_matches(
                owner_pid,
                boot_id=in_flight.get("owner_boot_id"),
                process_start=in_flight.get("owner_process_start"),
            )
            if identity is True:
                return "alive", f"owner pid {owner_pid} alive and fingerprint matches"
            if identity is False:
                return "mismatch", f"owner pid {owner_pid} fingerprint mismatch"
            return "unknown", f"owner pid {owner_pid} alive but identity could not be confirmed"
        return "unknown", f"owner pid {owner_pid} liveness unavailable"

    legacy_pid = _parse_legacy_owner_pid(in_flight.get("owner_instance_id"))
    if legacy_pid is not None:
        if _legacy_owner_pid_is_dead(legacy_pid):
            return "dead", f"legacy owner pid {legacy_pid} not alive"
        return "unknown", f"legacy owner pid {legacy_pid} could not be verified"

    return "unknown", "owner metadata missing or unsupported"


def _find_job_index(jobs: List[Dict[str, Any]], job_id: str) -> Optional[int]:
    for i, job in enumerate(jobs):
        if job.get("id") == job_id:
            return i
    return None


def _build_jobs_index(jobs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(job.get("id")): job for job in jobs if job.get("id")}


def _apply_run_outcome(
    job: Dict[str, Any],
    *,
    success: bool,
    error: Optional[str],
    run_at: str,
    recompute_next_run: bool,
    delivery_error: Optional[str] = None,
) -> bool:
    """Apply run outcome to a job in-place.

    Returns True when the job should be removed because a repeat limit was hit.
    """
    was_paused = job.get("state") == "paused" or not job.get("enabled", True)

    job["last_run_at"] = run_at
    job["last_status"] = "ok" if success else "error"
    job["last_error"] = None if success else error
    job["last_delivery_error"] = delivery_error

    if job.get("repeat"):
        job["repeat"]["completed"] = job["repeat"].get("completed", 0) + 1
        times = job["repeat"].get("times")
        completed = job["repeat"]["completed"]
        if times is not None and times > 0 and completed >= times:
            return True

    schedule = job.get("schedule", {})
    kind = schedule.get("kind")
    if recompute_next_run:
        next_run = compute_next_run(schedule, run_at)
    elif kind == "once":
        next_run = None
    else:
        next_run = job.get("next_run_at") or compute_next_run(schedule, run_at)

    job["next_run_at"] = next_run
    if was_paused:
        job["enabled"] = False
        job["state"] = "paused"
    elif next_run is None:
        if kind in ("cron", "interval"):
            job["enabled"] = True
            job["state"] = "error"
            if not job.get("last_error"):
                job["last_error"] = (
                    "Failed to compute next run for recurring schedule "
                    "(is the 'croniter' package installed in the gateway's Python env?)"
                )
            logger.error(
                "Job '%s' (%s) could not compute next_run_at; leaving enabled and marking state=error",
                job.get("name", job.get("id")),
                kind,
            )
        else:
            job["enabled"] = False
            job["state"] = "completed"
    else:
        job["state"] = "scheduled"

    return False


def _restore_recoverable_next_run(job: Dict[str, Any], in_flight: Dict[str, Any], *, now_iso: str) -> None:
    """Undo claim-time recurring schedule advancement so recovery can requeue the job."""
    kind = job.get("schedule", {}).get("kind")
    if kind not in ("cron", "interval"):
        return

    claimed_at = in_flight.get("claimed_at")
    recovered_due = claimed_at if _parse_iso_datetime(claimed_at) else now_iso
    job["next_run_at"] = recovered_due


def _collect_due_jobs(
    raw_jobs: List[Dict[str, Any]],
    now: datetime,
    *,
    skip_in_flight: bool = True,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Collect due jobs from raw storage records."""
    jobs = [_apply_skill_fields(j) for j in copy.deepcopy(raw_jobs)]
    due: List[Dict[str, Any]] = []
    needs_save = False
    raw_by_id = _build_jobs_index(raw_jobs)

    for job in jobs:
        manual_trigger_at = job.get("trigger_once_at")
        manual_trigger_dt = _parse_iso_datetime(manual_trigger_at) if manual_trigger_at else None
        manual_trigger_due = manual_trigger_dt is not None and manual_trigger_dt <= now

        if not job.get("enabled", True) and not manual_trigger_due:
            continue
        if skip_in_flight and job.get("in_flight"):
            continue

        if manual_trigger_due:
            raw = raw_by_id.get(job["id"])
            if raw is not None:
                raw["trigger_once_at"] = None
                needs_save = True
            job["trigger_once_at"] = None
            due.append(job)
            continue

        next_run = job.get("next_run_at")
        if not next_run:
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")
            recovered_next = _recoverable_oneshot_run_at(
                schedule,
                now,
                last_run_at=job.get("last_run_at"),
            )
            recovery_kind = "one-shot" if recovered_next else None
            if not recovered_next and kind in ("cron", "interval"):
                recovered_next = compute_next_run(schedule, now.isoformat())
                if recovered_next:
                    recovery_kind = kind
            if not recovered_next:
                continue

            job["next_run_at"] = recovered_next
            next_run = recovered_next
            logger.info(
                "Job '%s' had no next_run_at; recovering %s run at %s",
                job.get("name", job["id"]),
                recovery_kind,
                recovered_next,
            )
            raw = raw_by_id.get(job["id"])
            if raw is not None:
                raw["next_run_at"] = recovered_next
                needs_save = True

        next_run_dt = _parse_iso_datetime(next_run)
        if not next_run_dt:
            continue

        if next_run_dt <= now:
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")

            grace = _compute_grace_seconds(schedule)
            if kind in ("cron", "interval") and (now - next_run_dt).total_seconds() > grace:
                new_next = compute_next_run(schedule, now.isoformat())
                if new_next:
                    logger.info(
                        "Job '%s' missed its scheduled time (%s, grace=%ds). "
                        "Fast-forwarding to next run: %s",
                        job.get("name", job["id"]),
                        next_run,
                        grace,
                        new_next,
                    )
                    raw = raw_by_id.get(job["id"])
                    if raw is not None:
                        raw["next_run_at"] = new_next
                        needs_save = True
                    continue

            due.append(job)

    return due, needs_save


# =============================================================================
# Job CRUD Operations
# =============================================================================

def load_jobs() -> List[Dict[str, Any]]:
    """Load all jobs from storage."""
    ensure_dirs()
    if not JOBS_FILE.exists():
        return []
    
    try:
        with open(JOBS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except json.JSONDecodeError:
        # Retry with strict=False to handle bare control chars in string values
        try:
            with open(JOBS_FILE, 'r', encoding='utf-8') as f:
                data = json.loads(f.read(), strict=False)
                jobs = data.get("jobs", [])
                if jobs:
                    # Auto-repair: rewrite with proper escaping
                    save_jobs(jobs)
                    logger.warning("Auto-repaired jobs.json (had invalid control characters)")
                return jobs
        except Exception as e:
            logger.error("Failed to auto-repair jobs.json: %s", e)
            raise RuntimeError(f"Cron database corrupted and unrepairable: {e}") from e
    except IOError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e


def save_jobs(jobs: List[Dict[str, Any]]):
    """Save all jobs to storage."""
    ensure_dirs()
    fd, tmp_path = tempfile.mkstemp(dir=str(JOBS_FILE.parent), suffix='.tmp', prefix='.jobs_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump({"jobs": jobs, "updated_at": _hermes_now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, JOBS_FILE)
        _secure_file(JOBS_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _normalize_workdir(workdir: Optional[str]) -> Optional[str]:
    """Normalize and validate a cron job workdir.

    Rules:
      - Empty / None → None (feature off, preserves old behaviour).
      - ``~`` is expanded.  Relative paths are rejected — cron jobs run detached
        from any shell cwd, so relative paths have no stable meaning.
      - The path must exist and be a directory at create/update time.  We do
        NOT re-check at run time (a user might briefly unmount the dir; the
        scheduler will just fall back to old behaviour with a logged warning).

    Returns the absolute path string, or None when disabled.
    Raises ValueError on invalid input.
    """
    if workdir is None:
        return None
    raw = str(workdir).strip()
    if not raw:
        return None
    expanded = Path(raw).expanduser()
    if not expanded.is_absolute():
        raise ValueError(
            f"Cron workdir must be an absolute path (got {raw!r}). "
            f"Cron jobs run detached from any shell cwd, so relative paths are ambiguous."
        )
    resolved = expanded.resolve()
    if not resolved.exists():
        raise ValueError(f"Cron workdir does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"Cron workdir is not a directory: {resolved}")
    return str(resolved)


def create_job(
    prompt: Optional[str],
    schedule: str,
    name: Optional[str] = None,
    repeat: Optional[int] = None,
    deliver: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    skill: Optional[str] = None,
    skills: Optional[List[str]] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
    base_url: Optional[str] = None,
    script: Optional[str] = None,
    context_from: Optional[Union[str, List[str]]] = None,
    enabled_toolsets: Optional[List[str]] = None,
    workdir: Optional[str] = None,
    no_agent: bool = False,
) -> Dict[str, Any]:
    """
    Create a new cron job.

    Args:
        prompt: The prompt to run (must be self-contained, or a task instruction when skill is set).
                Ignored when ``no_agent=True`` except as an optional name hint.
        schedule: Schedule string (see parse_schedule)
        name: Optional friendly name
        repeat: How many times to run (None = forever, 1 = once)
        deliver: Where to deliver output ("origin", "local", "telegram", etc.)
        origin: Source info where job was created (for "origin" delivery)
        skill: Optional legacy single skill name to load before running the prompt
        skills: Optional ordered list of skills to load before running the prompt
        model: Optional per-job model override
        provider: Optional per-job provider override
        base_url: Optional per-job base URL override
        script: Optional path to a script whose stdout feeds the job. With
                ``no_agent=True`` the script IS the job — its stdout is
                delivered verbatim. Without ``no_agent``, its stdout is
                injected into the agent's prompt as context (data-collection /
                change-detection pattern). Paths resolve under
                ~/.hermes/scripts/; ``.sh`` / ``.bash`` files run via bash,
                anything else via Python.
        context_from: Optional job ID (or list of job IDs) whose most recent output
                      is injected into the prompt as context before each run.
                      Useful for chaining cron jobs: job A finds data, job B processes it.
        enabled_toolsets: Optional list of toolset names to restrict the agent to.
                          When set, only tools from these toolsets are loaded, reducing
                          token overhead. When omitted, all default tools are loaded.
                          Ignored when ``no_agent=True``.
        workdir: Optional absolute path.  When set, the job runs as if launched
                from that directory: AGENTS.md / CLAUDE.md / .cursorrules from
                that directory are injected into the system prompt, and the
                terminal/file/code_exec tools use it as their working directory
                (via TERMINAL_CWD).  When unset, the old behaviour is preserved
                (no context files injected, tools use the scheduler's cwd).
                With ``no_agent=True``, ``workdir`` is still applied as the
                script's cwd so relative paths inside the script behave
                predictably.
        no_agent: When True, skip the agent entirely — run ``script`` on schedule
                and deliver its stdout directly. Empty stdout = silent (no
                delivery). Requires ``script`` to be set. Ideal for classic
                watchdogs and periodic alerts that don't need LLM reasoning.

    Returns:
        The created job dict
    """
    parsed_schedule = parse_schedule(schedule)

    # Normalize repeat: treat 0 or negative values as None (infinite)
    if repeat is not None and repeat <= 0:
        repeat = None

    # Auto-set repeat=1 for one-shot schedules if not specified
    if parsed_schedule["kind"] == "once" and repeat is None:
        repeat = 1

    # Default delivery to origin if available, otherwise local
    if deliver is None:
        deliver = "origin" if origin else "local"

    job_id = uuid.uuid4().hex[:12]
    now = _hermes_now().isoformat()

    normalized_skills = _normalize_skill_list(skill, skills)
    normalized_model = str(model).strip() if isinstance(model, str) else None
    normalized_provider = str(provider).strip() if isinstance(provider, str) else None
    normalized_base_url = str(base_url).strip().rstrip("/") if isinstance(base_url, str) else None
    normalized_model = normalized_model or None
    normalized_provider = normalized_provider or None
    normalized_base_url = normalized_base_url or None
    normalized_script = str(script).strip() if isinstance(script, str) else None
    normalized_script = normalized_script or None
    normalized_toolsets = [str(t).strip() for t in enabled_toolsets if str(t).strip()] if enabled_toolsets else None
    normalized_toolsets = normalized_toolsets or None
    normalized_workdir = _normalize_workdir(workdir)
    normalized_no_agent = bool(no_agent)

    # no_agent jobs are meaningless without a script — the script IS the job.
    # Surface this as a clear ValueError at create time so bad configs never
    # reach the scheduler.
    if normalized_no_agent and not normalized_script:
        raise ValueError(
            "no_agent=True requires a script — with no agent and no script "
            "there is nothing for the job to run."
        )

    # Normalize context_from: accept str or list of str, store as list or None
    if isinstance(context_from, str):
        context_from = [context_from.strip()] if context_from.strip() else None
    elif isinstance(context_from, list):
        context_from = [str(j).strip() for j in context_from if str(j).strip()] or None
    else:
        context_from = None

    label_source = (prompt or (normalized_skills[0] if normalized_skills else None) or (normalized_script if normalized_no_agent else None)) or "cron job"
    job = {
        "id": job_id,
        "name": name or label_source[:50].strip(),
        "prompt": prompt,
        "skills": normalized_skills,
        "skill": normalized_skills[0] if normalized_skills else None,
        "model": normalized_model,
        "provider": normalized_provider,
        "base_url": normalized_base_url,
        "script": normalized_script,
        "no_agent": normalized_no_agent,
        "context_from": context_from,
        "schedule": parsed_schedule,
        "schedule_display": parsed_schedule.get("display", schedule),
        "repeat": {
            "times": repeat,  # None = forever
            "completed": 0
        },
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "created_at": now,
        "next_run_at": compute_next_run(parsed_schedule),
        "last_run_at": None,
        "last_status": None,
        "last_error": None,
        "last_delivery_error": None,
        # Delivery configuration
        "deliver": deliver,
        "origin": origin,  # Tracks where job was created for "origin" delivery
        "enabled_toolsets": normalized_toolsets,
        "workdir": normalized_workdir,
    }

    jobs = load_jobs()
    jobs.append(job)
    save_jobs(jobs)

    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID."""
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            return _apply_skill_fields(job)
    return None


def list_jobs(include_disabled: bool = False) -> List[Dict[str, Any]]:
    """List all jobs, optionally including disabled ones."""
    jobs = [_apply_skill_fields(j) for j in load_jobs()]
    if not include_disabled:
        jobs = [j for j in jobs if j.get("enabled", True)]
    return jobs


def update_job(job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update a job by ID, refreshing derived schedule fields when needed."""
    jobs = load_jobs()
    for i, job in enumerate(jobs):
        if job["id"] != job_id:
            continue

        # Validate / normalize workdir if present in updates.  Empty string or
        # None both mean "clear the field" (restore old behaviour).
        if "workdir" in updates:
            _wd = updates["workdir"]
            if _wd in (None, "", False):
                updates["workdir"] = None
            else:
                updates["workdir"] = _normalize_workdir(_wd)

        updated = _apply_skill_fields({**job, **updates})
        schedule_changed = "schedule" in updates

        if "skills" in updates or "skill" in updates:
            normalized_skills = _normalize_skill_list(updated.get("skill"), updated.get("skills"))
            updated["skills"] = normalized_skills
            updated["skill"] = normalized_skills[0] if normalized_skills else None

        if schedule_changed:
            updated_schedule = updated["schedule"]
            # The API may pass schedule as a raw string (e.g. "every 10m")
            # instead of a pre-parsed dict.  Normalize it the same way
            # create_job() does so downstream code can call .get() safely.
            if isinstance(updated_schedule, str):
                updated_schedule = parse_schedule(updated_schedule)
                updated["schedule"] = updated_schedule
            updated["schedule_display"] = updates.get(
                "schedule_display",
                updated_schedule.get("display", updated.get("schedule_display")),
            )
            if updated.get("state") != "paused":
                updated["next_run_at"] = compute_next_run(updated_schedule)

        if updated.get("enabled", True) and updated.get("state") != "paused" and not updated.get("next_run_at"):
            updated["next_run_at"] = compute_next_run(updated["schedule"])

        jobs[i] = updated
        save_jobs(jobs)
        return _apply_skill_fields(jobs[i])
    return None


def pause_job(job_id: str, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pause a job without deleting it."""
    return update_job(
        job_id,
        {
            "enabled": False,
            "state": "paused",
            "paused_at": _hermes_now().isoformat(),
            "paused_reason": reason,
        },
    )


def resume_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Resume a paused job and compute the next future run from now."""
    job = get_job(job_id)
    if not job:
        return None

    next_run_at = compute_next_run(job["schedule"])
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": next_run_at,
        },
    )


def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Trigger exactly one run on the next scheduler tick without changing pause/resume state."""
    job = get_job(job_id)
    if not job:
        return None
    return update_job(
        job_id,
        {
            "trigger_once_at": _hermes_now().isoformat(),
        },
    )


def remove_job(job_id: str) -> bool:
    """Remove a job by ID."""
    jobs = load_jobs()
    original_len = len(jobs)
    jobs = [j for j in jobs if j["id"] != job_id]
    if len(jobs) < original_len:
        save_jobs(jobs)
        return True
    return False


def recover_stale_inflight(now: Optional[datetime] = None) -> int:
    """Recover timed-out or clearly orphaned in-flight runs.

    Recovery is intentionally recorded as a failed run attempt: it clears the
    in-flight claim, stores an error status, and increments repeat accounting.
    This is conservative for operators because a claimed job may have performed
    partial side effects before the owner wedged or disappeared.
    """
    now_dt = now or _hermes_now()
    now_iso = now_dt.isoformat()
    with _jobs_file_lock:
        jobs = load_jobs()
        recovered = 0
        needs_save = False

        for idx in range(len(jobs) - 1, -1, -1):
            job = jobs[idx]
            in_flight = job.get("in_flight")
            if not isinstance(in_flight, dict):
                continue

            timeout_dt = _parse_iso_datetime(in_flight.get("timeout_at"))
            run_id = in_flight.get("run_id", "unknown")
            claimed_at_dt = _parse_iso_datetime(in_flight.get("claimed_at"))
            grace_seconds = _orphan_recovery_grace_seconds()
            within_grace = (
                claimed_at_dt is not None
                and (now_dt - claimed_at_dt).total_seconds() < grace_seconds
            )

            reason: Optional[str] = None
            log_message: Optional[str] = None
            owner_state = "unknown"
            owner_reason = "owner state not evaluated"

            if timeout_dt is None:
                owner_state, owner_reason = _get_inflight_owner_state(in_flight, now_dt=now_dt)
                if owner_state in ("dead", "mismatch") and not within_grace:
                    reason = f"orphan_recovered: {owner_reason}; run_id={run_id}"
                    log_message = "Recovering malformed orphaned in-flight run for job '%s' (run_id=%s): %s"
                elif owner_state == "alive":
                    logger.warning(
                        "Keeping malformed in-flight owner for job '%s' (run_id=%s): %s",
                        job.get("id"),
                        run_id,
                        owner_reason,
                    )
                elif owner_state == "unknown":
                    malformed_age_seconds = None
                    if claimed_at_dt is not None:
                        malformed_age_seconds = (now_dt - claimed_at_dt).total_seconds()
                    malformed_stale = (
                        claimed_at_dt is None
                        or malformed_age_seconds is None
                        or malformed_age_seconds >= (_cron_timeout_seconds() + grace_seconds)
                    )
                    if malformed_stale:
                        reason = f"stale_recovered: invalid_timeout_at; run_id={run_id}"
                        log_message = "Recovering malformed in-flight run for job '%s' (run_id=%s): missing/invalid timeout_at"
                    else:
                        logger.warning(
                            "Deferring malformed in-flight recovery for job '%s' (run_id=%s): %s",
                            job.get("id"),
                            run_id,
                            owner_reason,
                        )
                elif within_grace:
                    logger.debug(
                        "Malformed owner for job '%s' (run_id=%s) appears %s but remains within grace window",
                        job.get("id"),
                        run_id,
                        owner_state,
                    )
            elif timeout_dt <= now_dt:
                reason = f"stale_recovered: run_id={run_id}"
                log_message = "Recovering timed-out in-flight run for job '%s' (run_id=%s)"
            else:
                owner_state, owner_reason = _get_inflight_owner_state(in_flight, now_dt=now_dt)
                if owner_state in ("dead", "mismatch") and not within_grace:
                    reason = f"orphan_recovered: {owner_reason}; run_id={run_id}"
                    log_message = "Recovering orphaned in-flight run for job '%s' (run_id=%s): %s"
                elif owner_state == "unknown":
                    logger.debug(
                        "Deferring early recovery for job '%s' (run_id=%s): %s",
                        job.get("id"),
                        run_id,
                        owner_reason,
                    )
                elif owner_state == "alive":
                    logger.debug(
                        "Keeping live in-flight owner for job '%s' (run_id=%s): %s",
                        job.get("id"),
                        run_id,
                        owner_reason,
                    )
                elif within_grace:
                    logger.debug(
                        "Owner for job '%s' (run_id=%s) appears %s but remains within grace window",
                        job.get("id"),
                        run_id,
                        owner_state,
                    )

            if reason is None:
                continue

            if log_message:
                if log_message.count("%s") >= 3:
                    logger.warning(log_message, job.get("id"), run_id, reason)
                else:
                    logger.warning(log_message, job.get("id"), run_id)

            _restore_recoverable_next_run(job, in_flight, now_iso=now_iso)
            should_remove = _apply_run_outcome(
                job,
                success=False,
                error=reason,
                run_at=now_iso,
                recompute_next_run=False,
                delivery_error=None,
            )
            job["in_flight"] = None

            if should_remove:
                jobs.pop(idx)
            else:
                jobs[idx] = job

            recovered += 1
            needs_save = True

        if needs_save:
            save_jobs(jobs)

        return recovered


def claim_due_jobs(
    now: Optional[datetime] = None,
    owner_instance_id: str = "",
    max_parallel: int = 1,
    owner_pid: Optional[int] = None,
    owner_boot_id: Optional[str] = None,
    owner_process_start: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Claim due jobs by writing durable in-flight ownership metadata."""
    now_dt = now or _hermes_now()
    now_iso = now_dt.isoformat()

    with _jobs_file_lock:
        raw_jobs = load_jobs()
        due_jobs, needs_save = _collect_due_jobs(raw_jobs, now_dt, skip_in_flight=True)
        claimed: List[Dict[str, Any]] = []
        raw_by_id = _build_jobs_index(raw_jobs)

        try:
            claim_budget = max(0, int(max_parallel))
        except (TypeError, ValueError):
            claim_budget = 1

        if claim_budget <= 0:
            if needs_save:
                save_jobs(raw_jobs)
            return []

        claim_owner_pid = owner_pid if owner_pid is not None else os.getpid()
        if claim_owner_pid is not None and claim_owner_pid <= 0:
            claim_owner_pid = None
        claim_owner_boot_id = owner_boot_id if owner_boot_id is not None else _boot_fingerprint()
        claim_owner_process_start = owner_process_start
        if claim_owner_process_start is None and claim_owner_pid is not None:
            claim_owner_process_start = _process_start_fingerprint(claim_owner_pid)

        timeout_at = (
            now_dt
            + timedelta(seconds=_cron_timeout_seconds() + IN_FLIGHT_TIMEOUT_GRACE_SECONDS)
        ).isoformat()

        for due in due_jobs:
            if len(claimed) >= claim_budget:
                break
            raw = raw_by_id.get(due["id"])
            if raw is None or raw.get("in_flight"):
                continue

            run_id = uuid.uuid4().hex
            raw["in_flight"] = {
                "run_id": run_id,
                "owner_instance_id": owner_instance_id,
                "owner_pid": claim_owner_pid,
                "owner_boot_id": claim_owner_boot_id,
                "owner_process_start": claim_owner_process_start,
                "claimed_at": now_iso,
                "timeout_at": timeout_at,
                "started_at": None,
                "status": "claimed",
            }

            kind = raw.get("schedule", {}).get("kind")
            if kind in ("cron", "interval"):
                advanced_next = compute_next_run(raw["schedule"], now_iso)
                if advanced_next:
                    raw["next_run_at"] = advanced_next

            claimed.append(_apply_skill_fields(copy.deepcopy(raw)))
            needs_save = True

        if needs_save:
            save_jobs(raw_jobs)

        return claimed


def mark_job_started(job_id: str, run_id: str, started_at: Optional[str] = None) -> bool:
    """Mark an owned in-flight run as actively running."""
    with _jobs_file_lock:
        jobs = load_jobs()
        idx = _find_job_index(jobs, job_id)
        if idx is None:
            return False

        job = jobs[idx]
        in_flight = job.get("in_flight")
        if not isinstance(in_flight, dict) or in_flight.get("run_id") != run_id:
            return False

        in_flight["started_at"] = started_at or _hermes_now().isoformat()
        in_flight["status"] = "running"
        job["in_flight"] = in_flight
        save_jobs(jobs)
        return True


def clear_inflight_if_owned(job_id: str, run_id: str, reason: Optional[str] = None) -> bool:
    """Clear in_flight only when the provided run_id still owns the claim."""
    with _jobs_file_lock:
        jobs = load_jobs()
        idx = _find_job_index(jobs, job_id)
        if idx is None:
            return False

        job = jobs[idx]
        in_flight = job.get("in_flight")
        if not isinstance(in_flight, dict) or in_flight.get("run_id") != run_id:
            return False

        job["in_flight"] = None
        if reason:
            now_iso = _hermes_now().isoformat()
            job["last_run_at"] = now_iso
            job["last_status"] = "error"
            job["last_error"] = reason
            job["last_delivery_error"] = None
            if job.get("schedule", {}).get("kind") == "once":
                job["next_run_at"] = None
                job["enabled"] = False
                if job.get("state") != "paused":
                    job["state"] = "completed"

        save_jobs(jobs)
        return True


def finalize_job_run(
    job_id: str,
    run_id: str,
    success: bool,
    error: Optional[str] = None,
    finished_at: Optional[str] = None,
    delivery_error: Optional[str] = None,
) -> bool:
    """Finalize a run only when run_id still matches the stored owner."""
    with _jobs_file_lock:
        jobs = load_jobs()
        idx = _find_job_index(jobs, job_id)
        if idx is None:
            return False

        job = jobs[idx]
        in_flight = job.get("in_flight")
        if not isinstance(in_flight, dict) or in_flight.get("run_id") != run_id:
            return False

        run_at = finished_at or _hermes_now().isoformat()
        should_remove = _apply_run_outcome(
            job,
            success=success,
            error=error,
            run_at=run_at,
            recompute_next_run=False,
            delivery_error=delivery_error,
        )
        job["in_flight"] = None

        if should_remove:
            jobs.pop(idx)
        else:
            jobs[idx] = job
        save_jobs(jobs)
        return True


def update_delivery_error_if_latest(job_id: str, run_at: str, delivery_error: Optional[str]) -> bool:
    """Update delivery status only if the recorded run still matches ``run_at``."""
    with _jobs_file_lock:
        jobs = load_jobs()
        idx = _find_job_index(jobs, job_id)
        if idx is None:
            return False

        job = jobs[idx]
        if job.get("last_run_at") != run_at:
            return False

        job["last_delivery_error"] = delivery_error
        jobs[idx] = job
        save_jobs(jobs)
        return True


def mark_job_run(job_id: str, success: bool, error: Optional[str] = None,
                 delivery_error: Optional[str] = None):
    """
    Mark a job as having been run.
    
    Updates last_run_at, last_status, increments completed count,
    computes next_run_at, and auto-deletes if repeat limit reached.

    ``delivery_error`` is tracked separately from the agent error — a job
    can succeed (agent produced output) but fail delivery (platform down).
    """
    with _jobs_file_lock:
        jobs = load_jobs()
        idx = _find_job_index(jobs, job_id)
        if idx is None:
            logger.warning("mark_job_run: job_id %s not found, skipping save", job_id)
            return

        job = jobs[idx]
        run_at = _hermes_now().isoformat()
        should_remove = _apply_run_outcome(
            job,
            success=success,
            error=error,
            run_at=run_at,
            recompute_next_run=True,
            delivery_error=delivery_error,
        )

        if should_remove:
            jobs.pop(idx)
        else:
            jobs[idx] = job
        save_jobs(jobs)


def advance_next_run(job_id: str) -> bool:
    """Preemptively advance next_run_at for a recurring job before execution.

    Call this BEFORE run_job() so that if the process crashes mid-execution,
    the job won't re-fire on the next gateway restart.  This converts the
    scheduler from at-least-once to at-most-once for recurring jobs — missing
    one run is far better than firing dozens of times in a crash loop.

    One-shot jobs are left unchanged so they can still retry on restart.

    Returns True if next_run_at was advanced, False otherwise.
    """
    with _jobs_file_lock:
        jobs = load_jobs()
        for job in jobs:
            if job["id"] == job_id:
                kind = job.get("schedule", {}).get("kind")
                if kind not in ("cron", "interval"):
                    return False
                now = _hermes_now().isoformat()
                new_next = compute_next_run(job["schedule"], now)
                if new_next and new_next != job.get("next_run_at"):
                    job["next_run_at"] = new_next
                    save_jobs(jobs)
                    return True
                return False
        return False


def get_due_jobs() -> List[Dict[str, Any]]:
    """Get all jobs that are due to run now.

    For recurring jobs (cron/interval), if the scheduled time is stale
    (more than one period in the past, e.g. because the gateway was down),
    the job is fast-forwarded to the next future run instead of firing
    immediately.  This prevents a burst of missed jobs on gateway restart.
    """
    with _jobs_file_lock:
        now = _hermes_now()
        raw_jobs = load_jobs()
        due, needs_save = _collect_due_jobs(raw_jobs, now, skip_in_flight=True)
        if needs_save:
            save_jobs(raw_jobs)
        return due


def save_job_output(job_id: str, output: str):
    """Save job output to file."""
    ensure_dirs()
    job_output_dir = OUTPUT_DIR / job_id
    job_output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(job_output_dir)
    
    timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = job_output_dir / f"{timestamp}.md"
    
    fd, tmp_path = tempfile.mkstemp(dir=str(job_output_dir), suffix='.tmp', prefix='.output_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(output)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, output_file)
        _secure_file(output_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    
    return output_file


# =============================================================================
# Skill reference rewriting (curator integration)
# =============================================================================

def rewrite_skill_refs(
    consolidated: Optional[Dict[str, str]] = None,
    pruned: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Rewrite cron job skill references after a curator consolidation pass.

    When the curator consolidates a skill X into umbrella Y (or archives X
    as pruned), any cron job that lists ``X`` in its ``skills`` field will
    fail to load ``X`` at run time — the scheduler logs a warning and
    skips the skill, so the job runs without the instructions it was
    scheduled to follow. See cron/scheduler.py where ``skill_view`` is
    called per skill name.

    This function repairs cron jobs in-place:

    - A skill listed in ``consolidated`` is replaced with its umbrella
      target (the ``into`` value). If the umbrella is already in the
      job's skill list, the stale name is dropped without duplication.
    - A skill listed in ``pruned`` is dropped outright — there is no
      forwarding target.
    - Ordering and other skills in the list are preserved.
    - The legacy ``skill`` field is realigned via ``_apply_skill_fields``.

    Args:
        consolidated: mapping of ``old_skill_name -> umbrella_skill_name``.
        pruned: list of skill names that were archived with no forwarding
            target.

    Returns a report dict::

        {
            "rewrites": [
                {
                    "job_id": ...,
                    "job_name": ...,
                    "before": [...],
                    "after": [...],
                    "mapped": {"old": "new", ...},
                    "dropped": ["old", ...],
                },
                ...
            ],
            "jobs_updated": N,
            "jobs_scanned": M,
        }

    Best-effort: exceptions from loading/saving propagate to the caller so
    tests can assert behaviour; the curator invocation site wraps this
    call in a try/except so a failure here never breaks the curator.
    """
    consolidated = dict(consolidated or {})
    pruned_set = set(pruned or [])
    # A skill listed in both wins as "consolidated" — it has a target,
    # which is the more useful of the two outcomes.
    pruned_set -= set(consolidated.keys())

    if not consolidated and not pruned_set:
        return {"rewrites": [], "jobs_updated": 0, "jobs_scanned": 0}

    with _jobs_file_lock:
        jobs = load_jobs()
        rewrites: List[Dict[str, Any]] = []
        changed = False

        for job in jobs:
            skills_before = _normalize_skill_list(job.get("skill"), job.get("skills"))
            if not skills_before:
                continue

            mapped: Dict[str, str] = {}
            dropped: List[str] = []
            new_skills: List[str] = []

            for name in skills_before:
                if name in consolidated:
                    target = consolidated[name]
                    mapped[name] = target
                    if target and target not in new_skills:
                        new_skills.append(target)
                elif name in pruned_set:
                    dropped.append(name)
                else:
                    if name not in new_skills:
                        new_skills.append(name)

            if not mapped and not dropped:
                continue

            job["skills"] = new_skills
            job["skill"] = new_skills[0] if new_skills else None
            changed = True

            rewrites.append({
                "job_id": job.get("id"),
                "job_name": job.get("name") or job.get("id"),
                "before": list(skills_before),
                "after": list(new_skills),
                "mapped": mapped,
                "dropped": dropped,
            })

        if changed:
            save_jobs(jobs)
            logger.info(
                "Curator rewrote skill references in %d cron job(s)", len(rewrites)
            )

        return {
            "rewrites": rewrites,
            "jobs_updated": len(rewrites),
            "jobs_scanned": len(jobs),
        }
