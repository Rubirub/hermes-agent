# Cron orphaned in-flight recovery implementation plan

> For Hermes: use this as the execution plan for the cron parallelism branch. Follow the existing spec in `designs/2026-04-05-cron-orphaned-inflight-recovery-spec.md`.

**Goal:** Make cron reclaim orphaned `in_flight` jobs shortly after the owning gateway process dies, instead of waiting for the full cron timeout.

**Architecture:** Keep the current short global scheduler lock + per-job lock model. Extend persisted `in_flight` metadata with structured owner identity, add best-effort owner liveness checks in `cron/jobs.py`, and let the normal `tick()` flow recover orphaned claims before re-claiming due work. Do not rely on shutdown-time claim clearing.

**Tech Stack:** Python stdlib only (`os`, `pathlib`, `datetime`, optional `/proc` reads on Linux), existing cron persistence in `cron/jobs.py`, scheduler flow in `cron/scheduler.py`, pytest in `tests/cron/`.

---

## Preconditions

Before touching code, re-read:
- `designs/2026-04-05-cron-orphaned-inflight-recovery-spec.md`
- `cron/jobs.py`
- `cron/scheduler.py`
- `tests/cron/test_jobs.py`
- `tests/cron/test_scheduler.py`

Keep these current invariants intact:
- claim under short scheduler lock
- run outside global lock
- save output before finalize
- finalize only when `run_id` still owns the claim
- stale/orphan recovery must go through shared outcome logic

---

## Task 1: Add test scaffolding for owner-state decisions in `tests/cron/test_jobs.py`

**Objective:** Create a clean place to test orphan recovery without depending on real host process state.

**Files:**
- Modify: `tests/cron/test_jobs.py`

**Step 1: Add imports you will need**

Add any missing imports near the top of `tests/cron/test_jobs.py`:
- `timedelta`, `datetime`, `timezone` are already there
- add `types` or `dataclasses` only if truly needed
- prefer patching helpers over creating heavy fake classes

**Step 2: Add a focused test section**

Create a new test class below `TestInFlightRecovery`, for example:

```python
class TestOrphanedInFlightRecovery:
    pass
```

Do not add production code yet.

**Step 3: Run collection only**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts='' --collect-only
```

Expected:
- test file collects cleanly
- no syntax/import failures

---

## Task 2: Add failing unit tests for early orphan recovery in `tests/cron/test_jobs.py`

**Objective:** Lock in the intended behavior before changing recovery code.

**Files:**
- Modify: `tests/cron/test_jobs.py`

**Step 1: Add a failing test for dead-owner early recovery**

Add a test in the new class, with this shape:

```python
def test_orphaned_inflight_is_recovered_before_timeout_when_owner_is_dead(self, tmp_cron_dir, monkeypatch):
    now = datetime(2026, 3, 18, 4, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

    job = create_job(prompt="orphan candidate", schedule="every 1h")
    jobs = load_jobs()
    jobs[0]["next_run_at"] = (now - timedelta(minutes=5)).isoformat()
    save_jobs(jobs)

    claimed = claim_due_jobs(now=now, owner_instance_id="instance-a", max_parallel=1)
    assert len(claimed) == 1

    claimed_state = get_job(job["id"])
    assert claimed_state is not None

    future_recovery_time = now + timedelta(seconds=90)
    monkeypatch.setattr("cron.jobs._get_inflight_owner_state", lambda inflight, now_dt=None: ("dead", "owner pid not alive"))
    monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 30.0)

    recovered = recover_stale_inflight(now=future_recovery_time)
    assert recovered == 1

    updated = get_job(job["id"])
    assert updated is not None
    assert updated.get("in_flight") is None
    assert updated.get("last_status") == "error"
    assert "orphan_recovered" in (updated.get("last_error") or "")
```

Use the helper names above even though they do not exist yet — the test should fail first.

**Step 2: Add a failing test for live-owner no-op**

```python
def test_live_owner_is_not_recovered_before_timeout(self, tmp_cron_dir, monkeypatch):
    ...
    monkeypatch.setattr("cron.jobs._get_inflight_owner_state", lambda inflight, now_dt=None: ("alive", "owner still alive"))
    monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 30.0)
    recovered = recover_stale_inflight(now=now + timedelta(seconds=90))
    assert recovered == 0
```

Assert `in_flight` remains present.

**Step 3: Add a failing test for unknown-owner fallback**

```python
def test_unknown_owner_state_falls_back_to_timeout_only(self, tmp_cron_dir, monkeypatch):
    ...
    monkeypatch.setattr("cron.jobs._get_inflight_owner_state", lambda inflight, now_dt=None: ("unknown", "no fingerprint"))
    monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 30.0)
    assert recover_stale_inflight(now=now + timedelta(seconds=90)) == 0
    timeout_at = datetime.fromisoformat(get_job(job["id"])["in_flight"]["timeout_at"])
    assert recover_stale_inflight(now=timeout_at + timedelta(seconds=1)) == 1
```

**Step 4: Run only the new tests**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts='' -k 'orphaned or live_owner or unknown_owner'
```

Expected:
- FAIL
- failures should be about missing helper functions / changed behavior, not syntax

---

## Task 3: Add failing backward-compatibility tests for legacy owner metadata

**Objective:** Ensure old `jobs.json` records remain recoverable.

**Files:**
- Modify: `tests/cron/test_jobs.py`

**Step 1: Add failing legacy-PID parsing test**

Add a test that mutates persisted `in_flight` into a legacy shape:

```python
def test_legacy_owner_instance_id_pid_can_be_recovered_early(self, tmp_cron_dir, monkeypatch):
    now = datetime(2026, 3, 18, 4, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

    job = create_job(prompt="legacy orphan", schedule="every 1h")
    jobs = load_jobs()
    jobs[0]["next_run_at"] = (now - timedelta(minutes=5)).isoformat()
    save_jobs(jobs)
    claim_due_jobs(now=now, owner_instance_id="43210-legacyaa", max_parallel=1)

    jobs = load_jobs()
    jobs[0]["in_flight"] = {
        "run_id": jobs[0]["in_flight"]["run_id"],
        "owner_instance_id": "43210-legacyaa",
        "claimed_at": jobs[0]["in_flight"]["claimed_at"],
        "timeout_at": jobs[0]["in_flight"]["timeout_at"],
        "started_at": jobs[0]["in_flight"]["started_at"],
        "status": jobs[0]["in_flight"]["status"],
    }
    save_jobs(jobs)

    monkeypatch.setattr("cron.jobs._legacy_owner_pid_is_dead", lambda pid: True)
    monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 30.0)
    recovered = recover_stale_inflight(now=now + timedelta(seconds=90))
    assert recovered == 1
```

This assumes you add a small testable helper for legacy PID liveness.

**Step 2: Add malformed-owner-id fallback test**

```python
def test_malformed_legacy_owner_id_does_not_guess(self, tmp_cron_dir, monkeypatch):
    ...
    jobs[0]["in_flight"]["owner_instance_id"] = "weird-format"
    ...
    recovered = recover_stale_inflight(now=now + timedelta(seconds=90))
    assert recovered == 0
```

Then assert timeout-based recovery still works later.

**Step 3: Add repeat-limit early-orphan test**

Mirror the existing stale repeat-limit test, but trigger recovery via dead owner before timeout.

Expected final assertion:
```python
assert get_job(job["id"]) is None
```

**Step 4: Run the recovery test block**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts='' -k 'legacy or malformed or repeat_limit or orphaned'
```

Expected:
- FAIL for behavior, not syntax

---

## Task 4: Add owner metadata and liveness helpers in `cron/jobs.py`

**Objective:** Make owner liveness testable and keep recovery logic readable.

**Files:**
- Modify: `cron/jobs.py`

**Step 1: Add small helper functions near `_cron_timeout_seconds()` / datetime helpers**

Add helpers with narrow responsibilities. Suggested exact API:

```python
def _orphan_recovery_grace_seconds() -> float:
    try:
        value = float(os.getenv("HERMES_CRON_ORPHAN_GRACE_SECONDS", 60))
    except (TypeError, ValueError):
        value = 60.0
    return max(value, 0.0)


def _parse_legacy_owner_pid(owner_instance_id: Optional[str]) -> Optional[int]:
    ...


def _linux_boot_id() -> Optional[str]:
    ...


def _linux_process_start_fingerprint(pid: int) -> Optional[str]:
    ...


def _process_identity_matches(pid: int, *, boot_id: Optional[str], process_start: Optional[str]) -> Optional[bool]:
    ...


def _legacy_owner_pid_is_dead(pid: int) -> bool:
    ...


def _get_inflight_owner_state(in_flight: dict, now_dt: Optional[datetime] = None) -> tuple[str, str]:
    ...
```

Return values for `_get_inflight_owner_state` should be constrained to:
- `("alive", reason)`
- `("dead", reason)`
- `("mismatch", reason)`
- `("unknown", reason)`

**Step 2: Keep OS behavior conservative**

Implementation rules:
- on Linux, use `/proc` and `/proc/sys/kernel/random/boot_id`
- on unsupported platforms or unreadable files, return `unknown`
- never guess “dead” unless you have strong evidence

**Step 3: Run only `tests/cron/test_jobs.py`**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts=''
```

Expected:
- still failing, but now because `claim_due_jobs()` and `recover_stale_inflight()` have not been updated yet

---

## Task 5: Extend claim payload in `cron/jobs.py`

**Objective:** Persist structured owner identity on newly claimed jobs.

**Files:**
- Modify: `cron/jobs.py`
- Modify: `cron/scheduler.py`

**Step 1: Add a scheduler helper for current owner metadata**

In `cron/scheduler.py`, add a helper near `_INSTANCE_ID`:

```python
def _current_owner_metadata() -> dict:
    return {
        "owner_instance_id": _INSTANCE_ID,
        "owner_pid": os.getpid(),
        "owner_boot_id": ...,          # best-effort
        "owner_process_start": ...,    # best-effort
    }
```

If you prefer to keep all OS inspection in `cron/jobs.py`, make `claim_due_jobs()` compute/store the structured fields there instead. But pick one place and keep it consistent.

**Step 2: Update `claim_due_jobs()`**

Extend this payload in `cron/jobs.py:809-816`:

Current:
```python
raw["in_flight"] = {
    "run_id": run_id,
    "owner_instance_id": owner_instance_id,
    ...
}
```

Target shape:
```python
raw["in_flight"] = {
    "run_id": run_id,
    "owner_instance_id": owner_instance_id,
    "owner_pid": owner_pid,
    "owner_boot_id": owner_boot_id,
    "owner_process_start": owner_process_start,
    "claimed_at": now_iso,
    "timeout_at": timeout_at,
    "started_at": None,
    "status": "claimed",
}
```

Do not remove `owner_instance_id`.

**Step 3: Add/adjust tests that inspect persisted claim shape**

If needed, add a small assertion to one recovery test that `owner_pid` is present on newly claimed jobs.

**Step 4: Run focused tests**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts='' -k 'claim or orphan or stale'
```

Expected:
- some recovery tests still fail until Task 6 is done

---

## Task 6: Extend `recover_stale_inflight()` for orphan recovery

**Objective:** Recover dead-owner claims before timeout while preserving existing accounting.

**Files:**
- Modify: `cron/jobs.py`

**Step 1: Refactor the decision branch, not the outcome path**

Do not change `_apply_run_outcome(...)` semantics.

Replace this effective logic:
```python
timeout_dt = _parse_iso_datetime(in_flight.get("timeout_at"))
if timeout_dt and timeout_dt > now_dt:
    continue
```

with a three-stage decision:

```python
timeout_dt = _parse_iso_datetime(in_flight.get("timeout_at"))
if timeout_dt and timeout_dt <= now_dt:
    reason = f"stale_recovered: run_id={run_id}"
    ...recover...
    continue

owner_state, owner_reason = _get_inflight_owner_state(in_flight, now_dt=now_dt)
claimed_at_dt = _parse_iso_datetime(in_flight.get("claimed_at"))
grace_seconds = _orphan_recovery_grace_seconds()
within_grace = claimed_at_dt is not None and (now_dt - claimed_at_dt).total_seconds() < grace_seconds

if owner_state in ("dead", "mismatch") and not within_grace:
    reason = f"orphan_recovered: {owner_reason}; run_id={run_id}"
    ...recover...
    continue

continue
```

Keep the actual recovery mutation exactly routed through `_apply_run_outcome(...)` as today.

**Step 2: Add explicit logging**

Use `logger.warning(...)` before recovery with messages that distinguish:
- timeout stale recovery
- dead owner recovery
- identity mismatch recovery
- unknown owner fallback

**Step 3: Make legacy fallback conservative**

If owner liveness is unknown, do nothing early and leave timeout-based behavior unchanged.

**Step 4: Run the jobs tests**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts=''
```

Expected:
- all `tests/cron/test_jobs.py` tests pass

---

## Task 7: Add scheduler-level restart regression tests in `tests/cron/test_scheduler.py`

**Objective:** Prove a tick can recover a dead-owner claim and dispatch the job.

**Files:**
- Modify: `tests/cron/test_scheduler.py`

**Step 1: Add a tick-level orphan reclaim test**

Under `TestParallelCronExecution`, add:

```python
def test_tick_recovers_orphan_and_dispatches_job(self, cron_runtime, monkeypatch):
    job = create_job(prompt="orphan", schedule="every 1h", name="job-orphan")
    self._set_due_now()
    monkeypatch.setattr("cron.scheduler.load_config", lambda: {"cron": {"max_parallel_jobs": 1, "wrap_response": False}})

    claimed = claim_due_jobs(now=scheduler._hermes_now(), owner_instance_id="instance-a", max_parallel=1)
    assert len(claimed) == 1

    jobs = load_jobs()
    jobs[0]["in_flight"]["timeout_at"] = (scheduler._hermes_now() + timedelta(hours=1)).isoformat()
    save_jobs(jobs)

    monkeypatch.setattr("cron.jobs._get_inflight_owner_state", lambda inflight, now_dt=None: ("dead", "owner pid not alive"))
    monkeypatch.setattr("cron.jobs._orphan_recovery_grace_seconds", lambda: 0.0)

    with patch("cron.scheduler.run_job", return_value=(True, "# output", "done", None)), \
         patch("cron.scheduler.save_job_output", return_value=cron_runtime / "out.md"), \
         patch("cron.scheduler._deliver_result"):
        dispatched = tick(verbose=False)
        assert dispatched == 1
        _wait_for_cron_workers()

    updated = get_job(job["id"])
    assert updated is not None
    assert updated.get("in_flight") is None
    assert updated.get("last_status") == "ok"
```

**Step 2: Add live-owner no-reclaim test**

Same setup, but monkeypatch owner state to `alive` and assert:
```python
assert tick(verbose=False) == 0
```

**Step 3: Add grace-window handoff test**

Set owner state to `dead`, but patch grace to 60s and claimed_at to “just now”. Assert first tick is 0; after moving time past grace, next tick is 1.

**Step 4: Add legacy-owner tick regression**

Mutate `in_flight` to legacy-only shape and assert reclaim still works if legacy PID helper says dead.

**Step 5: Run scheduler tests**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_scheduler.py -q -o addopts=''
```

Expected:
- failures, if any, should now be limited to naming/patching mismatches

---

## Task 8: Add stale-completion safety regression for recovered-orphan ownership changes

**Objective:** Make sure old work finishing late cannot deliver after ownership has been recovered/replaced.

**Files:**
- Modify: `tests/cron/test_scheduler.py`

**Step 1: Add a focused test around finalize discard after orphan recovery**

Reuse the existing stale-finalize pattern around `tests/cron/test_scheduler.py:1044+`.

Add a test like:

```python
def test_old_owner_completion_is_discarded_after_orphan_recovery(self, cron_runtime, monkeypatch):
    claimed = {
        "id": "job-1",
        "name": "Job 1",
        "deliver": "local",
        "in_flight": {"run_id": "run-old"},
    }

    with patch("cron.scheduler._try_acquire_job_lock", return_value=MagicMock()), \
         patch("cron.scheduler._scheduler_lock", side_effect=_noop_scheduler_lock), \
         patch("cron.scheduler._release_lock_file"), \
         patch("cron.scheduler.mark_job_started", return_value=True), \
         patch("cron.scheduler.run_job", return_value=(True, "# output", "ok", None)), \
         patch("cron.scheduler.save_job_output"), \
         patch("cron.scheduler.finalize_job_run", return_value=False), \
         patch("cron.scheduler._deliver_result") as deliver_mock:
        result = scheduler._run_claimed_job(claimed, verbose=False)

    assert result is False
    deliver_mock.assert_not_called()
```

This is close to existing coverage, but the point here is to keep the orphan-reclaim mental model explicit.

**Step 2: Run only the stale/discard block**

Run:
```bash
source venv/bin/activate
python -m pytest tests/cron/test_scheduler.py -q -o addopts='' -k 'stale or orphan or discard'
```

Expected:
- PASS

---

## Task 9: Optional but recommended — tighten gateway shutdown observability

**Objective:** Improve operational clarity without changing correctness semantics.

**Files:**
- Modify: `gateway/run.py`
- Optional modify: `cron/scheduler.py`

**Step 1: Decide whether to wire in `shutdown_worker_pool()`**

Do **not** clear in-flight ownership on shutdown.

Acceptable scope for this task:
- add logging of active cron workers during shutdown, or
- call `shutdown_worker_pool(wait=False, cancel_futures=False)` after ticker stop purely to detach executor threads, if and only if you confirm it does not break tests or falsely signal completion

This is optional because it is adjacent hygiene, not the core fix.

**Step 2: If you touch shutdown behavior, add a minimal test or leave it out**

If you cannot add a robust test quickly, skip this task. Do not let shutdown experimentation muddy the core cron fix.

---

## Task 10: Run the full targeted suite and inspect diffs

**Objective:** Verify the fix without unrelated churn.

**Files:**
- No new production files expected beyond the edited cron/test files

**Step 1: Run targeted cron suites**

Run exactly:

```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts=''
python -m pytest tests/cron/test_scheduler.py -q -o addopts=''
python -m pytest tests/cron/ -q -o addopts=''
```

**Step 2: If any gateway-facing behavior changed, run one extra smoke test**

```bash
source venv/bin/activate
python -m pytest tests/test_cli_init.py -q -o addopts=''
```

**Step 3: Inspect the working tree**

Run:
```bash
git status --short
git diff -- cron/jobs.py cron/scheduler.py tests/cron/test_jobs.py tests/cron/test_scheduler.py gateway/run.py designs/2026-04-05-cron-orphaned-inflight-recovery-spec.md designs/2026-04-05-cron-orphaned-inflight-implementation-plan.md
```

Expected:
- only intended files changed
- no unrelated churn

---

## Task 11: Manual validation on a non-critical cron job

**Objective:** Prove restart recovery works in the real gateway process.

**Files:**
- No permanent code changes

**Step 1: Prepare a safe recurring local-delivery test job**

Use a trivial cron job that:
- runs every few minutes
- produces a short response
- has local or safe delivery

**Step 2: Confirm it is claimed**

Inspect `~/.hermes/cron/jobs.json` and confirm `in_flight` exists while it runs.

**Step 3: Restart/kill gateway during execution**

Use the real service manager for the environment.

**Step 4: Verify next tick behavior**

Expected after orphan grace:
- old in-flight claim is recovered
- job is reclaimable on next tick
- it does not sit blocked until full timeout

**Step 5: Verify no duplicate completion is delivered**

Look for:
- one valid new run
- old stale completion discarded
- explicit orphan recovery log line

---

## Acceptance checklist

Implementation is done only if all are true:

- [ ] `claim_due_jobs()` persists structured owner metadata for new claims
- [ ] `recover_stale_inflight()` supports early orphan recovery for dead/mismatched owners
- [ ] unknown/unsupported owner state falls back to timeout-only behavior
- [ ] legacy `owner_instance_id` records remain recoverable
- [ ] repeat accounting still routes through `_apply_run_outcome(...)`
- [ ] tick-level tests prove restart recovery works without duplicate dispatch
- [ ] targeted cron suites pass
- [ ] manual restart regression passes on a safe test job

---

## Suggested commit boundaries

Keep commits small and reviewable:

1. `test: add failing orphan recovery unit tests`
2. `feat: persist structured cron owner metadata`
3. `feat: recover orphaned in-flight cron jobs before timeout`
4. `test: add scheduler restart regression coverage`
5. `docs: add orphaned in-flight recovery plan`

---

## Notes for the implementer

- Do not overbuild a cross-platform process-inspection framework. Best-effort Linux support + conservative fallback is enough.
- Do not replace shared outcome logic with a new bespoke mutation path.
- Do not “fix” this by clearing claims at shutdown; that can create duplicate execution.
- The branch’s safety claim depends on this fix. Without it, every forced restart can freeze recurring work until timeout.
