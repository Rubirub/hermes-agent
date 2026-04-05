# Cron orphaned in-flight recovery spec

> For Hermes: treat this as a design/spec doc for the cron parallelism branch, not an implementation transcript.

## Goal

Prevent a gateway restart from stranding claimed cron jobs for the full cron timeout window.

## Recommendation

Yes: this fix belongs on the parallel cron branch.

This is not a random operational paper cut. It is directly in the `in_flight` ownership model introduced for safe parallel claiming and non-overlap. The branch is not production-safe without a dead-owner recovery path.

---

## Incident that motivated this spec

Observed on VPS after restarting the gateway:

- `Regime Opportunity Lab` remained `enabled=true`, `state=scheduled`
- but `jobs.json` still showed `in_flight.status="running"`
- `owner_instance_id` pointed to the old dead gateway PID
- `timeout_at` was still in the future
- scheduler would not reclaim the job until the full timeout elapsed

With `HERMES_CRON_TIMEOUT=7200`, that meant the job stayed blocked for about 2 hours.

We have already reduced VPS runtime config to 1 hour (`HERMES_CRON_TIMEOUT=3600`), but that only reduces blast radius. It does not fix the bug.

---

## Current code behavior

### Claim path

In `cron/jobs.py`, `claim_due_jobs()` writes in-flight ownership:

- `run_id`
- `owner_instance_id`
- `claimed_at`
- `timeout_at`
- `started_at`
- `status`

Relevant code:
- `cron/jobs.py:773-831`
- ownership payload write at `cron/jobs.py:809-816`

Current owner identity is opaque:
- `owner_instance_id` is passed from scheduler
- scheduler builds it as `_INSTANCE_ID = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"`
- see `cron/scheduler.py:64`

### Recovery path

`recover_stale_inflight()` only recovers when `timeout_at <= now`.

Relevant code:
- `cron/jobs.py:720-770`
- critical gate: `cron/jobs.py:738-740`

Today it does **not** check whether the recorded owner process is dead.

### Tick path

Each scheduler tick does:
1. `recover_stale_inflight(now=now)`
2. `claim_due_jobs(...)`

Relevant code:
- `cron/scheduler.py:794-805`

So if a job has a future `timeout_at`, the new gateway instance will skip it even when the old owner is already gone.

### Gateway shutdown path

The gateway starts a cron ticker thread in `gateway/run.py:6457-6466` and stops that ticker in `gateway/run.py:6476-6478`.

However:
- the cron worker executor is not explicitly integrated into gateway shutdown
- a running cron worker can keep the process alive until systemd escalates to SIGKILL
- on forced kill, claimed jobs remain persisted as in-flight

Relevant code:
- ticker start/stop: `gateway/run.py:6457-6478`
- worker pool exists in `cron/scheduler.py:66-178`
- no gateway-side `shutdown_worker_pool()` call was found

### Existing tests

The suite already covers timeout-based stale recovery, but only after `timeout_at` expires:
- `tests/cron/test_jobs.py:541-587`

The suite does **not** cover:
- dead owner before `timeout_at`
- gateway restart with orphaned in-flight ownership
- immediate reclaim of a recurring job after owner death

---

## Root cause

The bug is not “parallelism” in the abstract.

The root cause is more specific:

1. parallel-safe claiming persists `in_flight` ownership eagerly
2. ownership is used to suppress duplicate dispatch
3. recovery is timeout-only
4. the code does not treat “owner process is dead” as a recoverable terminal state

That means a restart or SIGKILL can leave a job blocked until full timeout expiry.

This is a design hole in the branch’s ownership model.

---

## Goals

1. Reclaim orphaned in-flight jobs shortly after owner death, without waiting for full cron timeout.
2. Preserve current anti-double-fire semantics for live owners.
3. Preserve repeat accounting and finalization semantics by continuing to use shared run outcome logic.
4. Keep backward compatibility for existing persisted jobs where possible.
5. Add restart-oriented tests, not just happy-path concurrency tests.

## Non-goals

1. Do not promise exactly-once execution.
2. Do not redesign cron workers into subprocesses in this change.
3. Do not clear claims blindly at gateway shutdown; that can create duplicate execution if the old worker is still actually running.
4. Do not solve generic hung-worker detection beyond the ownership/orphan problem unless it falls out cleanly from the same design.

---

## Required design change

## 1) Add structured owner identity to in-flight claims

Current `owner_instance_id` is useful for uniqueness but weak for liveness checks.

Extend persisted `in_flight` metadata to include structured owner fields in addition to the existing string:

```json
"in_flight": {
  "run_id": "...",
  "owner_instance_id": "2298108-ab12cd34",
  "owner_pid": 2298108,
  "owner_boot_id": "...",          // optional, best-effort
  "owner_process_start": "...",    // optional, best-effort fingerprint
  "claimed_at": "...",
  "timeout_at": "...",
  "started_at": null,
  "status": "claimed"
}
```

### Why

- `owner_pid` supports basic liveness checks.
- `owner_boot_id` and/or `owner_process_start` reduce PID-reuse false positives.
- keeping `owner_instance_id` preserves backward compatibility and existing diagnostics.

### File impact

- `cron/scheduler.py`
- `cron/jobs.py`

### Recommended helper shape

In `cron/scheduler.py`, add helpers that expose the current scheduler instance fingerprint:
- current PID
- best-effort boot id on Linux
- best-effort process start fingerprint on Linux

In `cron/jobs.py`, store those fields when claiming.

### Portability rule

Use best-effort OS-specific data:
- Linux: `/proc` and `/proc/sys/kernel/random/boot_id`
- non-Linux / unsupported environments: fall back conservatively

If robust owner fingerprint data is unavailable, keep current timeout-based recovery behavior instead of guessing aggressively.

---

## 2) Extend stale recovery into orphan recovery

Replace the effective rule:

- “recover only when `timeout_at` has passed”

with:

- recover when `timeout_at` has passed, OR
- recover earlier when the recorded owner is definitely dead/replaced and an orphan grace period has elapsed

### Proposed decision model

For each job with `in_flight`:

1. If `timeout_at <= now`:
   - recover exactly as today using shared outcome logic.

2. Else if owner liveness cannot be determined:
   - do not recover early
   - keep timeout-only behavior.

3. Else if owner is definitely still alive and fingerprint matches:
   - do not recover.

4. Else if owner is definitely dead or PID fingerprint mismatches:
   - if `now - claimed_at < ORPHAN_RECOVERY_GRACE_SECONDS`, do not recover yet
   - otherwise recover immediately using shared outcome logic.

### Recommended grace

Add a short orphan grace, something like:
- 30s minimum
- 60s recommended default

Purpose:
- avoid reclaiming during restart handoff jitter
- avoid racing against transient startup/reload windows

### Important nuance

Do **not** reclaim based only on “PID is different from current gateway PID”.
That is insufficient in multi-process environments and not what we actually mean.
The criterion is:
- the recorded owner process is not alive anymore, or
- the recorded process identity no longer matches the stored fingerprint.

### Outcome handling

Early orphan recovery must still route through `_apply_run_outcome(...)` exactly like timeout-based recovery.

Reason strings should be explicit, e.g.:
- `stale_recovered: run_id=...`
- `orphan_recovered: owner_dead run_id=... owner_instance_id=...`
- `orphan_recovered: owner_reused run_id=... owner_instance_id=...`

### File impact

Primary:
- `cron/jobs.py`

Possible helper placement:
- either small liveness helpers in `cron/jobs.py`
- or shared helpers in `cron/scheduler.py` / a small cron utility module

My preference: keep persistence + recovery decisions in `cron/jobs.py`, but isolate OS/process inspection behind a tiny helper layer so the recovery function stays readable.

---

## 3) Keep backward compatibility for already-persisted jobs

Existing `jobs.json` records only have `owner_instance_id`.

The recovery logic should handle both shapes:

### New-format jobs
Use structured fields:
- `owner_pid`
- `owner_boot_id`
- `owner_process_start`

### Legacy jobs
Fallback behavior:
1. try to parse PID from `owner_instance_id` prefix (`<pid>-<nonce>`)
2. if parse succeeds, allow best-effort dead-PID recovery
3. if parse fails, fall back to timeout-only recovery

This ensures live systems upgrade cleanly without requiring a jobs.json migration first.

---

## 4) Logging must become explicit

Today this failure mode is too silent operationally.

Add explicit logs for these cases:

1. claimed job skipped because owner still alive
2. orphan recovered because owner PID is gone
3. orphan recovered because owner fingerprint mismatch implies PID reuse/replacement
4. timeout-based stale recovery
5. legacy in-flight entry could not be owner-checked, using timeout-only fallback

This should appear in scheduler logs with job id and run id.

---

## 5) Do not make gateway shutdown responsible for correctness

It is tempting to “fix” this by clearing all owned in-flight jobs during gateway shutdown.
Do not make that the primary solution.

Why this is unsafe:
- if the old process is still running a cron worker thread while shutting down
- and the shutdown path clears ownership too early
- the new gateway can reclaim the same job while the old one is still running
- duplicate execution becomes possible

Given the current thread-based worker model, shutdown is not a trustworthy ownership-release point.

### Therefore

The correctness fix should be post-crash/post-kill orphan detection, not eager shutdown release.

### Optional follow-up

A separate follow-up can improve shutdown behavior and observability, but it should not be the main fix for this bug.

---

## Optional follow-up: shutdown hygiene

This is adjacent and likely worth its own follow-up issue:

- gateway currently stops the cron ticker thread
- but does not appear to coordinate cron worker pool shutdown with ownership semantics
- long-running cron workers may keep the process alive until systemd SIGKILLs it

Potential follow-up areas:
- explicit `shutdown_worker_pool()` integration in gateway shutdown
- clearer logging when shutdown is waiting on active cron workers
- longer-term consideration of subprocess-based cron workers if interruptibility becomes a real requirement

This follow-up should not block the orphan-recovery fix.

---

## Proposed file changes

## `cron/jobs.py`

### Add helpers

Add small helpers for:
- parsing legacy `owner_instance_id`
- reading process liveness/fingerprint best-effort
- deciding whether an in-flight owner is:
  - alive
  - dead
  - unknown
  - mismatched / reused

Suggested conceptual helpers:

- `_parse_owner_pid(in_flight) -> Optional[int]`
- `_read_boot_id() -> Optional[str]`
- `_read_process_fingerprint(pid: int) -> Optional[dict]`
- `_owner_state(in_flight) -> Literal["alive", "dead", "mismatch", "unknown"]`
- `_orphan_grace_seconds() -> float`

### Extend claim payload

In `claim_due_jobs()`, write structured owner fields when creating `in_flight`.

### Extend recovery

Refactor `recover_stale_inflight()` so it:
- preserves current timeout path
- adds the new owner-dead path
- keeps all final accounting through `_apply_run_outcome(...)`

## `cron/scheduler.py`

### Provide current instance metadata

If needed, add small helpers/constants for current process metadata used when claiming.

### No behavior change in tick ordering

Keep:
1. recover
2. claim

That order is correct.
Once orphan recovery exists, the next tick after a dead owner should reclaim the job naturally.

## `tests/cron/test_jobs.py`

Add unit coverage for owner-dead recovery decisions and accounting.

## `tests/cron/test_scheduler.py`

Add integration-style scheduler tests that prove restart recovery works in a tick-driven flow.

---

## Required tests to add

Below is the minimum acceptable regression suite.

## A. `tests/cron/test_jobs.py`

### 1) orphaned in-flight is recovered before timeout when owner is dead

Setup:
- create recurring job
- mark due
- claim it with owner metadata
- set `timeout_at` far in the future
- monkeypatch owner-state helper to return `dead`
- advance clock only past orphan grace, not full timeout

Assert:
- `recover_stale_inflight()` returns 1
- `in_flight` becomes `None`
- `last_status == "error"`
- `last_error` contains `orphan_recovered`
- `repeat.completed` increments

### 2) live owner is not recovered before timeout

Setup same as above, but owner-state returns `alive`.

Assert:
- `recover_stale_inflight()` returns 0
- `in_flight` remains present
- no status mutation occurs

### 3) unknown owner state falls back to timeout-only

Setup:
- future `timeout_at`
- owner-state returns `unknown`

Assert:
- no early recovery occurs

Then advance past `timeout_at` and assert normal stale recovery still happens.

### 4) legacy `owner_instance_id` PID parsing works

Setup:
- persisted in-flight has only `owner_instance_id="12345-abcd"`
- no structured owner fields
- helper simulates PID dead

Assert:
- early orphan recovery can still happen for legacy records

### 5) malformed legacy owner id does not guess

Setup:
- `owner_instance_id="weird-format"`
- no structured metadata

Assert:
- early recovery does not happen
- timeout-based recovery still works later

### 6) orphan recovery respects repeat limit

Mirror existing stale-repeat-limit test, but using dead-owner early recovery.

Assert:
- one-shot / repeat-limited semantics stay correct
- no extra runs slip through

### 7) PID reuse / fingerprint mismatch recovers as orphan

Only if structured fingerprinting is implemented.

Setup:
- owner PID exists but fingerprint mismatch indicates different process

Assert:
- early recovery occurs with explicit mismatch reason

## B. `tests/cron/test_scheduler.py`

### 8) tick recovers orphan and claims same job in same tick

Setup:
- persisted job is due
- has future `timeout_at`
- has in-flight record owned by dead process
- owner-state helper returns `dead`
- patch `run_job()` to return quickly

Assert:
- one `tick()` call recovers old in-flight and dispatches the job
- final state shows a new successful run

This is the most important restart regression test.

### 9) tick does not reclaim when owner is alive

Setup same as above, but owner-state returns `alive`.

Assert:
- `tick()` dispatches 0 new jobs
- in-flight ownership remains unchanged

### 10) legacy orphaned in-flight can be reclaimed on tick

Setup uses only legacy `owner_instance_id`.

Assert:
- scheduler still recovers and dispatches correctly

### 11) orphan grace prevents immediate reclaim during handoff

Setup:
- owner-state returns `dead`
- `claimed_at` is very recent, inside grace

Assert:
- first tick dispatches 0
- after grace elapses, next tick dispatches 1

This protects against over-eager recovery.

### 12) recovered orphan does not deliver stale completion from old run

This complements existing stale-finalize coverage.

Model the sequence:
- old run loses ownership / is recovered
- new run is claimed
- old run eventually tries to finalize

Assert:
- old finalize is discarded
- only the new owner’s completion is deliverable

---

## Acceptance criteria

This fix is complete only when all are true:

1. Restarting or killing the gateway during a cron run does not strand that job until full timeout if the owner process is gone.
2. A replacement gateway instance can reclaim an orphaned recurring job on the next tick after the orphan grace window.
3. Live-owner in-flight jobs are still protected from duplicate dispatch.
4. Repeat accounting remains correct for orphan recovery.
5. Legacy persisted jobs remain recoverable without a mandatory migration.
6. New tests cover both timeout-based and owner-death-based recovery.

---

## Rollout / validation plan

After implementation:

```bash
source venv/bin/activate
python -m pytest tests/cron/test_jobs.py -q -o addopts=''
python -m pytest tests/cron/test_scheduler.py -q -o addopts=''
python -m pytest tests/cron/ -q -o addopts=''
```

Then do a manual restart regression on a non-critical cron job:
1. schedule a long-ish recurring local-delivery test job
2. confirm it enters `in_flight`
3. restart/kill the gateway during execution
4. verify new gateway recovers the orphan on the next tick after grace
5. verify the job is not blocked until full timeout

---

## My recommendation

Implement this on the parallel branch before treating the branch as operationally trustworthy.

Reason:
- the branch’s core value is safe concurrent cron execution
- persisted ownership is part of that design
- dead-owner reclaim is a required half of persisted ownership
- without it, every forced restart can freeze jobs for the full timeout window

That is not acceptable behavior for the architecture Reuven wants.
