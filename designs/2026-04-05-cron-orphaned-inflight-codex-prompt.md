Goal:
- Implement orphaned in-flight cron recovery on the current branch so a gateway restart does not strand a claimed cron job until full timeout.

Context:
- Branch: `fix/cron-parallel-locking`
- Relevant design docs:
  - `designs/2026-04-05-cron-orphaned-inflight-recovery-spec.md`
  - `designs/2026-04-05-cron-orphaned-inflight-implementation-plan.md`
- Primary code files:
  - `cron/jobs.py`
  - `cron/scheduler.py`
  - `gateway/run.py`
- Primary test files:
  - `tests/cron/test_jobs.py`
  - `tests/cron/test_scheduler.py`
- Current bug:
  - `claim_due_jobs()` persists `in_flight` ownership metadata
  - `recover_stale_inflight()` only recovers once `timeout_at` expires
  - if the gateway restarts and the owning PID dies before `timeout_at`, the job stays blocked for the full timeout window
- Current branch already contains the parallel cron claim/non-overlap work; this fix is intended to complete that ownership model

Constraints:
- Follow the implementation plan and spec closely.
- Make the smallest high-confidence change that fixes orphaned in-flight recovery.
- Preserve current invariants:
  - short global scheduler lock for metadata transitions only
  - per-job non-overlap lock
  - save output before finalize
  - finalize only when `run_id` still owns the claim
  - stale/orphan recovery must continue to use shared outcome logic
- Do not add third-party dependencies.
- Prefer conservative Linux best-effort process identity checks with safe fallback to timeout-only behavior when liveness cannot be determined.
- Keep backward compatibility for legacy persisted jobs that only have `owner_instance_id`.
- Do not solve this by blindly clearing all in-flight claims at shutdown.
- Do not make broad unrelated refactors.
- Do not revert unrelated working tree changes.

Implementation requirements:
- Add structured owner metadata for new claims.
- Add owner-liveness/orphan detection helpers.
- Extend `recover_stale_inflight()` to recover dead/mismatched owners before full timeout, after a short orphan grace window.
- Keep timeout-based fallback for unknown/unsupported cases.
- Add regression tests in `tests/cron/test_jobs.py` and `tests/cron/test_scheduler.py` for:
  - dead-owner early recovery
  - live-owner no-op
  - unknown-owner timeout fallback
  - legacy owner metadata fallback
  - repeat-limit correctness under orphan recovery
  - tick-level orphan reclaim after restart-like conditions
  - grace-window behavior
  - stale completion discard after ownership changes

Done when:
- Code changes are implemented.
- Targeted cron tests pass.
- The diff is limited to the expected files.
- Final output explains root cause, changed files, validation run, and remaining risks.

Validation:
- Run exactly:
  - `source venv/bin/activate && python -m pytest tests/cron/test_jobs.py -q -o addopts=''`
  - `source venv/bin/activate && python -m pytest tests/cron/test_scheduler.py -q -o addopts=''`
  - `source venv/bin/activate && python -m pytest tests/cron/ -q -o addopts=''`
- If you touch gateway shutdown behavior, also run:
  - `source venv/bin/activate && python -m pytest tests/test_cli_init.py -q -o addopts=''`
- Inspect and summarize:
  - `git status --short`
  - `git diff --stat`

Output:
- Root cause
- Changed files
- Validation run
- Remaining risks / follow-ups
