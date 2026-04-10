# Cron parallel scheduling: orphaned in-flight recovery

This document describes the design that ships with the parallel cron scheduler changes.

## Problem

Parallel cron execution persists `in_flight` ownership so different jobs can run concurrently without allowing the same job to overlap with itself.

That ownership model had a hole: if the gateway process died after claiming a job, the persisted claim could block the next gateway instance until the full cron timeout elapsed. With long cron timeouts, a dead owner could strand work for a long time.

## Implemented design

The branch fixes that by extending the ownership model instead of weakening it.

### 1. Structured owner metadata on claim

When a scheduler instance claims a job, it now persists:
- `owner_instance_id`
- `owner_pid`
- `owner_boot_id` when available
- `owner_process_start` when available

This keeps the old opaque instance id for compatibility while adding enough process identity to do conservative liveness checks.

### 2. Early orphan recovery in addition to timeout recovery

`recover_stale_inflight()` now has two recovery paths:
- normal timeout-based recovery when `timeout_at <= now`
- early orphan recovery when the recorded owner is definitely dead or its fingerprint no longer matches, after a short grace window

If owner liveness cannot be determined, recovery stays conservative and falls back to timeout-only behavior.

### 3. Legacy compatibility

Older persisted jobs may only have `owner_instance_id` in the `<pid>-<nonce>` format.

The implementation preserves compatibility by:
- parsing the legacy PID when possible
- allowing best-effort dead-owner recovery for that legacy shape
- falling back to timeout-only behavior when legacy metadata is malformed or insufficient

### 4. Shared failure accounting

Orphan recovery is treated as a failed run attempt through the same outcome machinery used by stale timeout recovery.

That preserves the important invariants:
- repeat accounting stays correct
- one-shot and repeat-limited jobs are finalized consistently
- stale completion results are discarded if the run no longer owns the claim
- `save output before finalize` behavior stays intact

## Why this is the right fix

The bug was not merely "parallelism is hard". The specific defect was timeout-only recovery in an ownership model that already depended on persisted claims to prevent duplicate dispatch.

Blindly clearing claims at shutdown would be the wrong fix because the old process may still have live worker threads. Releasing ownership during shutdown can create duplicate execution. Post-crash orphan detection is safer than eager shutdown clearing.

## Current runtime model

The scheduler still uses:
- a short global scheduler lock for metadata transitions
- per-job non-overlap locks
- execution outside the global lock
- recovery before claim on each scheduler tick

That ordering is intentional. Once an orphaned claim is recovered, the next tick can naturally reclaim the job.

## Test coverage kept with this change

The regression coverage on this branch verifies:
- dead-owner early recovery before full timeout
- live-owner no-op
- unknown-owner timeout fallback
- legacy owner metadata fallback
- repeat-limit correctness under orphan recovery
- scheduler-level reclaim after restart-like conditions
- grace-window behavior
- stale completion discard after ownership changes
- paused run-once behavior under the restored parallel scheduler model

## Operational notes

This change improves recovery after gateway restarts or forced termination, but it does not try to guarantee exactly-once execution. It is still a best-effort threaded scheduler.

If a future cleanup is wanted, the most reasonable follow-up would be shutdown hygiene and worker-pool observability, not weakening ownership semantics.
