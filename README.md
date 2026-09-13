# paldo-outbound-agent

A bounded, audit-first outbound **lead-generation pipeline** written in plain Python
(standard library + SQLite only). It discovers appointment-based local service
businesses from public sources, enriches and qualifies them against explicit
evidence, prepares review-ready outreach drafts, and hands every operator-facing
message to a small pluggable **notifier** instead of talking to a chat platform
directly.

Every stage is deterministic, fixture-injectable, and dry by default: the durable
system state starts `PAUSED`, discovery starts `DRY_RUN`, every message cap starts
`0`, and each stage refuses to write to the canonical database unless the caller
explicitly injects a non-canonical one. Nothing in this repository sends email,
sends a LinkedIn message, or contacts anyone.

> This repository is a **sanitized copy**. All routes, credentials, markets,
> regions, businesses, and case-study details are placeholders. See
> [Sanitization](#sanitization) below.

## Layout

Root modules are flat and numbered by pipeline stage.

| Module | Role |
| --- | --- |
| `paldo_os_outbound.py` | Storage foundation: schema, migrations, config/state, normalization, qualification primitives |
| `step3_discovery.py` | Discovery: candidate normalization, dedupe, Google Places / Apify adapters, ingestion |
| `step6_website_enrichment.py` | Public-website enrichment with injected HTTP/DNS clients |
| `step7_pipeline.py` | End-to-end fixture pipeline (discovery → qualification) |
| `step8_knowledge_context.py` | Gold knowledge context packets |
| `step9_audit_offer.py` | Offer versions, audit intake (rejects sensitive data), one-page deliverables |
| `step10_drafting.py` | Versioned drafting policy, draft generation and revision gates |
| `step11_gmail_drafts.py` | Gmail **draft** creation (fixture provider; never sends) |
| `step12_followup.py` | Follow-up cadence, manual send/reply recording, suppression |
| `step14_scheduler.py` | Bounded scheduling: shared daily capacity, checkpoints, recovery |
| `step15_identity_personalization.py` | Decision-maker identity and personalization packets |
| `step16_campaign_intelligence.py` | Per-campaign intelligence packs (vocabulary, concepts, proof assets) |
| `step17b_pilot_policy.py` | Versioned pilot message contract and validators |
| `step22_integration.py` | Bounded integration path and production-cycle commit boundary |
| `step23_expansion.py` | LinkedIn/social opportunity boundaries and manual match recording |
| `notifier.py` | Operator-facing notification delivery: `console` (default) and `json_file` backends |
| `paldo_recovery.py` | Commits one already-saved production handoff without rediscovery |
| `paldo_research_queue.py` | Daily research queue reservation/commit boundary |
| `paldo_revision.py` | Stores one experimental message revision for operator review |
| `reddit_*.py`, `paldo_social_handoff.py`, `paldo_triage_analysis.py` | Bounded public-opportunity collection/triage/handoff helpers |
| `paldo_production_entrypoint.py` | CLI for the production boundary (configure / run / pause / resume / stop) |

`tests/` holds the `unittest` suite for every stage.

## Requirements

* Python 3.11 or newer.
* No third-party packages. No network access is required to run the tests.

## Running the tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Current real result on this snapshot:

```
Ran 315 tests
FAILED (failures=2)
```

Note: the canonical-guard tests open `DEFAULT_DB_PATH`, so running the suite
creates an empty, git-ignored `data/paldo_os_outbound.sqlite3` (schema only,
no records). Delete it if you want a pristine tree.

The two failures are **pre-existing and time-dependent**, not caused by this
copy: the Step 22 fixture path asserts a generated draft is ready, but the
Step 10 drafting gate reports `EVIDENCE_EXPIRED` for fixture evidence whose
timestamps are fixed in the test, so the run is blocked before the draft exists.
They are listed under [Known issues](#known-issues).

## Configuration

Configuration is split in two places:

* **Environment** (`os.environ`, see `.env.example`) — routing, credentials,
  workspace paths, lane/market markers.
* **SQLite system config** (`paldo_os_outbound.DEFAULT_CONFIG`) — operational
  caps, feature flags, and system state, readable through `Database.read_config()`
  and validated on write.

`config.example.json` shows a neutral placeholder configuration for both, with a
generic "Local service businesses" niche and a generic region.

Safety defaults are deliberately conservative and the tests assert them:
`system_state=PAUSED`, `discovery_mode=DRY_RUN`, `daily_message_cap=0`,
`gmail_send_enabled=0`, `linkedin_sending_enabled=0`, `notifications_enabled=0`,
`notification_daily_cap=0`, `scheduler_enabled=0`.

## Notification delivery (`notifier.py`)

The pipeline never talks to a chat or messaging platform. Operator-facing
messages (outbound-queue review cards, alerts, recovery cards, Reddit
opportunity cards) are dispatched through `notifier.Notifier`, which has the
call shape the rest of the pipeline expects:

```python
from notifier import Notifier, queue_notification

notifier = Notifier()                       # console backend
notifier.send_message({"channel_id": 1001, "thread_id": 1002,
                       "text": "card text", "message_fingerprint": "abc"})
```

Two backends are provided:

* `console` (default) — prints a readable summary to stdout.
* `json_file` — appends one JSON object per notification to the JSON Lines path
  resolved from `PALDO_NOTIFICATION_JSONL_PATH` or the
  `notification_jsonl_path` system config value (`JsonFileBackend(path)`).

Delivery is durable and idempotent: `queue_notification()` writes an outbox row
before the backend is called, keys it by a message fingerprint so a retried
stage never double-delivers, records every attempt, and leaves an unknown
external outcome in a state that can only be resolved by
`reconcile_notification_outcome()` against operator-supplied evidence.

Channel identifiers and the operator identifier are configuration, never
constants: they resolve from `PALDO_NOTIFICATION_CHANNEL_ID`,
`PALDO_NOTIFICATION_OPERATOR_ID`, and `PALDO_NOTIFICATION_CHANNELS` (JSON) and
fall back to the obvious placeholders in `notifier.PLACEHOLDER_CHANNELS`.

## Safety posture

* **Fails closed.** Canonical-database writes, live discovery, real providers,
  and non-zero caps require explicit opt-in; otherwise every stage returns a
  blocked result with a machine-readable reason.
* **Fixture-injected providers.** Gmail, HTTP, DNS, search, and notification
  providers are injected. Tests exercise the full flow with fakes; the modules
  make no network calls.
* **Idempotent by fingerprint.** Leads, candidates, drafts, deliveries, and
  recovery operations are keyed so a re-run reuses instead of duplicating.
* **No secrets in artifacts.** Handoffs and audit rows reject credential-like
  keys and values; message text is screened for credential-like content.
* **Human review.** Drafts stop at `REVIEW_PENDING`; cycles stop at
  `AWAITING_OPERATOR_APPROVAL`. Automated sending is not implemented.

## Sanitization

This copy was produced from a private working repository. It contains no chat
integration: the original chat-control module was replaced by `notifier.py`, and
all chat ids, operator ids, tokens, owner emails, absolute paths, market names,
regions, businesses, and case-study details were removed or replaced with
obvious placeholders. Structural identifiers were renamed accordingly (for
example `notification_outbox`, `NOTIFICATION_CHANNELS`, `migration_version` of
the notifier surface). One test that asserted the state of the owner's private
operational database was removed, because that database is not part of this
repository.

## Known issues

1. `test_step22_integration.Step22IntegrationTests.test_bounded_local_and_us_paths_wire_writer_reviewer_gmail_and_notification_idempotently`
   and the follow-on `..._manual_send_reply_stop_opt_out_hard_bounce_and_authorized_notification_recording`
   fail on a draft-readiness assertion: the Step 10 gate reports
   `CAMPAIGN_EVIDENCE_REQUIRED / EVIDENCE_EXPIRED /
   PERSONALIZATION_EVIDENCE_REQUIRED / VERIFIED_PUBLIC_BUSINESS_EMAIL_REQUIRED`
   for the fixed-timestamp fixture evidence, so `run_bounded_pilot` returns
   `BLOCKED` with `blocking_reasons: [None]` (the gate forwards a `None`
   `last_error_category` instead of a fallback code). Both tests fail identically
   on the unmodified source snapshot.
2. `paldo_revision.py` keeps the identifiers of one historical proposal
   (`PROPOSAL_ID = 7`, `RECOVERY_RUN_ID`) because they are part of the module's
   contract; the run id now comes from `PALDO_RECOVERY_RUN_ID`.

## License / usage

No license file is included in this sanitized copy. Treat the code as
reference material: it is defensive by construction, but it is not a supported
product and it is not wired to any live account.
