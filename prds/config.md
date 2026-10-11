# Norman Config

## Session Limits
max_tasks_per_session: 15
warn_at_tasks: 12

## Project
name: Trail
repo: https://github.com/open-wander/trail
created: 2026-02-09

## Commands (auto-detect from project)
build: go build -o ./bin/trail ./cmd/trail
test: go test ./...
lint: go vet ./... && gosec ./...

## Subagent Defaults
default_subagent: general-purpose

## Model Strategy
advisor_mode: always        # always | auto | never — controls advisory review
advisor_model: opus
default_model: sonnet
quick_model: haiku
progress_compress_after: 10
verify_rigor: single        # single | adversarial — Mode 5 verification depth
security_max_reqs_per_prd: 8  # Mode 10 — requirements verified per PRD; extras are logged, not dropped silently

## Cleanup
sweep_mode: offer           # off | offer | auto — post-phase sweep (Mode 9)
