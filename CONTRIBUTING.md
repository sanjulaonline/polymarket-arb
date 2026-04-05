# Contributing to polymarket-arb

Thanks for your interest in contributing.

## Development setup

1. Fork the repository and clone your fork.
2. Install Rust stable (1.78+ recommended).
3. Copy `.env.example` to `.env` and fill safe local values.
4. Build and test:

```bash
cargo check
cargo test
```

## Project layout

- `src/` runtime bot code
- `src/feeds/` external market/oracle streams
- `src/polymarket/` CLOB API client and poller
- `docs/` architecture and project docs

## Contribution workflow

1. Create a branch from `main`.
2. Keep changes focused and atomic.
3. Add or update tests when behavior changes.
4. Run formatting and lint checks before opening a PR:

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Pull request checklist

- Code builds and tests pass locally.
- New config fields are documented in `.env.example` and `README.md`.
- Breaking changes are clearly described.
- PR description explains why the change is needed.

## Commit message guidance

Use clear, action-oriented messages.

Examples:
- `feat(detector): add oracle staleness gate`
- `fix(proxy): handle stalled ws reconnect`
- `docs: add contribution and security policies`

## Reporting bugs

Use the bug report template in `.github/ISSUE_TEMPLATE/bug_report.yml`.
Include logs, OS, Rust version, and steps to reproduce.
