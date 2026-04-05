# Project Structure

## Root

- `Cargo.toml`: crate metadata and dependencies
- `README.md`: setup and usage
- `.env.example`: runtime configuration template
- `LICENSE`: MIT license
- `CONTRIBUTING.md`: contribution workflow
- `CODE_OF_CONDUCT.md`: community standards
- `SECURITY.md`: vulnerability reporting process

## Source

- `src/main.rs`: app entrypoint
- `src/config.rs`: configuration parsing
- `src/detector.rs`: trading signal logic
- `src/feeds/`: market/oracle stream handlers
- `src/polymarket/`: API client and poller

## Docs and community

- `docs/`: architecture and project docs
- `.github/`: issue templates, PR template, CI workflow
