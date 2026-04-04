# Live Trading Readiness Map

This document tracks what is required for safe live auto-trading and the current status in this workspace.

## Current Verdict

- Status: NOT 100% live-ready yet.
- Reason: Live preflight currently fails because required exchange credentials are not present in environment variables.

Observed result:

```bash
FAIL: missing required env vars: BINANCE_API_KEY, BINANCE_API_SECRET
```

## Verified Checks

### 1) Live config generation

- Command:

```bash
python3 scripts/prepare_live_config.py
```

- Result: PASS.
- Output config: `config/settings.live.yaml`.
- Confirmed behavior:
  - `system.paper_mode` set to `false`.
  - Any enabled exchange has `testnet` set to `false`.

### 2) Live preflight validation

- Command:

```bash
python3 scripts/preflight_live_trading.py
```

- Result: FAIL (missing required env vars).

Preflight rules validated by script:
- Live config file exists (`NT_CONFIG_PATH` or `config/settings.live.yaml`).
- `paper_mode` is `false`.
- At least one exchange is enabled.
- Enabled exchanges are not on testnet.
- Required exchange credentials are present in env vars.

## Blocking Items (Must Be Completed)

1. Set real exchange credentials in environment for enabled exchanges.
2. Re-run live preflight until it returns PASS.
3. Run guarded live startup path and verify health/telemetry after startup.

## Exact Completion Commands

```bash
# 1) Generate live config
python3 scripts/prepare_live_config.py

# 2) Export real credentials (example for Binance)
export BINANCE_API_KEY=your_real_key
export BINANCE_API_SECRET=your_real_secret

# 3) Point runtime to live config
export NT_CONFIG_PATH=config/settings.live.yaml

# 4) Must pass before any live run
python3 scripts/preflight_live_trading.py

# 5) Start live engine (guarded script includes preflight)
bash scripts/start_live_autotrading.sh
```

## Go/No-Go Rule

- GO only if preflight returns PASS and health endpoints remain stable after startup.
- NO-GO if any required credential is missing, any enabled exchange is testnet, or paper mode is active.
