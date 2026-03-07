# YuruQuant

GM-only futures strategy runtime with a strict typed config contract and Polars-first data pipeline.

Ride alone, trade slow, watch the sunset.

## Install

```powershell
python -m pip install -r requirements.txt
```

## Run

```powershell
# BACKTEST
python main.py --mode BACKTEST --config config/strategy.yaml

# LIVE
python main.py --mode LIVE --config config/strategy.yaml
```

## Runtime Path

`main.py -> adapters.gm.callbacks -> core.engine -> pipelines(entry/risk) -> adapters.gm.orders`

## Highlights

- Strict YAML contract (`strategy/config/validator.py`) with hard failure on unknown/removed fields
- Single action set: `none|buy|sell|close_long|close_short`
- Polars-only kline and indicator pipeline
- Incremental symbol bar buffers (no per-bar full-history fetch)
- Risk flow: `hard stop -> break-even -> trailing -> dynamic -> time stop`

## Config

Use `config/strategy.yaml` and provide credentials via:

- `gm.token` / `gm.strategy_id` in YAML, or
- `GM_TOKEN` / `GM_STRATEGY_ID` environment variables

## Tests

```powershell
# unit tests
python -m unittest discover -s tests/unit -p "test_*.py"

# performance benchmark
python tests/perf/benchmark_on_bar.py
```

## Notes

- Local replay is removed (`GM_FORCE_LOCAL` is rejected).
- `runtime.mode` must be `BACKTEST` or `LIVE`.
- Config validation is strict and backward-incompatible by design.
