# Quant Framework Scaffold

Fresh scaffold for a multi-platform quant framework.

Current boundary:

- `quantframe/`: framework only
- `strategies/`: strategy implementations only
- `resources/`: configs, universes, and instrument metadata only
- current supported platform: `GM`

Current default example:

- strategy factory: `strategies.trend.turtle_breakout:create_strategy`
- structure: signal model -> target allocator -> risk overlay -> execution planner
- strategy style: daily Turtle breakout with ATR-based sizing
- purpose: use a proven public trend-following template instead of a placeholder-only example

Run:

```powershell
C:\Users\wuktt\miniconda3\envs\minner\python.exe main.py --config resources\configs\gm_turtle_breakout.yaml
```

Next topics:

- GM continuous contract and roll mapping
- whether this Turtle baseline should stay daily or move to hourly
- order lifecycle and reverse-position behavior
- reporting contract
