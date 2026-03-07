# YuruQuant

Ride alone, trade slow, watch the sunset. A laid-back trading bot for A-Share & Futures. Sometimes we catch the trend (like catching a glimpse of Fuji). Sometimes we just sit by the fire and eat noodles. Profit is nice, but peace of mind is better.

## 量化策略重写版（仅保留策略思想）

本项目已按“全量清空后重写”方式实现，保留以下策略核心思想：

- 1h 趋势过滤
- 5m 严格缠论中枢突破
- 下一根确认执行
- 分层风控（硬止损 / 首段止盈 / 追踪止损 / 时间止损 / 组合DD）

## 运行

```powershell
# BACKTEST
conda run -n minner python main.py --mode BACKTEST --config config/strategy.yaml

# LIVE
conda run -n minner python main.py --mode LIVE --config config/strategy.yaml
```

当未提供 `gm.token` / `gm.strategy_id` 时，程序会自动进入本地仿真运行，确保可启动并产出报表。

## 目录

- `main.py`：程序入口
- `strategy/`：引擎、流水线、GM适配、报表
- `config/strategy.yaml`：单一配置文件
- `tests/`：烟测
- `docs/`：中文文档套件（含 `07_项目行为准则.md`）
