# 08 AKShare 数据准备

## 目标

- 将 AKShare 相关代码独立在项目根目录 `akshare_adapter/`。
- 先做离线数据准备，不改现有 GM 回测执行路径。

## 安装

```powershell
conda run -n minner pip install akshare
```

## 下载到本地

```powershell
conda run -n minner python scripts/prepare_akshare_data.py --symbols CZCE.ap,CFFEX.IC,SHFE.rb --freqs 300s,3600s --out-dir data/akshare --sleep 1.0 --start "2025-08-20 00:00:00" --end "2026-02-13 15:00:00"
```

## 输出目录

- `data/akshare/CZCE.ap/300s.csv`
- `data/akshare/CZCE.ap/3600s.csv`
- `data/akshare/CZCE.ap/1d.csv`
- 其余品种同样结构。

## 说明

- 运行时符号格式采用你当前项目格式（如 `CZCE.ap`、`CFFEX.IC`）。
- 脚本内部会转换为 AKShare/Sina 连续符号格式（如 `AP0`、`IC0`）。
- 输出列统一为：`eob/open/high/low/close/volume`，便于后续接入策略引擎。
- `--start/--end` 可选；不传则保存该接口返回的全量可得区间。

## 用本地 AKShare 数据跑回测

- 配置文件中的 `runtime.symbols`、`runtime.freq_5m`、`runtime.freq_1h` 要和下载时一致。
- 回测时间使用 `gm.backtest_start` 与 `gm.backtest_end`。
- 运行时设置环境变量 `LOCAL_DATA_ROOT` 指向缓存目录，并强制本地模式：

```powershell
$env:GM_FORCE_LOCAL="1"
$env:LOCAL_DATA_ROOT="data/akshare"
conda run -n minner python main.py --mode BACKTEST --config config/strategy.yaml
```

- 本地回测读取路径规则：
  - `data/akshare/<csymbol>/<freq>.csv`
  - 例如 `data/akshare/CZCE.ap/300s.csv`
  - 例如 `data/akshare/CZCE.ap/3600s.csv`
