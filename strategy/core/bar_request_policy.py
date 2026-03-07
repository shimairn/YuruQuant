from __future__ import annotations


def normalize_freq_alias(value: object) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "5m": "5m",
        "5min": "5m",
        "300s": "5m",
        "1h": "1h",
        "60m": "1h",
        "3600s": "1h",
    }
    return aliases.get(raw, raw)


def resolve_req_1h_count(cfg) -> int:
    return max(int(cfg.runtime.warmup_1h), 1)


def has_enough_warmup(df_5m, df_1h, req_5m: int, req_1h: int) -> tuple[bool, int, int]:
    min_5m = max(int(req_5m), 1)
    min_1h = max(int(req_1h), 1)
    return len(df_5m) >= min_5m and len(df_1h) >= min_1h, min_5m, min_1h
