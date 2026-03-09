from __future__ import annotations

from typing import Any


DEFAULTS: dict[str, Any] = {
    'runtime': {'mode': 'BACKTEST', 'run_id': 'run_001'},
    'broker': {
        'gm': {
            'token': '',
            'strategy_id': '',
            'serv_addr': '',
            'backtest': {
                'start': '2025-08-20 00:00:00',
                'end': '2026-02-13 15:00:00',
                'max_days': 180,
                'initial_cash': 500000.0,
                'match_mode': 0,
                'intraday': False,
            },
            'subscribe_wait_group': True,
            'wait_group_timeout': 10,
        }
    },
    'universe': {
        'symbols': [],
        'entry_frequency': '300s',
        'trend_frequency': '3600s',
        'warmup': {'entry_bars': 180, 'trend_bars': 120},
        'instrument_defaults': {
            'multiplier': 10.0,
            'min_tick': 1.0,
            'min_lot': 1,
            'lot_step': 1,
            'sessions': {
                'day': [['09:00', '11:30'], ['13:30', '15:00']],
                'night': [['21:00', '23:00']],
            },
        },
        'instrument_overrides': {},
        'risk_clusters': {},
    },
    'strategy': {
        'environment': {
            'mode': 'ma_macd',
            'ma_period': 60,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'tsmom_lookbacks': [24, 48, 96],
            'tsmom_min_agree': 2,
        },
        'entry': {
            'donchian_lookback': 36,
            'min_channel_width_atr': 0.5,
            'breakout_atr_buffer': 0.30,
            'session_end_buffer_bars': 0,
            'entry_block_major_gap_bars': 0,
        },
        'exit': {
            'hard_stop_atr': 2.2,
            'protected_activate_r': 1.2,
            'armed_flush_buffer_bars': 0,
            'armed_flush_min_gap_minutes': 180,
            'session_flat_all_phases_buffer_bars': 0,
            'session_flat_scope': 'disabled',
        },
    },
    'portfolio': {
        'risk_per_trade_ratio': 0.015,
        'max_total_armed_risk_ratio': 0.0,
        'max_cluster_armed_risk_ratio': 0.0,
        'max_same_direction_cluster_positions': 0,
        'max_daily_loss_ratio': 0.05,
        'max_drawdown_halt_ratio': 0.15,
        'drawdown_halt_mode': 'hard',
        'drawdown_risk_schedule': [],
    },
    'execution': {
        'fill_policy': 'next_bar_open',
        'backtest_commission_ratio': 0.001,
        'backtest_slippage_ratio': 0.002,
    },
    'reporting': {
        'enabled': True,
        'output_dir': 'reports',
        'signals_filename': 'signals.csv',
        'executions_filename': 'executions.csv',
        'portfolio_daily_filename': 'portfolio_daily.csv',
    },
    'observability': {'level': 'WARN', 'sample_every_n': 50},
}

TOP_LEVEL_KEYS = {'runtime', 'broker', 'universe', 'strategy', 'portfolio', 'execution', 'reporting', 'observability'}
RUNTIME_KEYS = {'mode', 'run_id'}
BROKER_KEYS = {'gm'}
GM_KEYS = {'token', 'strategy_id', 'serv_addr', 'backtest', 'subscribe_wait_group', 'wait_group_timeout'}
BACKTEST_KEYS = {'start', 'end', 'max_days', 'initial_cash', 'match_mode', 'intraday'}
UNIVERSE_KEYS = {'symbols', 'entry_frequency', 'trend_frequency', 'warmup', 'instrument_defaults', 'instrument_overrides', 'risk_clusters'}
WARMUP_KEYS = {'entry_bars', 'trend_bars'}
INSTRUMENT_KEYS = {'multiplier', 'min_tick', 'min_lot', 'lot_step', 'sessions'}
SESSIONS_KEYS = {'day', 'night'}
STRATEGY_KEYS = {'environment', 'entry', 'exit'}
ENVIRONMENT_KEYS = {'mode', 'ma_period', 'macd_fast', 'macd_slow', 'macd_signal', 'tsmom_lookbacks', 'tsmom_min_agree'}
ENTRY_KEYS = {'donchian_lookback', 'min_channel_width_atr', 'breakout_atr_buffer', 'session_end_buffer_bars', 'entry_block_major_gap_bars'}
EXIT_KEYS = {'hard_stop_atr', 'protected_activate_r', 'armed_flush_buffer_bars', 'armed_flush_min_gap_minutes', 'session_flat_all_phases_buffer_bars', 'session_flat_scope'}
PORTFOLIO_KEYS = {
    'risk_per_trade_ratio',
    'max_total_armed_risk_ratio',
    'max_cluster_armed_risk_ratio',
    'max_same_direction_cluster_positions',
    'max_daily_loss_ratio',
    'max_drawdown_halt_ratio',
    'drawdown_halt_mode',
    'drawdown_risk_schedule',
}
EXECUTION_KEYS = {'fill_policy', 'backtest_commission_ratio', 'backtest_slippage_ratio'}
REPORTING_KEYS = {'enabled', 'output_dir', 'signals_filename', 'executions_filename', 'portfolio_daily_filename'}
OBSERVABILITY_KEYS = {'level', 'sample_every_n'}

__all__ = [
    'BACKTEST_KEYS',
    'BROKER_KEYS',
    'DEFAULTS',
    'ENTRY_KEYS',
    'ENVIRONMENT_KEYS',
    'EXECUTION_KEYS',
    'EXIT_KEYS',
    'GM_KEYS',
    'INSTRUMENT_KEYS',
    'OBSERVABILITY_KEYS',
    'PORTFOLIO_KEYS',
    'REPORTING_KEYS',
    'RUNTIME_KEYS',
    'SESSIONS_KEYS',
    'STRATEGY_KEYS',
    'TOP_LEVEL_KEYS',
    'UNIVERSE_KEYS',
    'WARMUP_KEYS',
]
