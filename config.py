"""EDINET Monitor - 設定管理."""
import os
import yaml
from pathlib import Path

# アプリケーションのルートディレクトリ
APP_DIR = Path(__file__).parent
DEFAULT_CONFIG_PATH = APP_DIR / "config.yaml"
DEFAULT_DB_PATH = APP_DIR / "edinet_monitor.db"
DEFAULT_LOG_PATH = APP_DIR / "edinet_monitor.log"
PDF_CACHE_DIR = APP_DIR / "pdf_cache"


DEFAULT_CONFIG = {
    "edinet": {
        "api_key": "",
        "base_url": "https://api.edinet-fsa.go.jp/api/v2",
        "polling_interval_sec": 60,
        "request_timeout_sec": 30,
        "max_retries": 3,
        "retry_interval_sec": 10,
    },
    "monitoring": {
        "enabled_categories": [
            "大量保有報告書",
            "変更報告書",
            "増資",
            "CB/転換社債",
            "MSワラント",
            "第三者割当",
            "自己株取得",
            "公開買付",
            "主要株主異動",
        ],
        "watchlist_sec_codes": [],
        "skip_corrections": True,
    },
    "notification": {
        "enabled": True,
        "sound": True,
    },
    "database": {
        "path": str(DEFAULT_DB_PATH),
    },
    "logging": {
        "level": "INFO",
        "file": str(DEFAULT_LOG_PATH),
    },
    "gui": {
        "window_width": 1200,
        "window_height": 750,
    },
}


def load_config(path: str = None) -> dict:
    """設定ファイルを読み込む. 存在しなければデフォルトを生成."""
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH

    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            user_config = yaml.safe_load(f) or {}
        return _merge_config(DEFAULT_CONFIG, user_config)

    # デフォルト設定ファイルを生成
    save_config(DEFAULT_CONFIG, config_path)
    return DEFAULT_CONFIG.copy()


def save_config(config: dict, path: str = None):
    """設定ファイルを保存."""
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _merge_config(default: dict, override: dict) -> dict:
    """デフォルト設定にユーザー設定をマージ."""
    result = default.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_config(result[key], value)
        else:
            result[key] = value
    return result
