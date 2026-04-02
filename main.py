"""EDINET Monitor - エントリポイント.

EDINET開示監視ローカルデスクトップアプリ。
起動: python main.py
"""
import logging
import sys
from pathlib import Path

from config import load_config, APP_DIR, PDF_CACHE_DIR
from storage import Storage
from classifier import Classifier
from monitor import EdinetMonitor
from notifier import Notifier
from gui import EdinetMonitorGUI


def setup_logging(config: dict):
    """ロギング設定."""
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", str(APP_DIR / "edinet_monitor.log"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    # 設定読み込み
    config = load_config()

    # ロギング
    setup_logging(config)
    logger = logging.getLogger(__name__)
    logger.info("EDINET Monitor 起動")

    # APIキーチェック
    api_key = config.get("edinet", {}).get("api_key", "")
    if not api_key:
        logger.error("APIキーが未設定です。config.yaml の edinet.api_key を設定してください。")
        print("\n[エラー] EDINET APIキーが未設定です。")
        print("config.yaml の edinet.api_key にキーを設定してください。")
        print(f"設定ファイル: {APP_DIR / 'config.yaml'}\n")

    # PDFキャッシュディレクトリ
    Path(PDF_CACHE_DIR).mkdir(parents=True, exist_ok=True)

    # ストレージ
    db_path = config.get("database", {}).get("path", str(APP_DIR / "edinet_monitor.db"))
    storage = Storage(db_path)

    # 分類器
    mon_cfg = config.get("monitoring", {})
    classifier = Classifier(
        enabled_categories=mon_cfg.get("enabled_categories"),
        skip_corrections=mon_cfg.get("skip_corrections", True),
    )

    # 通知
    notif_cfg = config.get("notification", {})
    notifier = Notifier(
        enabled=notif_cfg.get("enabled", True),
        sound=notif_cfg.get("sound", True),
        max_priority_to_notify=notif_cfg.get("max_priority_to_notify", 2),
    )

    # GUI (モニター依存のコールバックはGUIから注入)
    gui = EdinetMonitorGUI(config, storage, None, notifier)

    # モニター
    edinet_cfg = config.get("edinet", {})
    edinet_cfg["watchlist_sec_codes"] = mon_cfg.get("watchlist_sec_codes", [])
    monitor = EdinetMonitor(
        config=edinet_cfg,
        storage=storage,
        classifier=classifier,
        on_new_docs=gui.enqueue_new_docs,
        on_status_change=gui.enqueue_status,
    )

    # GUIにモニターを注入
    gui.monitor = monitor

    # 起動
    logger.info("GUI起動")
    gui.run()
    logger.info("EDINET Monitor 終了")


if __name__ == "__main__":
    main()
