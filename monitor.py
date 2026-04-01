"""EDINET Monitor - API監視サービス.

EDINET API v2を定期的にポーリングし、新着開示を検出する。
起動時に過去数日分のデータからEDINETコード→会社名マッピングを構築する。
"""
import json
import logging
import threading
import time
from datetime import datetime, date, timedelta
from typing import Callable, Optional

import requests

from models import Document
from classifier import Classifier
from storage import Storage

logger = logging.getLogger(__name__)

# コードリスト構築に使う過去日数
# 400日 = 約260営業日分で、年次報告書(有報)提出日もカバーし上場企業の大半を解決可能
CODELIST_WARMUP_DAYS = 400


class EdinetMonitor:
    """EDINET APIポーリング監視."""

    def __init__(
        self,
        config: dict,
        storage: Storage,
        classifier: Classifier,
        on_new_docs: Callable[[list[Document]], None],
        on_status_change: Callable[[str, str], None] = None,
    ):
        self.api_key = config.get("api_key", "")
        self.base_url = config.get("base_url", "https://api.edinet-fsa.go.jp/api/v2")
        self.polling_interval = config.get("polling_interval_sec", 60)
        self.request_timeout = config.get("request_timeout_sec", 30)
        self.max_retries = config.get("max_retries", 3)
        self.retry_interval = config.get("retry_interval_sec", 10)

        self.storage = storage
        self.classifier = classifier
        self.on_new_docs = on_new_docs
        self.on_status_change = on_status_change or (lambda s, m: None)

        self.watchlist = config.get("watchlist_sec_codes", [])

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_poll_time: Optional[str] = None
        self._consecutive_errors = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_poll_time(self) -> Optional[str]:
        return self._last_poll_time

    def start(self):
        """監視を開始."""
        if self._running:
            return
        if not self.api_key:
            self.on_status_change("error", "APIキーが未設定です。config.yamlを確認してください。")
            logger.error("APIキーが未設定")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.on_status_change("running", "監視開始")
        logger.info("監視開始 (間隔: %d秒)", self.polling_interval)

    def stop(self):
        """監視を停止."""
        self._running = False
        self.on_status_change("stopped", "監視停止")
        logger.info("監視停止")

    def poll_once(self) -> list[Document]:
        """1回だけポーリングを実行."""
        return self._poll()

    def _run_loop(self):
        """メインポーリングループ."""
        # コードリスト構築 (バックグラウンド)
        self._warmup_codelist()

        # 既存書類の対象会社名を再解決
        self._re_resolve_targets()

        # 初回ポーリング
        self._poll()

        while self._running:
            self._smart_sleep()
            if not self._running:
                break
            self._poll()

    def _smart_sleep(self):
        """時間帯に応じたスリープ.

        場中 (8:45-16:30): バーストポーリング (毎分55秒〜翌05秒に2秒間隔)
        場外: 通常間隔 (polling_interval)
        """
        now = datetime.now()
        hour_min = now.hour * 100 + now.minute  # e.g. 945 = 9:45

        # 場中判定 (平日 8:45-16:30)
        is_weekday = now.weekday() < 5
        is_market_hours = is_weekday and 845 <= hour_min <= 1630

        if not is_market_hours:
            # 場外: 通常間隔
            self._interruptible_sleep(self.polling_interval)
            return

        # 場中: 次の分の58秒まで待機 → バーストポーリング
        # :58, :00, :02, :04, :06 の5回 (前2秒 + 後6秒)
        sec = now.second
        if sec < 58:
            wait = 58 - sec
            self._interruptible_sleep(wait)
        for i in range(4):
            if not self._running:
                return
            time.sleep(2)
            self._poll()

    def _interruptible_sleep(self, seconds: int):
        """中断可能なスリープ."""
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def _warmup_codelist(self):
        """過去のAPIデータからEDINETコード→会社名マッピングを構築."""
        existing = self.storage.get_edinet_code_count()
        if existing > 7000:
            logger.info("コードリスト既に %d 件登録済み、ウォームアップスキップ", existing)
            return

        self.on_status_change("polling", "コードリスト構築中...")
        logger.info("コードリスト構築開始 (過去%d日)", CODELIST_WARMUP_DAYS)

        today = date.today()
        for days_ago in range(0, CODELIST_WARMUP_DAYS):
            if not self._running:
                break
            d = today - timedelta(days=days_ago)
            d_str = d.strftime("%Y-%m-%d")
            try:
                results = self._fetch_documents(d_str)
                codes = self._extract_codes(results)
                if codes:
                    self.storage.save_edinet_codes(codes)
                # 進捗表示 (10日ごと)
                if days_ago > 0 and days_ago % 10 == 0:
                    count = self.storage.get_edinet_code_count()
                    self.on_status_change("polling", f"コードリスト構築中... {count}件 ({days_ago}/{CODELIST_WARMUP_DAYS}日)")
            except Exception as e:
                logger.debug("コードリスト構築: %s スキップ (%s)", d_str, e)
            # レートリミット対策 (0.3秒間隔)
            if days_ago > 0:
                time.sleep(0.3)

        code_count = self.storage.get_edinet_code_count()
        logger.info("コードリスト構築完了: %d 件登録", code_count)
        self.on_status_change("running", f"コードリスト: {code_count}件")

    def _re_resolve_targets(self):
        """DB上の未解決対象会社名をコードリストから再解決."""
        import sqlite3
        conn = sqlite3.connect(self.storage.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("""
                SELECT doc_id, issuer_edinet_code, subject_edinet_code, subsidiary_edinet_code,
                       issuer_name, subject_name, subsidiary_name
                FROM documents
                WHERE (issuer_edinet_code IS NOT NULL AND issuer_edinet_code != '' AND (issuer_name IS NULL OR issuer_name = ''))
                   OR (subject_edinet_code IS NOT NULL AND subject_edinet_code != '' AND (subject_name IS NULL OR subject_name = ''))
                   OR (subsidiary_edinet_code IS NOT NULL AND subsidiary_edinet_code != '' AND (subsidiary_name IS NULL OR subsidiary_name = ''))
            """).fetchall()
            if not rows:
                return
            updated = 0
            for row in rows:
                doc_id = row["doc_id"]
                for code_col, name_col, sec_col in [
                    ("issuer_edinet_code", "issuer_name", "issuer_sec_code"),
                    ("subject_edinet_code", "subject_name", "subject_sec_code"),
                    ("subsidiary_edinet_code", "subsidiary_name", None),
                ]:
                    code = row[code_col] if code_col in row.keys() else None
                    name = row[name_col] if name_col in row.keys() else None
                    if code and not name:
                        result = self.storage.lookup_edinet_code(code)
                        if result:
                            if sec_col:
                                conn.execute(
                                    f"UPDATE documents SET {name_col} = ?, {sec_col} = ? WHERE doc_id = ?",
                                    (result[0], result[1] or "", doc_id),
                                )
                            else:
                                conn.execute(
                                    f"UPDATE documents SET {name_col} = ? WHERE doc_id = ?",
                                    (result[0], doc_id),
                                )
                            updated += 1
            conn.commit()
            if updated:
                logger.info("対象会社名を %d 件再解決", updated)
        finally:
            conn.close()

    def _extract_codes(self, results: list[dict]) -> list[tuple[str, str, Optional[str]]]:
        """APIレスポンスからEDINETコード→(会社名, 証券コード)を抽出."""
        codes = []
        seen = set()
        for item in results:
            ec = item.get("edinetCode")
            name = item.get("filerName", "")
            sec = item.get("secCode")
            if ec and name and ec not in seen:
                codes.append((ec, name, sec))
                seen.add(ec)
        return codes

    def _resolve_target_info(self, doc: Document):
        """対象会社のEDINETコードを会社名・証券コードに解決."""
        if doc.issuer_edinet_code:
            result = self.storage.lookup_edinet_code(doc.issuer_edinet_code)
            if result:
                doc.issuer_name, doc.issuer_sec_code = result[0], result[1] or ""
        if doc.subject_edinet_code:
            result = self.storage.lookup_edinet_code(doc.subject_edinet_code)
            if result:
                doc.subject_name, doc.subject_sec_code = result[0], result[1] or ""
        if doc.subsidiary_edinet_code:
            result = self.storage.lookup_edinet_code(doc.subsidiary_edinet_code)
            if result:
                doc.subsidiary_name = result[0]

    def _poll(self) -> list[Document]:
        """EDINET APIから書類一覧を取得し、新着を検出."""
        today_str = date.today().strftime("%Y-%m-%d")
        self.on_status_change("polling", f"取得中... ({today_str})")

        for attempt in range(1, self.max_retries + 1):
            try:
                results = self._fetch_documents(today_str)
                self._consecutive_errors = 0
                break
            except requests.exceptions.Timeout:
                logger.warning("APIタイムアウト (試行 %d/%d)", attempt, self.max_retries)
                self.storage.save_log("timeout", f"タイムアウト (試行 {attempt})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._handle_error("APIタイムアウト")
                    return []
            except requests.exceptions.ConnectionError:
                logger.warning("接続エラー (試行 %d/%d)", attempt, self.max_retries)
                self.storage.save_log("connection_error", f"接続エラー (試行 {attempt})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._handle_error("接続エラー")
                    return []
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else "不明"
                logger.error("HTTPエラー %s (試行 %d/%d)", status_code, attempt, self.max_retries)
                self.storage.save_log("http_error", f"HTTPエラー {status_code}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._handle_error(f"HTTPエラー {status_code}")
                    return []
            except Exception as e:
                logger.exception("予期しないエラー")
                self.storage.save_log("error", str(e))
                self._handle_error(str(e))
                return []

        # コードマッピングを更新
        codes = self._extract_codes(results)
        if codes:
            self.storage.save_edinet_codes(codes)

        # 新着書類を検出
        new_docs = []
        for item in results:
            doc_id = item.get("docID")
            if not doc_id or self.storage.doc_exists(doc_id):
                continue

            doc = self._parse_item(item)
            category, priority, tag = self.classifier.classify_with_tag(doc)

            if category == "その他":
                continue

            priority = self.classifier.adjust_priority_for_watchlist(
                priority, doc.sec_code or "", self.watchlist
            )

            doc.event_category = category
            doc.priority = priority
            doc.tag = tag
            doc.created_at = datetime.now().isoformat()

            # 対象会社名を解決
            self._resolve_target_info(doc)

            self.storage.save_document(doc)
            new_docs.append(doc)

        # ステータス更新
        now_str = datetime.now().strftime("%H:%M:%S")
        self._last_poll_time = now_str
        total_count = len(results)
        new_count = len(new_docs)

        status_msg = f"最終取得: {now_str} | 全{total_count}件中 新着{new_count}件"
        self.on_status_change("running", status_msg)
        self.storage.save_log("ok", status_msg, new_count)
        logger.info("取得完了: 全%d件, 新着%d件", total_count, new_count)

        if new_docs:
            self.on_new_docs(new_docs)

        return new_docs

    def _fetch_documents(self, date_str: str) -> list[dict]:
        """EDINET API から書類一覧を取得."""
        url = f"{self.base_url}/documents.json"
        params = {
            "date": date_str,
            "type": "2",
            "Subscription-Key": self.api_key,
        }
        response = requests.get(url, params=params, timeout=self.request_timeout)
        response.raise_for_status()

        data = response.json()
        metadata = data.get("metadata", {})
        status = metadata.get("status")
        if status and str(status) != "200":
            message = metadata.get("message", "不明なエラー")
            raise requests.exceptions.HTTPError(
                f"EDINET API エラー: {status} - {message}"
            )
        return data.get("results") or []

    def _parse_item(self, item: dict) -> Document:
        """APIレスポンスの1件をDocumentに変換."""
        return Document(
            doc_id=item.get("docID", ""),
            edinet_code=item.get("edinetCode", ""),
            sec_code=item.get("secCode"),
            filer_name=item.get("filerName", ""),
            doc_type_code=item.get("docTypeCode", ""),
            doc_description=item.get("docDescription", ""),
            submit_datetime=item.get("submitDateTime", ""),
            ordinance_code=item.get("ordinanceCode", ""),
            form_code=item.get("formCode", ""),
            issuer_edinet_code=item.get("issuerEdinetCode"),
            subject_edinet_code=item.get("subjectEdinetCode"),
            subsidiary_edinet_code=item.get("subsidiaryEdinetCode"),
            current_report_reason=item.get("currentReportReason"),
            raw_json=json.dumps(item, ensure_ascii=False),
            xbrl_flag=item.get("xbrlFlag") == "1",
            pdf_flag=item.get("pdfFlag") == "1",
            withdrawal_status=item.get("withdrawalStatus", "0"),
        )

    def _handle_error(self, message: str):
        """エラーハンドリング."""
        self._consecutive_errors += 1
        self.on_status_change("error", f"エラー: {message} (連続{self._consecutive_errors}回)")
        logger.error("連続エラー %d回: %s", self._consecutive_errors, message)

    def download_pdf(self, doc_id: str, save_path: str) -> bool:
        """書類のPDFをダウンロード."""
        url = f"{self.base_url}/documents/{doc_id}"
        params = {
            "type": "2",
            "Subscription-Key": self.api_key,
        }
        try:
            response = requests.get(url, params=params, timeout=60, stream=True)
            response.raise_for_status()
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info("PDF保存: %s -> %s", doc_id, save_path)
            return True
        except Exception as e:
            logger.error("PDFダウンロード失敗: %s - %s", doc_id, e)
            return False
