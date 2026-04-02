"""EDINET Monitor - polling service."""
import json
import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import Callable, Optional

import requests

from classifier import Classifier
from models import Document
from storage import Storage

logger = logging.getLogger(__name__)

CODELIST_WARMUP_DAYS = 400


class EdinetMonitor:
    """Polls the EDINET API and emits newly recognized documents."""

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
        if self._running:
            return
        if not self.api_key:
            self.on_status_change("error", "API key is not configured. Please update config.yaml.")
            logger.error("API key is not configured")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.on_status_change("running", "Monitoring started")
        logger.info("Monitoring started (interval: %d sec)", self.polling_interval)

    def stop(self):
        self._running = False
        self.on_status_change("stopped", "Monitoring stopped")
        logger.info("Monitoring stopped")

    def poll_once(self) -> list[Document]:
        return self._poll()

    def _run_loop(self):
        self._warmup_codelist()
        self._re_resolve_targets()
        self._poll()

        while self._running:
            self._smart_sleep()
            if not self._running:
                break
            self._poll()

    def _smart_sleep(self):
        now = datetime.now()
        hour_min = now.hour * 100 + now.minute

        is_weekday = now.weekday() < 5
        is_market_hours = is_weekday and 845 <= hour_min <= 1630

        if not is_market_hours:
            self._interruptible_sleep(self.polling_interval)
            return

        sec = now.second
        if sec < 58:
            self._interruptible_sleep(58 - sec)
        for _ in range(4):
            if not self._running:
                return
            time.sleep(2)
            self._poll()

    def _interruptible_sleep(self, seconds: int):
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def _warmup_codelist(self):
        existing = self.storage.get_edinet_code_count()
        if existing > 7000:
            logger.info("EDINET codes already warmed up: %d rows", existing)
            return

        self.on_status_change("polling", "Warming up EDINET code cache...")
        logger.info("Starting code warmup (%d days)", CODELIST_WARMUP_DAYS)

        today = date.today()
        for days_ago in range(CODELIST_WARMUP_DAYS):
            if not self._running:
                break
            target_date = today - timedelta(days=days_ago)
            date_str = target_date.strftime("%Y-%m-%d")
            try:
                results, *_ = self._fetch_documents(date_str)
                codes = self._extract_codes(results)
                if codes:
                    self.storage.save_edinet_codes(codes)
                if days_ago > 0 and days_ago % 10 == 0:
                    count = self.storage.get_edinet_code_count()
                    self.on_status_change(
                        "polling",
                        f"Warming up EDINET codes... {count} rows ({days_ago}/{CODELIST_WARMUP_DAYS} days)",
                    )
            except Exception as exc:
                logger.debug("Code warmup skipped for %s: %s", date_str, exc)
            if days_ago > 0:
                time.sleep(0.3)

        code_count = self.storage.get_edinet_code_count()
        logger.info("Code warmup finished: %d rows", code_count)
        self.on_status_change("running", f"EDINET codes ready: {code_count}")

    def _re_resolve_targets(self):
        import sqlite3

        conn = sqlite3.connect(self.storage.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT doc_id, issuer_edinet_code, subject_edinet_code, subsidiary_edinet_code,
                       issuer_name, subject_name, subsidiary_name
                FROM documents
                WHERE (issuer_edinet_code IS NOT NULL AND issuer_edinet_code != '' AND (issuer_name IS NULL OR issuer_name = ''))
                   OR (subject_edinet_code IS NOT NULL AND subject_edinet_code != '' AND (subject_name IS NULL OR subject_name = ''))
                   OR (subsidiary_edinet_code IS NOT NULL AND subsidiary_edinet_code != '' AND (subsidiary_name IS NULL OR subsidiary_name = ''))
                """
            ).fetchall()
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
                logger.info("Resolved missing target names for %d rows", updated)
        finally:
            conn.close()

    def _extract_codes(self, results: list[dict]) -> list[tuple[str, str, Optional[str]]]:
        codes = []
        seen = set()
        for item in results:
            edinet_code = item.get("edinetCode")
            filer_name = item.get("filerName", "")
            sec_code = item.get("secCode")
            if edinet_code and filer_name and edinet_code not in seen:
                codes.append((edinet_code, filer_name, sec_code))
                seen.add(edinet_code)
        return codes

    def _resolve_target_info(self, doc: Document):
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
        today_str = date.today().strftime("%Y-%m-%d")
        self.on_status_change("polling", f"Fetching... ({today_str})")

        poll_started_at = datetime.now()
        poll_started_iso = poll_started_at.isoformat()
        request_started_at = None
        response_received_at = None
        http_status = None
        results: list[dict] = []

        for attempt in range(1, self.max_retries + 1):
            try:
                results, request_started_at, response_received_at, http_status = self._fetch_documents(today_str)
                self._consecutive_errors = 0
                break
            except requests.exceptions.Timeout:
                logger.warning("API timeout (attempt %d/%d)", attempt, self.max_retries)
                self.storage.save_log("timeout", f"API timeout (attempt {attempt})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._save_failed_poll_metric(
                        poll_started_at,
                        today_str,
                        request_started_at,
                        response_received_at,
                        http_status,
                        "timeout",
                        "API timeout",
                    )
                    self._handle_error("API timeout")
                    return []
            except requests.exceptions.ConnectionError:
                logger.warning("Connection error (attempt %d/%d)", attempt, self.max_retries)
                self.storage.save_log("connection_error", f"Connection error (attempt {attempt})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._save_failed_poll_metric(
                        poll_started_at,
                        today_str,
                        request_started_at,
                        response_received_at,
                        http_status,
                        "connection_error",
                        "Connection error",
                    )
                    self._handle_error("Connection error")
                    return []
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response else None
                logger.error("HTTP error %s (attempt %d/%d)", status_code or "unknown", attempt, self.max_retries)
                self.storage.save_log("http_error", f"HTTP error {status_code or 'unknown'}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_interval)
                else:
                    self._save_failed_poll_metric(
                        poll_started_at,
                        today_str,
                        request_started_at,
                        response_received_at,
                        status_code,
                        "http_error",
                        str(exc),
                    )
                    self._handle_error(f"HTTP error {status_code or 'unknown'}")
                    return []
            except Exception as exc:
                logger.exception("Unexpected polling error")
                self.storage.save_log("error", str(exc))
                self._save_failed_poll_metric(
                    poll_started_at,
                    today_str,
                    request_started_at,
                    response_received_at,
                    http_status,
                    "unexpected_error",
                    str(exc),
                )
                self._handle_error(str(exc))
                return []

        codes = self._extract_codes(results)
        if codes:
            self.storage.save_edinet_codes(codes)

        api_seen_at = response_received_at or datetime.now().isoformat()
        api_docs = [self._parse_item(item) for item in results if item.get("docID")]
        self.storage.save_api_observations(api_docs, observed_at=api_seen_at)
        self.storage.reconcile_screen_observations(api_docs)

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
                priority,
                doc.sec_code or "",
                self.watchlist,
            )

            doc.event_category = category
            doc.priority = priority
            doc.tag = tag
            doc.created_at = datetime.now().isoformat()

            self._resolve_target_info(doc)

            self.storage.save_document(doc)
            self.storage.record_document_event(doc, "monitor_recognized", event_at=doc.created_at)
            new_docs.append(doc)

        poll_completed_at = datetime.now()
        self._last_poll_time = poll_completed_at.strftime("%H:%M:%S")
        total_count = len(results)
        new_count = len(new_docs)

        status_msg = f"Latest poll {self._last_poll_time} | total {total_count} new {new_count}"
        self.on_status_change("running", status_msg)
        self.storage.save_log("ok", status_msg, new_count)
        self.storage.save_poll_metric(
            poll_started_at=poll_started_iso,
            poll_completed_at=poll_completed_at.isoformat(),
            poll_target_date=today_str,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            http_status=http_status,
            results_count=total_count,
            new_docs_count=new_count,
            duration_ms=int((poll_completed_at - poll_started_at).total_seconds() * 1000),
        )
        logger.info("Poll completed: total=%d new=%d", total_count, new_count)

        if new_docs:
            self.on_new_docs(new_docs)

        return new_docs

    def _save_failed_poll_metric(
        self,
        poll_started_at: datetime,
        poll_target_date: str,
        request_started_at: Optional[str],
        response_received_at: Optional[str],
        http_status: Optional[int],
        error_type: str,
        error_message: str,
    ):
        self.storage.save_poll_metric(
            poll_started_at=poll_started_at.isoformat(),
            poll_completed_at=datetime.now().isoformat(),
            poll_target_date=poll_target_date,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            http_status=http_status,
            duration_ms=int((datetime.now() - poll_started_at).total_seconds() * 1000),
            error_type=error_type,
            error_message=error_message,
        )

    def _fetch_documents(self, date_str: str) -> tuple[list[dict], str, str, int]:
        url = f"{self.base_url}/documents.json"
        params = {
            "date": date_str,
            "type": "2",
            "Subscription-Key": self.api_key,
        }
        request_started_at = datetime.now().isoformat()
        response = requests.get(url, params=params, timeout=self.request_timeout)
        response_received_at = datetime.now().isoformat()
        response.raise_for_status()

        data = response.json()
        metadata = data.get("metadata", {})
        status = metadata.get("status")
        if status and str(status) != "200":
            message = metadata.get("message", "Unknown error")
            raise requests.exceptions.HTTPError(f"EDINET API error: {status} - {message}")
        return data.get("results") or [], request_started_at, response_received_at, response.status_code

    def _parse_item(self, item: dict) -> Document:
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
        self._consecutive_errors += 1
        self.on_status_change("error", f"Error: {message} (consecutive {self._consecutive_errors})")
        logger.error("Consecutive error %d: %s", self._consecutive_errors, message)

    def download_pdf(self, doc_id: str, save_path: str) -> bool:
        url = f"{self.base_url}/documents/{doc_id}"
        params = {
            "type": "2",
            "Subscription-Key": self.api_key,
        }
        try:
            response = requests.get(url, params=params, timeout=60, stream=True)
            response.raise_for_status()
            with open(save_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    handle.write(chunk)
            logger.info("PDF downloaded: %s -> %s", doc_id, save_path)
            return True
        except Exception as exc:
            logger.error("PDF download failed: %s - %s", doc_id, exc)
            return False
