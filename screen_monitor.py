"""EDINET screen monitoring via headless browser."""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import date, datetime
from typing import Callable, Optional

from playwright.sync_api import Page, sync_playwright

from storage import Storage

logger = logging.getLogger(__name__)


class EdinetScreenMonitor:
    """Observes EDINET's screen listing and records first-seen timestamps."""

    def __init__(
        self,
        config: dict,
        storage: Storage,
        on_status_change: Optional[Callable[[str, str], None]] = None,
        on_new_screen_docs: Optional[Callable[[list[dict]], None]] = None,
    ):
        self.storage = storage
        self.on_status_change = on_status_change or (lambda status, message: None)
        self.on_new_screen_docs = on_new_screen_docs or (lambda docs: None)

        self.enabled = config.get("enabled", False)
        self.interval_sec = config.get("polling_interval_sec", 15)
        self.channel = config.get("browser_channel", "msedge")
        self.source_name = config.get("source_name", "edinet_screen_large_holding")
        self.search_url = config.get("search_url", "https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx")

        categories = config.get("simple_search_categories", {})
        self.use_securities_reports = categories.get("securities_reports", False)
        self.use_large_holding = categories.get("large_holding", True)
        self.use_timely_reports = categories.get("timely_reports", False)
        self.use_other = categories.get("other", False)

        self._running = False
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self):
        if self._running or not self.enabled:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.on_status_change("running", "Screen monitor started")
        logger.info("Screen monitor started")

    def stop(self):
        self._running = False
        self.on_status_change("stopped", "Screen monitor stopped")
        logger.info("Screen monitor stopped")

    def poll_once(self) -> int:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel=self.channel, headless=True)
            try:
                page = browser.new_page(viewport={"width": 1600, "height": 3000})
                return self._poll_page(page)
            finally:
                browser.close()

    def _run_loop(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel=self.channel, headless=True)
            try:
                page = browser.new_page(viewport={"width": 1600, "height": 3000})
                self._poll_with_status(page)
                while self._running:
                    now = datetime.now()
                    # 次の毎分:01秒までの待ち時間を計算
                    secs_into_minute = now.second + now.microsecond / 1_000_000
                    if secs_into_minute < 1:
                        wait = 1 - secs_into_minute
                    else:
                        wait = 61 - secs_into_minute
                    # ただし最低でもinterval_sec秒は空ける（連続アクセス防止）
                    wait = max(wait, self.interval_sec)
                    self._interruptible_sleep(int(wait))
                    if not self._running:
                        break
                    self._poll_with_status(page)
            finally:
                browser.close()

    def _poll_with_status(self, page: Page):
        try:
            count = self._poll_page(page)
            self.on_status_change("running", f"Screen monitor polled: {count} rows")
        except Exception as exc:
            logger.exception("Screen monitor poll failed")
            self.on_status_change("error", f"Screen monitor error: {exc}")

    def _is_market_burst_window(self) -> bool:
        now = datetime.now()
        hour_min = now.hour * 100 + now.minute
        is_weekday = now.weekday() < 5
        return is_weekday and 845 <= hour_min <= 1630

    def _run_market_burst(self, page: Page):
        now = datetime.now()
        if now.second < 58:
            self._interruptible_sleep(58 - now.second)

        # Mirror the API-side burst cadence around the minute boundary.
        for _ in range(5):
            if not self._running:
                return
            self._poll_with_status(page)
            self._interruptible_sleep(2)

    def _interruptible_sleep(self, seconds: int):
        end_time = time.time() + max(0, seconds)
        while self._running and time.time() < end_time:
            time.sleep(min(1, end_time - time.time()))

    def _poll_page(self, page: Page) -> int:
        observed_at = datetime.now().isoformat()
        observations = self._try_poll(page, observed_at)

        # 0行の場合はページ状態が壊れている可能性 → 新しいページで再試行
        if not observations:
            current_url = page.url
            logger.warning("Parsed 0 rows (url=%s), retrying with fresh navigation", current_url)
            page.goto(self.search_url, wait_until="load", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            observations = self._try_poll(page, observed_at)

        new_obs = []
        if observations:
            new_obs = self.storage.save_screen_observations(observations, observed_at=observed_at)
            for obs in new_obs:
                logger.info(
                    "screen_new: edinet_code=%s filer=%s doc=%s submit=%s first_seen=%s",
                    obs.get("edinet_code", ""),
                    obs.get("filer_name", ""),
                    obs.get("doc_description", ""),
                    obs.get("submit_datetime", ""),
                    observed_at,
                )
            if new_obs:
                self.on_new_screen_docs(new_obs)
            self.storage.reconcile_screen_observations(
                self.storage.get_documents(date=date.today().strftime("%Y-%m-%d"), limit=1000)
            )
        logger.info("Screen monitor parsed %d rows (new: %d)", len(observations), len(new_obs))
        return len(observations)

    def _try_poll(self, page: Page, observed_at: str) -> list[dict]:
        """検索ページへ遷移→検索実行→パース。失敗時は空リストを返す。"""
        # 検索ページへ遷移（domcontentloadedで早期に制御を取る）
        try:
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            logger.warning("goto search page failed, retrying")
            try:
                page.goto(self.search_url, wait_until="domcontentloaded", timeout=60000)
            except Exception:
                logger.warning("goto retry also failed")
                return []

        # 検索ボタンが出るまで待つ
        try:
            page.locator("#W0018BTNBTN_SEARCH").wait_for(state="visible", timeout=30000)
        except Exception:
            logger.warning("Search button not found, page may not have loaded")
            return []

        self._configure_simple_search(page)
        page.locator("#W0018BTNBTN_SEARCH").click(force=True)

        # 結果ページへの遷移を待つ
        navigated = False
        try:
            page.wait_for_url("**/WEEE0030.aspx*", timeout=30000)
            navigated = True
        except Exception:
            logger.warning("wait_for_url timeout (url=%s)", page.url)

        if not navigated:
            if "WEEK0010" in page.url:
                logger.warning("Still on search page, click did not trigger navigation")
                return []
            page.wait_for_load_state("networkidle", timeout=15000)

        page.wait_for_timeout(8000)
        body_text = page.locator("body").inner_text(timeout=30000)
        return self._parse_observations(body_text, observed_at)

    def _configure_simple_search(self, page: Page):
        page.evaluate(
            """
            (settings) => {
              const setChecked = (id, checked) => {
                const el = document.getElementById(id);
                if (!el) return;
                el.checked = checked;
                el.value = checked ? 'true' : 'false';
                el.dispatchEvent(new Event('change', { bubbles: true }));
              };
              setChecked('W0018vCHKSYORUI1', settings.useSecuritiesReports);
              setChecked('W0018vCHKSYORUI2', settings.useLargeHolding);
              setChecked('W0018vCHKSYORUI4', settings.useTimelyReports);
              setChecked('W0018vCHKSYORUI3', settings.useOther);
              const period = document.getElementById('W0018vD_KIKAN');
              if (period) {
                period.value = '1';
                period.dispatchEvent(new Event('change', { bubbles: true }));
              }
            }
            """,
            {
                "useSecuritiesReports": self.use_securities_reports,
                "useLargeHolding": self.use_large_holding,
                "useTimelyReports": self.use_timely_reports,
                "useOther": self.use_other,
            },
        )

    def _parse_observations(self, body_text: str, observed_at: str) -> list[dict]:
        lines = [self._normalize_line(line) for line in body_text.splitlines()]
        lines = [line for line in lines if line]

        start_index = -1
        for i, line in enumerate(lines):
            if self._is_submit_datetime(line):
                start_index = i
                break
        if start_index == -1:
            return []

        data_lines = lines[start_index:]
        observations = []
        i = 0
        while i < len(data_lines):
            if not self._is_submit_datetime(data_lines[i]):
                i += 1
                continue

            submit_datetime = self._normalize_submit_datetime(data_lines[i])
            doc_description = data_lines[i + 1] if i + 1 < len(data_lines) else ""
            edinet_code = data_lines[i + 2] if i + 2 < len(data_lines) else ""
            filer_name = data_lines[i + 3] if i + 3 < len(data_lines) else ""
            target_text = data_lines[i + 4] if i + 4 < len(data_lines) else ""

            if not edinet_code.startswith("E") or not doc_description or not filer_name:
                i += 1
                continue

            screen_key = self._build_screen_key(submit_datetime, doc_description, edinet_code, filer_name)
            observations.append(
                {
                    "screen_key": screen_key,
                    "submit_datetime": submit_datetime,
                    "doc_description": doc_description,
                    "edinet_code": edinet_code,
                    "filer_name": filer_name,
                    "target_text": target_text,
                    "screen_source": self.source_name,
                    "observed_at": observed_at,
                }
            )
            i += 5
        return observations

    @staticmethod
    def _normalize_line(line: str) -> str:
        return line.replace("\xa0", " ").strip()

    @staticmethod
    def _is_submit_datetime(line: str) -> bool:
        if len(line) < 16:
            return False
        try:
            datetime.strptime(line[:16], "%Y/%m/%d %H:%M")
            return True
        except ValueError:
            return False

    @staticmethod
    def _normalize_submit_datetime(value: str) -> str:
        return value[:16].replace("/", "-")

    @staticmethod
    def _build_screen_key(submit_datetime: str, doc_description: str, edinet_code: str, filer_name: str) -> str:
        raw = "||".join([submit_datetime, doc_description, edinet_code, filer_name])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()
