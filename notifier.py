"""EDINET Monitor - デスクトップ通知."""
import logging
import subprocess
import sys
from typing import Optional

from models import Document

logger = logging.getLogger(__name__)

# winotify が使えない場合のフォールバック
try:
    from winotify import Notification, audio
    HAS_WINOTIFY = True
except ImportError:
    HAS_WINOTIFY = False
    logger.warning("winotify が見つかりません。通知はコンソール出力のみになります。")


class Notifier:
    """デスクトップ通知."""

    APP_ID = "EDINET Monitor"

    def __init__(self, enabled: bool = True, sound: bool = True):
        self.enabled = enabled
        self.sound = sound

    def notify(self, doc: Document):
        """書類の新着通知を送信."""
        if not self.enabled:
            return

        title = self._build_title(doc)
        body = self._build_body(doc)

        if HAS_WINOTIFY:
            self._notify_windows(title, body, doc)
        else:
            self._notify_fallback(title, body)

    def notify_batch(self, docs: list[Document]):
        """複数書類をまとめて通知.

        3件以下なら個別通知、4件以上ならサマリー通知。
        """
        if not self.enabled or not docs:
            return

        if len(docs) <= 3:
            for doc in docs:
                self.notify(doc)
        else:
            # サマリー通知
            title = f"EDINET: 新着 {len(docs)} 件"
            lines = []
            for doc in docs[:5]:
                cat = doc.event_category
                target = doc.target_display
                if target:
                    lines.append(f"[{cat}] {target} ← {doc.filer_name}")
                else:
                    ticker = f"({doc.ticker})" if doc.ticker else ""
                    lines.append(f"[{cat}] {doc.filer_name}{ticker}")
            if len(docs) > 5:
                lines.append(f"... 他 {len(docs) - 5} 件")
            body = "\n".join(lines)

            if HAS_WINOTIFY:
                self._notify_windows(title, body)
            else:
                self._notify_fallback(title, body)

    def notify_error(self, message: str):
        """エラー通知."""
        if not self.enabled:
            return
        if HAS_WINOTIFY:
            self._notify_windows("EDINET Monitor - エラー", message)
        else:
            self._notify_fallback("EDINET Monitor - エラー", message)

    def _build_title(self, doc: Document) -> str:
        """通知タイトルを生成."""
        parts = [f"[{doc.event_category}]"]
        # 対象会社がある場合はそちらを主表示
        target = doc.target_display
        if target:
            parts.append(target)
            parts.append(f"← {doc.filer_name}")
        else:
            if doc.ticker:
                parts.append(f"({doc.ticker})")
            parts.append(doc.filer_name)
        return " ".join(parts)

    def _build_body(self, doc: Document) -> str:
        """通知本文を生成."""
        parts = [doc.doc_description]
        if doc.submit_time:
            parts.append(f"提出: {doc.submit_time}")
        return " | ".join(parts)

    def _notify_windows(self, title: str, body: str, doc: Document = None):
        """Windows トースト通知."""
        try:
            toast = Notification(
                app_id=self.APP_ID,
                title=title,
                msg=body,
                duration="long",
            )
            if self.sound:
                toast.set_audio(audio.Default, loop=False)
            toast.show()
        except Exception as e:
            logger.error("通知送信失敗: %s", e)
            self._notify_fallback(title, body)

    def _notify_fallback(self, title: str, body: str):
        """フォールバック: コンソール出力."""
        print(f"\n{'='*50}")
        print(f"[通知] {title}")
        print(f"  {body}")
        print(f"{'='*50}\n")
