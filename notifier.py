"""EDINET Monitor - desktop notification helpers."""
import logging
from datetime import datetime

from models import Document

logger = logging.getLogger(__name__)

try:
    from winotify import Notification, audio

    HAS_WINOTIFY = True
except ImportError:
    HAS_WINOTIFY = False
    logger.warning("winotify is not installed; falling back to console notifications")


class Notifier:
    """Sends desktop notifications for newly detected documents."""

    APP_ID = "EDINET Monitor"

    def __init__(self, enabled: bool = True, sound: bool = True, max_priority_to_notify: int = 2):
        self.enabled = enabled
        self.sound = sound
        self.max_priority_to_notify = max_priority_to_notify

    def notify(self, doc: Document) -> str | None:
        if not self.enabled or doc.priority > self.max_priority_to_notify:
            return None

        title = self._build_title(doc)
        body = self._build_body(doc)

        if HAS_WINOTIFY:
            self._notify_windows(title, body)
        else:
            self._notify_fallback(title, body)
        return datetime.now().isoformat()

    def notify_batch(self, docs: list[Document]) -> dict[str, str]:
        if not self.enabled or not docs:
            return {}

        docs = [doc for doc in docs if doc.priority <= self.max_priority_to_notify]
        if not docs:
            return {}

        completion_times: dict[str, str] = {}
        if len(docs) <= 3:
            for doc in docs:
                completed_at = self.notify(doc)
                if completed_at:
                    completion_times[doc.doc_id] = completed_at
            return completion_times

        title = f"EDINET: new documents {len(docs)}"
        lines = []
        for doc in docs[:5]:
            target = doc.target_display
            if target:
                lines.append(f"[{doc.event_category}] {target} / {doc.filer_name}")
            else:
                ticker = f"({doc.ticker})" if doc.ticker else ""
                lines.append(f"[{doc.event_category}] {doc.filer_name}{ticker}")
        if len(docs) > 5:
            lines.append(f"... and {len(docs) - 5} more")
        body = "\n".join(lines)

        if HAS_WINOTIFY:
            self._notify_windows(title, body)
        else:
            self._notify_fallback(title, body)

        completed_at = datetime.now().isoformat()
        for doc in docs:
            completion_times[doc.doc_id] = completed_at
        return completion_times

    def notify_error(self, message: str):
        if not self.enabled:
            return
        if HAS_WINOTIFY:
            self._notify_windows("EDINET Monitor - Error", message)
        else:
            self._notify_fallback("EDINET Monitor - Error", message)

    def _build_title(self, doc: Document) -> str:
        parts = [f"[{doc.event_category}]"]
        target = doc.target_display
        if target:
            parts.append(target)
            parts.append(f"/ {doc.filer_name}")
        else:
            if doc.ticker:
                parts.append(f"({doc.ticker})")
            parts.append(doc.filer_name)
        return " ".join(parts)

    def _build_body(self, doc: Document) -> str:
        parts = [doc.doc_description]
        if doc.submit_time:
            parts.append(f"submitted: {doc.submit_time}")
        return " | ".join(parts)

    def _notify_windows(self, title: str, body: str):
        try:
            logger.info("notification_send: title=%s", title)
            toast = Notification(
                app_id=self.APP_ID,
                title=title,
                msg=body,
                duration="long",
            )
            if self.sound:
                toast.set_audio(audio.Default, loop=False)
            toast.show()
            logger.info("notification_sent: title=%s", title)
        except Exception as exc:
            logger.error("Notification failed: %s", exc)
            self._notify_fallback(title, body)

    def _notify_fallback(self, title: str, body: str):
        print(f"\n{'=' * 50}")
        print(f"[Notification] {title}")
        print(f"  {body}")
        print(f"{'=' * 50}\n")
