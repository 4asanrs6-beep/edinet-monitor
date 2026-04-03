"""EDINET Monitor - SQLite storage."""
import logging
import sqlite3
from datetime import datetime
from typing import Optional

from models import Document

logger = logging.getLogger(__name__)


class Storage:
    """SQLite-backed persistence for documents and monitoring telemetry."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    edinet_code TEXT,
                    sec_code TEXT,
                    filer_name TEXT NOT NULL,
                    doc_type_code TEXT,
                    doc_description TEXT,
                    submit_datetime TEXT,
                    ordinance_code TEXT,
                    form_code TEXT,
                    issuer_edinet_code TEXT,
                    subject_edinet_code TEXT,
                    subsidiary_edinet_code TEXT,
                    current_report_reason TEXT,
                    issuer_name TEXT DEFAULT '',
                    issuer_sec_code TEXT DEFAULT '',
                    subject_name TEXT DEFAULT '',
                    subject_sec_code TEXT DEFAULT '',
                    subsidiary_name TEXT DEFAULT '',
                    event_category TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 4,
                    tag TEXT DEFAULT '',
                    is_read INTEGER NOT NULL DEFAULT 0,
                    memo TEXT DEFAULT '',
                    raw_json TEXT,
                    xbrl_flag INTEGER DEFAULT 0,
                    pdf_flag INTEGER DEFAULT 0,
                    withdrawal_status TEXT DEFAULT '0',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_documents_submit
                    ON documents(submit_datetime DESC);
                CREATE INDEX IF NOT EXISTS idx_documents_category
                    ON documents(event_category);
                CREATE INDEX IF NOT EXISTS idx_documents_priority
                    ON documents(priority);
                CREATE INDEX IF NOT EXISTS idx_documents_is_read
                    ON documents(is_read);

                CREATE TABLE IF NOT EXISTS edinet_codes (
                    edinet_code TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    sec_code TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    docs_found INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_log_timestamp
                    ON monitor_log(timestamp DESC);

                CREATE TABLE IF NOT EXISTS document_latency (
                    doc_id TEXT PRIMARY KEY,
                    submit_datetime TEXT DEFAULT '',
                    doc_type_code TEXT DEFAULT '',
                    filer_name TEXT DEFAULT '',
                    event_category TEXT DEFAULT '',
                    api_first_seen_at TEXT,
                    monitor_recognized_at TEXT,
                    gui_queue_received_at TEXT,
                    notification_started_at TEXT,
                    notification_completed_at TEXT,
                    screen_first_seen_at TEXT,
                    screen_source TEXT DEFAULT '',
                    notes TEXT DEFAULT '',
                    last_updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_document_latency_submit
                    ON document_latency(submit_datetime DESC);
                CREATE INDEX IF NOT EXISTS idx_document_latency_api_seen
                    ON document_latency(api_first_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_document_latency_category
                    ON document_latency(event_category);

                CREATE TABLE IF NOT EXISTS poll_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    poll_started_at TEXT NOT NULL,
                    poll_completed_at TEXT,
                    poll_target_date TEXT,
                    request_started_at TEXT,
                    response_received_at TEXT,
                    http_status INTEGER,
                    results_count INTEGER DEFAULT 0,
                    new_docs_count INTEGER DEFAULT 0,
                    duration_ms INTEGER DEFAULT 0,
                    error_type TEXT DEFAULT '',
                    error_message TEXT DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_poll_metrics_started
                    ON poll_metrics(poll_started_at DESC);

                CREATE TABLE IF NOT EXISTS screen_observations (
                    screen_key TEXT PRIMARY KEY,
                    submit_datetime TEXT DEFAULT '',
                    doc_description TEXT DEFAULT '',
                    edinet_code TEXT DEFAULT '',
                    filer_name TEXT DEFAULT '',
                    target_text TEXT DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    screen_source TEXT DEFAULT '',
                    matched_doc_id TEXT DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_screen_observations_first_seen
                    ON screen_observations(first_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_screen_observations_matched_doc
                    ON screen_observations(matched_doc_id);
                """
            )
            self._migrate_documents_schema(conn)
            self._migrate_document_latency_schema(conn)
            conn.commit()
        finally:
            conn.close()

    def _migrate_documents_schema(self, conn: sqlite3.Connection):
        cols = conn.execute("PRAGMA table_info(documents)").fetchall()
        col_names = {row["name"] for row in cols}
        if "tag" not in col_names:
            conn.execute("ALTER TABLE documents ADD COLUMN tag TEXT DEFAULT ''")
        if "is_starred" not in col_names:
            conn.execute("ALTER TABLE documents ADD COLUMN is_starred INTEGER NOT NULL DEFAULT 0")

    def _migrate_document_latency_schema(self, conn: sqlite3.Connection):
        cols = conn.execute("PRAGMA table_info(document_latency)").fetchall()
        col_names = {row["name"] for row in cols}
        for name, ddl in [
            ("screen_first_seen_at", "ALTER TABLE document_latency ADD COLUMN screen_first_seen_at TEXT"),
            ("screen_source", "ALTER TABLE document_latency ADD COLUMN screen_source TEXT DEFAULT ''"),
            ("notes", "ALTER TABLE document_latency ADD COLUMN notes TEXT DEFAULT ''"),
        ]:
            if name not in col_names:
                conn.execute(ddl)

    # ===== EDINET codes =====

    def save_edinet_codes(self, codes: list[tuple[str, str, Optional[str]]]):
        if not codes:
            return
        now = datetime.now().isoformat()
        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT INTO edinet_codes (edinet_code, company_name, sec_code, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(edinet_code) DO UPDATE SET
                    company_name = excluded.company_name,
                    sec_code = COALESCE(excluded.sec_code, edinet_codes.sec_code),
                    updated_at = excluded.updated_at
                """,
                [(code, name, sec, now) for code, name, sec in codes],
            )
            conn.commit()
        finally:
            conn.close()

    def lookup_edinet_code(self, edinet_code: str) -> Optional[tuple[str, Optional[str]]]:
        if not edinet_code:
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT company_name, sec_code FROM edinet_codes WHERE edinet_code = ?",
                (edinet_code,),
            ).fetchone()
            if row:
                return (row["company_name"], row["sec_code"])
            return None
        finally:
            conn.close()

    def lookup_by_company_name(self, company_name: str) -> Optional[tuple[str, Optional[str]]]:
        """会社名からedinet_code, sec_codeを逆引き. 完全一致→前方一致→部分一致の順で検索."""
        if not company_name:
            return None
        # 全角半角・スペース揺れを吸収するため正規化
        name = company_name.strip()
        conn = self._connect()
        try:
            # 1. 完全一致
            row = conn.execute(
                "SELECT edinet_code, sec_code FROM edinet_codes WHERE company_name = ? LIMIT 1",
                (name,),
            ).fetchone()
            if row:
                return (row["edinet_code"], row["sec_code"])
            # 2. 前方一致（「株式会社○○」→「株式会社○○ホールディングス」等に対応）
            row = conn.execute(
                "SELECT edinet_code, sec_code FROM edinet_codes WHERE company_name LIKE ? LIMIT 1",
                (f"{name}%",),
            ).fetchone()
            if row:
                return (row["edinet_code"], row["sec_code"])
            # 3. 部分一致（最終手段）
            row = conn.execute(
                "SELECT edinet_code, sec_code FROM edinet_codes WHERE company_name LIKE ? LIMIT 1",
                (f"%{name}%",),
            ).fetchone()
            if row:
                return (row["edinet_code"], row["sec_code"])
            return None
        finally:
            conn.close()

    def get_edinet_code_count(self) -> int:
        conn = self._connect()
        try:
            row = conn.execute("SELECT COUNT(*) as cnt FROM edinet_codes").fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    # ===== document storage =====

    def doc_exists(self, doc_id: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute("SELECT 1 FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
            return row is not None
        finally:
            conn.close()

    def save_document(self, doc: Document):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO documents (
                    doc_id, edinet_code, sec_code, filer_name,
                    doc_type_code, doc_description, submit_datetime,
                    ordinance_code, form_code,
                    issuer_edinet_code, subject_edinet_code, subsidiary_edinet_code,
                    current_report_reason,
                    issuer_name, issuer_sec_code,
                    subject_name, subject_sec_code,
                    subsidiary_name,
                    event_category, priority, tag,
                    is_read, memo, raw_json, xbrl_flag, pdf_flag,
                    withdrawal_status, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    event_category = excluded.event_category,
                    priority = excluded.priority,
                    tag = excluded.tag,
                    issuer_name = excluded.issuer_name,
                    issuer_sec_code = excluded.issuer_sec_code,
                    subject_name = excluded.subject_name,
                    subject_sec_code = excluded.subject_sec_code,
                    subsidiary_name = excluded.subsidiary_name,
                    withdrawal_status = excluded.withdrawal_status
                """,
                (
                    doc.doc_id,
                    doc.edinet_code,
                    doc.sec_code,
                    doc.filer_name,
                    doc.doc_type_code,
                    doc.doc_description,
                    doc.submit_datetime,
                    doc.ordinance_code,
                    doc.form_code,
                    doc.issuer_edinet_code,
                    doc.subject_edinet_code,
                    doc.subsidiary_edinet_code,
                    doc.current_report_reason,
                    doc.issuer_name,
                    doc.issuer_sec_code,
                    doc.subject_name,
                    doc.subject_sec_code,
                    doc.subsidiary_name,
                    doc.event_category,
                    doc.priority,
                    doc.tag,
                    int(doc.is_read),
                    doc.memo,
                    doc.raw_json,
                    int(doc.xbrl_flag),
                    int(doc.pdf_flag),
                    doc.withdrawal_status,
                    doc.created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    # ===== latency telemetry =====

    def save_api_observations(self, docs: list[Document], observed_at: Optional[str] = None) -> list[Document]:
        """Save API observations and return list of newly detected docs."""
        if not docs:
            return []
        observed_at = observed_at or datetime.now().isoformat()
        now = datetime.now().isoformat()

        conn = self._connect()
        new_docs: list[Document] = []
        try:
            for doc in docs:
                if not doc.doc_id:
                    continue
                row = conn.execute(
                    "SELECT api_first_seen_at FROM document_latency WHERE doc_id = ?",
                    (doc.doc_id,),
                ).fetchone()
                if not row or not row["api_first_seen_at"]:
                    new_docs.append(doc)

            rows = [
                (
                    doc.doc_id,
                    doc.submit_datetime,
                    doc.doc_type_code,
                    doc.filer_name,
                    doc.event_category,
                    observed_at,
                    now,
                )
                for doc in docs
                if doc.doc_id
            ]
            if rows:
                conn.executemany(
                    """
                    INSERT INTO document_latency (
                        doc_id, submit_datetime, doc_type_code, filer_name,
                        event_category, api_first_seen_at, last_updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(doc_id) DO UPDATE SET
                        submit_datetime = CASE
                            WHEN excluded.submit_datetime != '' THEN excluded.submit_datetime
                            ELSE document_latency.submit_datetime
                        END,
                        doc_type_code = CASE
                            WHEN excluded.doc_type_code != '' THEN excluded.doc_type_code
                            ELSE document_latency.doc_type_code
                        END,
                        filer_name = CASE
                            WHEN excluded.filer_name != '' THEN excluded.filer_name
                            ELSE document_latency.filer_name
                        END,
                        event_category = CASE
                            WHEN excluded.event_category != '' THEN excluded.event_category
                            ELSE document_latency.event_category
                        END,
                        api_first_seen_at = COALESCE(document_latency.api_first_seen_at, excluded.api_first_seen_at),
                        last_updated_at = excluded.last_updated_at
                    """,
                    rows,
                )
            conn.commit()
        finally:
            conn.close()
        return new_docs

    def record_document_event(
        self,
        doc: Document,
        event_name: str,
        event_at: Optional[str] = None,
        screen_source: str = "",
        notes: str = "",
    ):
        if not doc.doc_id:
            return

        column_map = {
            "monitor_recognized": "monitor_recognized_at",
            "gui_queue_received": "gui_queue_received_at",
            "notification_started": "notification_started_at",
            "notification_completed": "notification_completed_at",
            "screen_first_seen": "screen_first_seen_at",
        }
        column = column_map.get(event_name)
        if not column:
            raise ValueError(f"unknown latency event: {event_name}")

        event_at = event_at or datetime.now().isoformat()
        now = datetime.now().isoformat()

        conn = self._connect()
        try:
            conn.execute(
                f"""
                INSERT INTO document_latency (
                    doc_id, submit_datetime, doc_type_code, filer_name, event_category,
                    {column}, screen_source, notes, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    submit_datetime = CASE
                        WHEN excluded.submit_datetime != '' THEN excluded.submit_datetime
                        ELSE document_latency.submit_datetime
                    END,
                    doc_type_code = CASE
                        WHEN excluded.doc_type_code != '' THEN excluded.doc_type_code
                        ELSE document_latency.doc_type_code
                    END,
                    filer_name = CASE
                        WHEN excluded.filer_name != '' THEN excluded.filer_name
                        ELSE document_latency.filer_name
                    END,
                    event_category = CASE
                        WHEN excluded.event_category != '' THEN excluded.event_category
                        ELSE document_latency.event_category
                    END,
                    {column} = COALESCE(document_latency.{column}, excluded.{column}),
                    screen_source = CASE
                        WHEN excluded.screen_source != '' THEN excluded.screen_source
                        ELSE document_latency.screen_source
                    END,
                    notes = CASE
                        WHEN excluded.notes != '' THEN excluded.notes
                        ELSE document_latency.notes
                    END,
                    last_updated_at = excluded.last_updated_at
                """,
                (
                    doc.doc_id,
                    doc.submit_datetime,
                    doc.doc_type_code,
                    doc.filer_name,
                    doc.event_category,
                    event_at,
                    screen_source,
                    notes,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def save_poll_metric(
        self,
        *,
        poll_started_at: str,
        poll_completed_at: Optional[str] = None,
        poll_target_date: str = "",
        request_started_at: Optional[str] = None,
        response_received_at: Optional[str] = None,
        http_status: Optional[int] = None,
        results_count: int = 0,
        new_docs_count: int = 0,
        duration_ms: int = 0,
        error_type: str = "",
        error_message: str = "",
    ):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO poll_metrics (
                    poll_started_at, poll_completed_at, poll_target_date,
                    request_started_at, response_received_at, http_status,
                    results_count, new_docs_count, duration_ms,
                    error_type, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    poll_started_at,
                    poll_completed_at,
                    poll_target_date,
                    request_started_at,
                    response_received_at,
                    http_status,
                    results_count,
                    new_docs_count,
                    duration_ms,
                    error_type,
                    error_message,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def save_screen_observations(self, observations: list[dict], observed_at: Optional[str] = None) -> list[dict]:
        """Save screen observations and return list of newly detected ones."""
        if not observations:
            return []
        observed_at = observed_at or datetime.now().isoformat()
        conn = self._connect()
        new_observations: list[dict] = []
        try:
            existing_keys = set()
            for obs in observations:
                row = conn.execute(
                    "SELECT 1 FROM screen_observations WHERE screen_key = ?",
                    (obs["screen_key"],),
                ).fetchone()
                if row:
                    existing_keys.add(obs["screen_key"])

            rows = []
            for obs in observations:
                rows.append(
                    (
                        obs["screen_key"],
                        obs.get("submit_datetime", ""),
                        obs.get("doc_description", ""),
                        obs.get("edinet_code", ""),
                        obs.get("filer_name", ""),
                        obs.get("target_text", ""),
                        observed_at,
                        observed_at,
                        obs.get("screen_source", ""),
                    )
                )
                if obs["screen_key"] not in existing_keys:
                    new_observations.append(obs)

            conn.executemany(
                """
                INSERT INTO screen_observations (
                    screen_key, submit_datetime, doc_description, edinet_code,
                    filer_name, target_text, first_seen_at, last_seen_at, screen_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(screen_key) DO UPDATE SET
                    submit_datetime = CASE
                        WHEN excluded.submit_datetime != '' THEN excluded.submit_datetime
                        ELSE screen_observations.submit_datetime
                    END,
                    doc_description = CASE
                        WHEN excluded.doc_description != '' THEN excluded.doc_description
                        ELSE screen_observations.doc_description
                    END,
                    edinet_code = CASE
                        WHEN excluded.edinet_code != '' THEN excluded.edinet_code
                        ELSE screen_observations.edinet_code
                    END,
                    filer_name = CASE
                        WHEN excluded.filer_name != '' THEN excluded.filer_name
                        ELSE screen_observations.filer_name
                    END,
                    target_text = CASE
                        WHEN excluded.target_text != '' THEN excluded.target_text
                        ELSE screen_observations.target_text
                    END,
                    last_seen_at = excluded.last_seen_at,
                    screen_source = CASE
                        WHEN excluded.screen_source != '' THEN excluded.screen_source
                        ELSE screen_observations.screen_source
                    END
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()
        return new_observations

    def reconcile_screen_observations(self, docs: list[Document]) -> int:
        matched = 0
        matched_events: list[tuple[Document, str, str]] = []
        conn = self._connect()
        try:
            for doc in docs:
                if not doc.doc_id:
                    continue
                screen_row = conn.execute(
                    """
                    SELECT screen_key, first_seen_at, screen_source
                    FROM screen_observations
                    WHERE matched_doc_id = ''
                      AND submit_datetime = ?
                      AND doc_description = ?
                      AND edinet_code = ?
                      AND filer_name = ?
                    ORDER BY first_seen_at ASC
                    LIMIT 1
                    """,
                    (
                        self._normalize_submit_minute(doc.submit_datetime),
                        doc.doc_description,
                        doc.edinet_code,
                        doc.filer_name,
                    ),
                ).fetchone()
                if not screen_row:
                    continue
                conn.execute(
                    "UPDATE screen_observations SET matched_doc_id = ? WHERE screen_key = ?",
                    (doc.doc_id, screen_row["screen_key"]),
                )
                matched_events.append(
                    (
                        doc,
                        screen_row["first_seen_at"],
                        screen_row["screen_source"] or "edinet_screen",
                    )
                )
                matched += 1
            conn.commit()
        finally:
            conn.close()
        for doc, first_seen_at, screen_source in matched_events:
            self.record_document_event(
                doc,
                "screen_first_seen",
                event_at=first_seen_at,
                screen_source=screen_source,
            )
            self._log_latency_comparison(doc.doc_id)
        return matched

    def _log_latency_comparison(self, doc_id: str):
        """Log the screen vs API latency for a reconciled document."""
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT doc_id, filer_name, doc_type_code, submit_datetime,
                       api_first_seen_at, screen_first_seen_at
                FROM document_latency
                WHERE doc_id = ?
                """,
                (doc_id,),
            ).fetchone()
            if not row:
                return
            api_at = row["api_first_seen_at"]
            screen_at = row["screen_first_seen_at"]
            filer = row["filer_name"] or ""
            submit = row["submit_datetime"] or ""
            if api_at and screen_at:
                try:
                    api_dt = datetime.fromisoformat(api_at)
                    screen_dt = datetime.fromisoformat(screen_at)
                    diff_sec = (api_dt - screen_dt).total_seconds()
                    if diff_sec > 0:
                        leader = "screen"
                        lead_sec = diff_sec
                    else:
                        leader = "api"
                        lead_sec = -diff_sec
                    logger.info(
                        "latency: doc=%s filer=%s submit=%s screen=%s api=%s | %s led by %.1fs",
                        doc_id, filer, submit,
                        screen_at, api_at,
                        leader, lead_sec,
                    )
                except (ValueError, TypeError):
                    pass
            elif screen_at and not api_at:
                logger.info(
                    "latency: doc=%s filer=%s submit=%s | screen detected at %s, API not yet seen",
                    doc_id, filer, submit, screen_at,
                )
            elif api_at and not screen_at:
                logger.info(
                    "latency: doc=%s filer=%s submit=%s | API detected at %s, screen not yet seen",
                    doc_id, filer, submit, api_at,
                )
        finally:
            conn.close()

    def get_recent_latency_records(self, limit: int = 100) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM document_latency
                ORDER BY COALESCE(notification_completed_at, monitor_recognized_at, api_first_seen_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_recent_poll_metrics(self, limit: int = 200) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM poll_metrics
                ORDER BY poll_started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_recent_screen_observations(self, limit: int = 200) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM screen_observations
                ORDER BY first_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    # ===== read APIs =====

    def get_documents(
        self,
        date: Optional[str] = None,
        category: Optional[str] = None,
        is_read: Optional[bool] = None,
        sec_code: Optional[str] = None,
        search_text: Optional[str] = None,
        sort_by: str = "time",
        limit: int = 500,
    ) -> list[Document]:
        conditions = []
        params = []

        if date:
            conditions.append("submit_datetime LIKE ?")
            params.append(f"{date}%")
        if category:
            conditions.append("event_category = ?")
            params.append(category)
        if is_read is not None:
            conditions.append("is_read = ?")
            params.append(int(is_read))
        if sec_code:
            conditions.append("(sec_code LIKE ? OR issuer_sec_code LIKE ? OR subject_sec_code LIKE ?)")
            params.extend([f"{sec_code}%", f"{sec_code}%", f"{sec_code}%"])
        if search_text:
            conditions.append(
                "(filer_name LIKE ? OR doc_description LIKE ? OR memo LIKE ?"
                " OR issuer_name LIKE ? OR subject_name LIKE ?)"
            )
            pattern = f"%{search_text}%"
            params.extend([pattern, pattern, pattern, pattern, pattern])

        where = " AND ".join(conditions) if conditions else "1=1"
        order = "priority ASC, submit_datetime DESC" if sort_by == "priority" else "submit_datetime DESC"
        query = f"""
            SELECT * FROM documents
            WHERE {where}
            ORDER BY {order}
            LIMIT ?
        """
        params.append(limit)

        conn = self._connect()
        try:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_doc(row) for row in rows]
        finally:
            conn.close()

    def update_read_status(self, doc_id: str, is_read: bool):
        conn = self._connect()
        try:
            conn.execute("UPDATE documents SET is_read = ? WHERE doc_id = ?", (int(is_read), doc_id))
            conn.commit()
        finally:
            conn.close()

    def update_starred(self, doc_id: str, is_starred: bool):
        conn = self._connect()
        try:
            conn.execute("UPDATE documents SET is_starred = ? WHERE doc_id = ?", (int(is_starred), doc_id))
            conn.commit()
        finally:
            conn.close()

    def get_starred_documents(self, limit: int = 500) -> list[Document]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM documents WHERE is_starred = 1 ORDER BY submit_datetime DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [self._row_to_doc(row) for row in rows]
        finally:
            conn.close()

    def update_memo(self, doc_id: str, memo: str):
        conn = self._connect()
        try:
            conn.execute("UPDATE documents SET memo = ? WHERE doc_id = ?", (memo, doc_id))
            conn.commit()
        finally:
            conn.close()

    def mark_all_read(self):
        conn = self._connect()
        try:
            conn.execute("UPDATE documents SET is_read = 1 WHERE is_read = 0")
            conn.commit()
        finally:
            conn.close()

    def get_unread_count(self) -> int:
        conn = self._connect()
        try:
            row = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE is_read = 0").fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    def save_log(self, status: str, message: str, docs_found: int = 0):
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO monitor_log (timestamp, status, message, docs_found) VALUES (?, ?, ?, ?)",
                (datetime.now().isoformat(), status, message, docs_found),
            )
            conn.commit()
        finally:
            conn.close()

    def get_logs(self, limit: int = 100) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM monitor_log ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def _row_to_doc(self, row: sqlite3.Row) -> Document:
        return Document(
            doc_id=row["doc_id"],
            edinet_code=row["edinet_code"] or "",
            sec_code=row["sec_code"],
            filer_name=row["filer_name"],
            doc_type_code=row["doc_type_code"] or "",
            doc_description=row["doc_description"] or "",
            submit_datetime=row["submit_datetime"] or "",
            ordinance_code=row["ordinance_code"] or "",
            form_code=row["form_code"] or "",
            issuer_edinet_code=row["issuer_edinet_code"],
            subject_edinet_code=row["subject_edinet_code"],
            subsidiary_edinet_code=row["subsidiary_edinet_code"],
            current_report_reason=row["current_report_reason"],
            issuer_name=row["issuer_name"] or "",
            issuer_sec_code=row["issuer_sec_code"] or "",
            subject_name=row["subject_name"] or "",
            subject_sec_code=row["subject_sec_code"] or "",
            subsidiary_name=row["subsidiary_name"] or "",
            event_category=row["event_category"],
            priority=row["priority"],
            tag=row["tag"] if "tag" in row.keys() else "",
            is_read=bool(row["is_read"]),
            is_starred=bool(row["is_starred"]) if "is_starred" in row.keys() else False,
            memo=row["memo"] or "",
            raw_json=row["raw_json"] or "",
            xbrl_flag=bool(row["xbrl_flag"]),
            pdf_flag=bool(row["pdf_flag"]),
            withdrawal_status=row["withdrawal_status"] or "0",
            created_at=row["created_at"] or "",
        )

    @staticmethod
    def _normalize_submit_minute(value: str) -> str:
        if not value:
            return ""
        normalized = value.replace("/", "-").strip()
        if len(normalized) >= 16:
            return normalized[:16]
        return normalized
