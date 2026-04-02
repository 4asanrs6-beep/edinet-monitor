"""EDINET Monitor - SQLiteストレージ."""
import sqlite3
from datetime import datetime
from typing import Optional

from models import Document


class Storage:
    """SQLiteによる開示書類の永続化."""

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
        """テーブル作成."""
        conn = self._connect()
        try:
            conn.executescript("""
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
            """)
            self._migrate_documents_schema(conn)
            conn.commit()
        finally:
            conn.close()

    def _migrate_documents_schema(self, conn: sqlite3.Connection):
        """documents テーブルの後方互換マイグレーション."""
        cols = conn.execute("PRAGMA table_info(documents)").fetchall()
        col_names = {row["name"] for row in cols}
        if "tag" not in col_names:
            conn.execute("ALTER TABLE documents ADD COLUMN tag TEXT DEFAULT ''")

    # ===== EDINETコードリスト =====

    def save_edinet_codes(self, codes: list[tuple[str, str, Optional[str]]]):
        """EDINETコードを一括保存. codes: [(edinet_code, company_name, sec_code), ...]"""
        if not codes:
            return
        now = datetime.now().isoformat()
        conn = self._connect()
        try:
            conn.executemany(
                """INSERT INTO edinet_codes (edinet_code, company_name, sec_code, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(edinet_code) DO UPDATE SET
                       company_name = excluded.company_name,
                       sec_code = COALESCE(excluded.sec_code, edinet_codes.sec_code),
                       updated_at = excluded.updated_at""",
                [(code, name, sec, now) for code, name, sec in codes],
            )
            conn.commit()
        finally:
            conn.close()

    def lookup_edinet_code(self, edinet_code: str) -> Optional[tuple[str, Optional[str]]]:
        """EDINETコードから (会社名, 証券コード) を取得."""
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

    def get_edinet_code_count(self) -> int:
        """登録済みEDINETコード数."""
        conn = self._connect()
        try:
            row = conn.execute("SELECT COUNT(*) as cnt FROM edinet_codes").fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    # ===== 書類操作 =====

    def doc_exists(self, doc_id: str) -> bool:
        """docIDが既に存在するか確認."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def save_document(self, doc: Document):
        """書類を保存 (既存なら更新)."""
        conn = self._connect()
        try:
            conn.execute("""
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
            """, (
                doc.doc_id, doc.edinet_code, doc.sec_code, doc.filer_name,
                doc.doc_type_code, doc.doc_description, doc.submit_datetime,
                doc.ordinance_code, doc.form_code,
                doc.issuer_edinet_code, doc.subject_edinet_code, doc.subsidiary_edinet_code,
                doc.current_report_reason,
                doc.issuer_name, doc.issuer_sec_code,
                doc.subject_name, doc.subject_sec_code,
                doc.subsidiary_name,
                doc.event_category, doc.priority, doc.tag,
                int(doc.is_read), doc.memo, doc.raw_json,
                int(doc.xbrl_flag), int(doc.pdf_flag),
                doc.withdrawal_status, doc.created_at,
            ))
            conn.commit()
        finally:
            conn.close()

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
        """書類を検索.

        Args:
            sort_by: "time" (時刻降順) or "priority" (優先度昇順→時刻降順)
        """
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
            conditions.append(
                "(sec_code LIKE ? OR issuer_sec_code LIKE ? OR subject_sec_code LIKE ?)"
            )
            params.extend([f"{sec_code}%", f"{sec_code}%", f"{sec_code}%"])
        if search_text:
            conditions.append(
                "(filer_name LIKE ? OR doc_description LIKE ? OR memo LIKE ?"
                " OR issuer_name LIKE ? OR subject_name LIKE ?)"
            )
            pattern = f"%{search_text}%"
            params.extend([pattern, pattern, pattern, pattern, pattern])

        where = " AND ".join(conditions) if conditions else "1=1"
        if sort_by == "priority":
            order = "priority ASC, submit_datetime DESC"
        else:
            order = "submit_datetime DESC"
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
        """既読/未読を更新."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE documents SET is_read = ? WHERE doc_id = ?",
                (int(is_read), doc_id),
            )
            conn.commit()
        finally:
            conn.close()

    def update_memo(self, doc_id: str, memo: str):
        """メモを更新."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE documents SET memo = ? WHERE doc_id = ?",
                (memo, doc_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_all_read(self):
        """全書類を既読にする."""
        conn = self._connect()
        try:
            conn.execute("UPDATE documents SET is_read = 1 WHERE is_read = 0")
            conn.commit()
        finally:
            conn.close()

    def get_unread_count(self) -> int:
        """未読件数を取得."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM documents WHERE is_read = 0"
            ).fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    def save_log(self, status: str, message: str, docs_found: int = 0):
        """監視ログを保存."""
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
        """監視ログを取得."""
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
        """DBの行をDocumentに変換."""
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
            memo=row["memo"] or "",
            raw_json=row["raw_json"] or "",
            xbrl_flag=bool(row["xbrl_flag"]),
            pdf_flag=bool(row["pdf_flag"]),
            withdrawal_status=row["withdrawal_status"] or "0",
            created_at=row["created_at"] or "",
        )
