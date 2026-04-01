"""EDINET Monitor - データモデル定義."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class EventCategory(str, Enum):
    """イベント分類."""
    TAIRYO_HOYU = "大量保有報告書"
    HENKO_HOKOKU = "変更報告書"
    ZOSHI = "増資"
    CB = "CB/転換社債"
    MS_WARRANT = "MSワラント"
    DAISAN_WARIATE = "第三者割当"
    JIKO_KABU = "自己株取得"
    TOB = "公開買付"
    KABUNUSHI_IDO = "主要株主異動"
    RINJI = "臨時報告書"
    OTHER = "その他"


class Priority(int, Enum):
    """優先度. 数値が小さいほど高優先."""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


# 優先度ごとの表示ラベル
PRIORITY_LABELS = {
    Priority.CRITICAL: "最重要",
    Priority.HIGH: "重要",
    Priority.MEDIUM: "中",
    Priority.LOW: "低",
}


@dataclass
class Document:
    """EDINET開示書類."""
    doc_id: str
    edinet_code: str
    sec_code: Optional[str]
    filer_name: str
    doc_type_code: str
    doc_description: str
    submit_datetime: str
    ordinance_code: str
    form_code: str
    issuer_edinet_code: Optional[str] = None
    subject_edinet_code: Optional[str] = None
    subsidiary_edinet_code: Optional[str] = None
    current_report_reason: Optional[str] = None
    # 対象会社情報 (コードリストから解決)
    issuer_name: str = ""
    issuer_sec_code: str = ""
    subject_name: str = ""
    subject_sec_code: str = ""
    subsidiary_name: str = ""
    # 分類結果
    event_category: str = EventCategory.OTHER.value
    priority: int = Priority.LOW.value
    tag: str = ""  # 補足タグ: "新規", "月次", "特例対象" 等
    # 状態
    is_read: bool = False
    memo: str = ""
    # メタデータ
    raw_json: str = ""
    xbrl_flag: bool = False
    pdf_flag: bool = False
    withdrawal_status: str = "0"
    # タイムスタンプ
    created_at: str = ""

    @property
    def ticker(self) -> str:
        """提出者の4桁証券コードを返す."""
        if self.sec_code and len(self.sec_code) >= 4:
            return self.sec_code[:4]
        return self.sec_code or ""

    @property
    def target_ticker(self) -> str:
        """対象会社の4桁証券コードを返す."""
        code = self.issuer_sec_code or self.subject_sec_code
        if code and len(code) >= 4:
            return code[:4]
        return code or ""

    @property
    def target_name(self) -> str:
        """対象会社名を返す (発行会社 > 対象会社 > 子会社の優先順)."""
        return self.issuer_name or self.subject_name or self.subsidiary_name or ""

    @property
    def target_display(self) -> str:
        """対象会社の表示文字列 (名前+ティッカー)."""
        name = self.target_name
        ticker = self.target_ticker
        if name and ticker:
            return f"{name}({ticker})"
        if name:
            return name
        # 名前が解決できない場合はEDINETコードを表示
        code = self.issuer_edinet_code or self.subject_edinet_code or self.subsidiary_edinet_code
        return f"[{code}]" if code else ""

    @property
    def priority_label(self) -> str:
        """優先度の表示ラベルを返す."""
        try:
            return PRIORITY_LABELS[Priority(self.priority)]
        except (ValueError, KeyError):
            return "低"

    @property
    def submit_time(self) -> str:
        """提出時刻 (HH:MM) を返す."""
        if " " in self.submit_datetime:
            return self.submit_datetime.split(" ", 1)[1][:5]
        return self.submit_datetime[:5] if self.submit_datetime else ""
