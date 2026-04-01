"""EDINET Monitor - イベント分類ロジック.

分類方針:
- docDescription (書類名) のキーワードマッチを主軸とする
- docTypeCode を補助的に使用する (実データ: 350=大量保有/変更, 240=公開買付届出, 270=公開買付報告, 220=自己株券買付)
- ルールは優先度順 (上にあるものが優先)
- 最も具体的なキーワードから判定することで誤分類を防ぐ
"""
from models import Document, EventCategory, Priority


# 分類ルール定義
# 上から順に評価し、最初にマッチしたものを採用
CLASSIFICATION_RULES = [
    # ---- 大量保有系 (最重要) ----
    # docTypeCode 350 は大量保有報告書と変更報告書の両方に使われるため、キーワードで区別
    {
        "category": EventCategory.HENKO_HOKOKU,
        "priority": Priority.CRITICAL,
        "keywords": ["変更報告書"],
        "exclude": [],
        "doc_type_codes": [],
    },
    {
        "category": EventCategory.TAIRYO_HOYU,
        "priority": Priority.CRITICAL,
        "keywords": ["大量保有報告書"],
        "exclude": ["変更"],
        "doc_type_codes": [],
    },
    # ---- 公開買付系 (最重要) ----
    # docTypeCode: 240=公開買付届出書, 270=公開買付報告書
    {
        "category": EventCategory.TOB,
        "priority": Priority.CRITICAL,
        "keywords": ["公開買付届出書", "公開買付報告書", "公開買付撤回届出書"],
        "exclude": [],
        "doc_type_codes": [],
    },
    {
        "category": EventCategory.TOB,
        "priority": Priority.HIGH,
        "keywords": ["意見表明報告書", "対質問回答報告書"],
        "exclude": [],
        "doc_type_codes": [],
    },
    # ---- 主要株主異動 ----
    {
        "category": EventCategory.KABUNUSHI_IDO,
        "priority": Priority.CRITICAL,
        "keywords": ["主要株主の異動", "主要株主異動"],
        "exclude": [],
        "doc_type_codes": [],
    },
    # ---- 希薄化系 (重要) ----
    {
        "category": EventCategory.MS_WARRANT,
        "priority": Priority.HIGH,
        "keywords": [
            "行使価額修正条項付",
            "行使価額修正条項付新株予約権",
            "MSワラント",
            "ムービング・ストライク",
        ],
        "exclude": [],
        "doc_type_codes": [],
    },
    {
        "category": EventCategory.CB,
        "priority": Priority.HIGH,
        "keywords": [
            "新株予約権付社債",
            "転換社債型",
            "転換社債",
        ],
        "exclude": [],
        "doc_type_codes": [],
    },
    {
        "category": EventCategory.DAISAN_WARIATE,
        "priority": Priority.HIGH,
        "keywords": ["第三者割当"],
        "exclude": [],
        "doc_type_codes": [],
    },
    {
        "category": EventCategory.ZOSHI,
        "priority": Priority.HIGH,
        "keywords": [
            "有価証券届出書",
            "発行登録追補書類",
        ],
        # 投信・外国投信関連は除外 (需給インパクト小)
        "exclude": ["内国投資信託", "外国投資信託", "内国投資証券", "外国投資証券"],
        "doc_type_codes": [],
    },
    # ---- 自己株取得 ----
    # docTypeCode: 220=自己株券買付状況報告書
    {
        "category": EventCategory.JIKO_KABU,
        "priority": Priority.HIGH,
        "keywords": ["自己株券買付状況報告書", "自己株式の取得", "自己株式取得"],
        "exclude": [],
        "doc_type_codes": [],
    },
    # ---- 臨時報告書 ----
    # 内国特定有価証券 (投信)・外国会社臨時報告書はノイズが多いため除外
    {
        "category": EventCategory.RINJI,
        "priority": Priority.MEDIUM,
        "keywords": ["臨時報告書"],
        "exclude": ["訂正", "内国特定有価証券", "外国会社臨時報告書"],
        "doc_type_codes": [],
    },
]

# 訂正書類判定キーワード
CORRECTION_KEYWORDS = ["訂正", "正誤"]


class Classifier:
    """開示書類のイベント分類器."""

    def __init__(self, enabled_categories: list[str] = None, skip_corrections: bool = True):
        """
        Args:
            enabled_categories: 監視対象カテゴリのリスト. Noneなら全カテゴリ有効.
            skip_corrections: 訂正書類をスキップするか.
        """
        self.enabled_categories = set(enabled_categories) if enabled_categories else None
        self.skip_corrections = skip_corrections

    def classify(self, doc: Document) -> tuple[str, int]:
        """書類を分類して (カテゴリ, 優先度) を返す.

        Returns:
            (event_category, priority) のタプル.
            対象外の場合は (EventCategory.OTHER, Priority.LOW) を返す.
        """
        desc = doc.doc_description or ""
        reason = doc.current_report_reason or ""
        combined = f"{desc} {reason}"

        # 訂正書類のスキップ
        if self.skip_corrections and self._is_correction(desc):
            return EventCategory.OTHER.value, Priority.LOW.value

        # 取り下げ済みはスキップ
        if doc.withdrawal_status not in ("0", "", None):
            return EventCategory.OTHER.value, Priority.LOW.value

        # ルールを順に評価
        for rule in CLASSIFICATION_RULES:
            category = rule["category"]

            # 有効カテゴリチェック
            if self.enabled_categories and category.value not in self.enabled_categories:
                continue

            # キーワードマッチ
            if not self._has_keyword(combined, rule["keywords"]):
                continue

            # 除外キーワードチェック
            if rule["exclude"] and self._has_keyword(desc, rule["exclude"]):
                continue

            # docTypeCodeでの追加確認 (設定されている場合のみ)
            if rule["doc_type_codes"] and doc.doc_type_code not in rule["doc_type_codes"]:
                continue

            return category.value, rule["priority"].value

        return EventCategory.OTHER.value, Priority.LOW.value

    def classify_with_tag(self, doc: Document) -> tuple[str, int, str]:
        """書類を分類して (カテゴリ, 優先度, タグ) を返す.

        タグは表示用の補足ラベル (例: "新規", "月次", "ルーティン")
        """
        category, priority = self.classify(doc)
        tag = self._compute_tag(doc, category, priority)
        priority = self._adjust_sub_priority(doc, category, priority, tag)
        return category, priority, tag

    def _compute_tag(self, doc: Document, category: str, priority: int) -> str:
        """重要度タグを生成."""
        desc = doc.doc_description or ""
        filer = doc.filer_name or ""

        if category == EventCategory.TAIRYO_HOYU.value:
            return "新規"  # 大量保有報告書は必ず初回5%超え

        if category == EventCategory.HENKO_HOKOKU.value:
            # 短期大量譲渡は特殊
            if "短期大量譲渡" in desc:
                return "短期大量譲渡"
            # 特例対象は機関投資家のルーティン寄り
            if "特例対象" in desc:
                return "特例対象"
            return ""

        if category == EventCategory.JIKO_KABU.value:
            # 自己株券買付状況報告書は月次のルーティン
            if "状況報告書" in desc:
                return "月次"
            return ""

        if category == EventCategory.RINJI.value:
            return ""

        if category == EventCategory.TOB.value:
            if "届出" in desc:
                return "新規"
            if "報告" in desc:
                return "完了"
            if "意見表明" in desc:
                return "意見"
            return ""

        return ""

    def _adjust_sub_priority(self, doc: Document, category: str, priority: int, tag: str) -> int:
        """タグに基づいて優先度を微調整."""
        # 大量保有 新規は最高優先
        if category == EventCategory.TAIRYO_HOYU.value:
            return Priority.CRITICAL.value

        # 変更報告書: 特例対象は1段階下げ (パッシブ運用の定期報告が多い)
        if tag == "特例対象":
            return min(priority + 1, Priority.MEDIUM.value)

        # 自己株取得: 月次報告は優先度下げ
        if tag == "月次":
            return Priority.MEDIUM.value

        # TOB新規は最高
        if category == EventCategory.TOB.value and tag == "新規":
            return Priority.CRITICAL.value

        return priority

    def is_target(self, doc: Document) -> bool:
        """監視対象の書類かどうか."""
        category, _ = self.classify(doc)
        return category != EventCategory.OTHER.value

    def adjust_priority_for_watchlist(
        self, priority: int, sec_code: str, watchlist: list[str]
    ) -> int:
        """ウォッチリスト銘柄なら優先度を1段階上げる."""
        if not watchlist or not sec_code:
            return priority
        ticker = sec_code[:4] if len(sec_code) >= 4 else sec_code
        if ticker in watchlist:
            return max(1, priority - 1)
        return priority

    def _has_keyword(self, text: str, keywords: list[str]) -> bool:
        """キーワードのいずれかがテキストに含まれるか."""
        return any(kw in text for kw in keywords)

    def _is_correction(self, description: str) -> bool:
        """訂正書類かどうか."""
        return any(kw in description for kw in CORRECTION_KEYWORDS)
