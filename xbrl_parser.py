"""EDINET Monitor - XBRL解析.

大量保有報告書・変更報告書等のXBRLから重要情報を自動抽出する。
"""
import html
import io
import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# 大量保有/変更報告書で抽出する要素名 → 表示ラベル
TAIRYO_FIELDS = {
    "NameOfIssuer": "発行者",
    "SecurityCodeOfIssuer": "証券コード",
    "HoldingRatioOfShareCertificatesEtc": "保有割合",
    "HoldingRatioOfShareCertificatesEtcPerLastReport": "前回保有割合",
    "TotalNumberOfStocksEtcHeld": "保有株式数",
    "TotalNumberOfOutstandingStocksEtc": "発行済株式総数",
    "PurposeOfHolding": "保有目的",
    "ActOfMakingImportantProposalEtcNA": "重要提案行為",
    "ActOfMakingImportantProposalEtc": "重要提案行為",
    "TotalNumberOfFilersAndJointHoldersCoverPage": "共同保有者数",
    "ReasonForFilingChangeReportCoverPage": "変更報告理由",
    "BaseDate": "基準日",
    "ListedOrOTC": "上場区分",
    "StockListing": "上場市場",
    "NumberOfResidualStocksHeld": "残株式数",
    "TotalAmountOfFundingForAcquisition": "取得資金総額",
}

# 公開買付で抽出する要素名
TOB_FIELDS = {
    "NameOfIssuer": "発行者",
    "SecurityCodeOfIssuer": "証券コード",
    "PriceOfTenderOffer": "買付価格",
    "NumberOfShareCertificatesEtcPlannedToBePurchased": "買付予定数",
    "TotalAmountOfPurchase": "買付総額",
    "PeriodOfTenderOffer": "買付期間",
}


class XbrlParser:
    """XBRL解析器."""

    def __init__(self, api_key: str, base_url: str = "https://api.edinet-fsa.go.jp/api/v2"):
        self.api_key = api_key
        self.base_url = base_url
        self._cache: dict[str, dict] = {}

    def extract(self, doc_id: str, event_category: str) -> Optional[dict]:
        """書類のXBRLから重要情報を抽出.

        Returns:
            抽出結果のdict. キーは日本語ラベル、値は文字列.
            失敗時はNone.
        """
        if doc_id in self._cache:
            return self._cache[doc_id]

        try:
            root = self._download_xbrl(doc_id)
            if root is None:
                return None

            if event_category in ("大量保有報告書", "変更報告書"):
                result = self._parse_tairyo(root, event_category)
            elif event_category == "公開買付":
                result = self._parse_tob(root)
            else:
                result = self._parse_generic(root)

            self._cache[doc_id] = result
            return result

        except Exception as e:
            logger.error("XBRL解析失敗 (%s): %s", doc_id, e)
            return None

    def _download_xbrl(self, doc_id: str) -> Optional[ET.Element]:
        """XBRLデータをダウンロードしてパース."""
        url = f"{self.base_url}/documents/{doc_id}"
        params = {"type": "1", "Subscription-Key": self.api_key}

        response = requests.get(url, params=params, timeout=60)
        if response.status_code != 200 or response.content[:2] != b"PK":
            logger.warning("XBRLダウンロード失敗: %s (status=%s)", doc_id, response.status_code)
            return None

        zf = zipfile.ZipFile(io.BytesIO(response.content))
        xbrl_files = [n for n in zf.namelist() if n.endswith(".xbrl")]
        if not xbrl_files:
            logger.warning("XBRLファイルが見つからない: %s", doc_id)
            return None

        with zf.open(xbrl_files[0]) as f:
            tree = ET.parse(io.BytesIO(f.read()))
            return tree.getroot()

    def _parse_tairyo(self, root: ET.Element, category: str) -> dict:
        """大量保有報告書/変更報告書のXBRLを解析."""
        raw = self._extract_elements(root, TAIRYO_FIELDS)
        result = {}

        # 発行者情報
        if "発行者" in raw:
            result["発行者"] = raw["発行者"]
        if "証券コード" in raw:
            result["証券コード"] = raw["証券コード"]
        if "上場市場" in raw:
            result["上場市場"] = raw["上場市場"]

        # 保有割合 (複数値がある場合、最初の値 = メイン提出者)
        ratios = self._extract_all_values(root, "HoldingRatioOfShareCertificatesEtc")
        prev_ratios = self._extract_all_values(root, "HoldingRatioOfShareCertificatesEtcPerLastReport")

        if ratios:
            # 最初の値 = 主たる提出者の保有割合
            current = float(ratios[0])
            result["保有割合"] = f"{current * 100:.2f}%"

            if prev_ratios:
                prev = float(prev_ratios[0])
                result["前回保有割合"] = f"{prev * 100:.2f}%"
                diff = (current - prev) * 100
                sign = "+" if diff >= 0 else ""
                result["増減"] = f"{sign}{diff:.2f}%"

                # 初回5%超え判定
                if prev == 0 and current > 0.05:
                    result["初回5%超え"] = "はい"

            # 合計値 (最後の値がある場合)
            if len(ratios) > 1:
                total = float(ratios[-1])
                if total != current:
                    result["合計保有割合"] = f"{total * 100:.2f}%"

        # 保有株数
        held_values = self._extract_all_values(root, "TotalNumberOfStocksEtcHeld")
        if held_values:
            result["保有株式数"] = f"{int(held_values[0]):,}"

        outstanding = self._extract_all_values(root, "TotalNumberOfOutstandingStocksEtc")
        if outstanding:
            result["発行済株式総数"] = f"{int(outstanding[0]):,}"

        # 保有目的
        if "保有目的" in raw:
            result["保有目的"] = raw["保有目的"][:100]

        # 重要提案行為
        for key in ("重要提案行為",):
            if key in raw:
                val = raw[key]
                result["重要提案行為"] = val[:100]
                if "該当" not in val and "なし" not in val:
                    result["重要提案行為"] = f"⚠ {val[:100]}"

        # 共同保有者
        if "共同保有者数" in raw:
            n = int(raw["共同保有者数"])
            result["共同保有者"] = f"{n}名" if n > 1 else "なし"

        # 変更報告理由
        if "変更報告理由" in raw:
            result["変更理由"] = raw["変更報告理由"][:120]

        # 取得資金
        if "取得資金総額" in raw:
            amount = int(raw["取得資金総額"])
            if amount >= 100_000_000:
                result["取得資金"] = f"{amount / 100_000_000:.1f}億円"
            else:
                result["取得資金"] = f"{amount / 10_000:.0f}万円"

        # 基準日
        if "基準日" in raw:
            result["基準日"] = raw["基準日"]

        return result

    def _parse_tob(self, root: ET.Element) -> dict:
        """公開買付届出書のXBRLを解析."""
        raw = self._extract_elements(root, TOB_FIELDS)
        result = {}
        for label, value in raw.items():
            result[label] = value[:150]
        return result

    def _parse_generic(self, root: ET.Element) -> dict:
        """汎用XBRL解析 (増資/CB/第三者割当/自己株取得等).

        有価証券届出書等はTextBlock要素にHTMLが含まれるため、
        HTMLからテキストを抽出して表示する。
        """
        result = {}

        # まず構造化データを試す
        simple_fields = {
            "CompanyNameCoverPage": "会社名",
            "SecurityCodeDEI": "証券コード",
            "TypesOfSecuritiesToRegisterForOfferingOrDistributionCoverPage": "届出対象有価証券",
        }
        raw = self._extract_elements(root, simple_fields)
        result.update(raw)

        # TextBlock要素からHTMLテキストを抽出
        textblock_fields = {
            "NewIssuanceOfSharesTextBlock": "新規発行株式",
            "MethodOfPublicOfferingTextBlock": "募集の方法",
            "TermsOfPublicOfferingNewIssuanceOfSharesTextBlock": "募集の条件",
            "AmountOfNetProceedsFromNewIssuanceTextBlock": "手取金の額",
            "UseOfNetProceedsTextBlock": "手取金の使途",
            "InformationAboutPartiesToBeAllottedToTextBlock": "割当先情報",
            "MajorShareholdersAfterThirdPartyAllotmentTextBlock": "割当後大株主",
            "InformationAboutLargeVolumeThirdPartyAllotmentTextBlock": "大規模第三者割当",
            # 自己株取得
            "AcquisitionOfTreasuryStockTextBlock": "自己株式取得",
            "NumberOfSharesAcquired": "取得株式数",
            "TotalAmountOfSharesAcquired": "取得総額",
            # CB/新株予約権
            "NewShareSubscriptionRightsTextBlock": "新株予約権",
            "TermsOfNewShareSubscriptionRightsTextBlock": "新株予約権の条件",
            "ConversionPriceTextBlock": "転換価格",
        }

        for elem in root.iter():
            tag = elem.tag.split("}")[-1]
            if tag in textblock_fields:
                label = textblock_fields[tag]
                if label in result:
                    continue
                # HTMLからテキストを抽出
                raw_html = elem.text or ""
                text = self._strip_html(raw_html)
                if text:
                    result[label] = text[:200]

        return result

    @staticmethod
    def _strip_html(html_text: str) -> str:
        """HTMLタグを除去してプレーンテキストにする."""
        text = re.sub(r"<br\s*/?>", "\n", html_text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        # 先頭のセクション番号やスペースを除去
        text = re.sub(r"^[\s\d．.]+", "", text)
        return text

    def _extract_elements(self, root: ET.Element, field_map: dict) -> dict:
        """XBRLルートから指定要素を抽出. 最初に見つかった値を使用."""
        result = {}
        for elem in root.iter():
            if not elem.text or not elem.text.strip():
                continue
            tag = elem.tag.split("}")[-1]
            if tag in field_map and field_map[tag] not in result:
                result[field_map[tag]] = elem.text.strip()
        return result

    def _extract_all_values(self, root: ET.Element, element_name: str) -> list[str]:
        """指定要素名の全値をリストで返す."""
        values = []
        for elem in root.iter():
            tag = elem.tag.split("}")[-1]
            if tag == element_name and elem.text and elem.text.strip():
                values.append(elem.text.strip())
        return values
