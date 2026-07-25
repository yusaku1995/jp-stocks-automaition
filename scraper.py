# -*- coding: utf-8 -*-
"""
日本株指標収集スクリプト（無料データ版）

取得元
- 当日株価・出来高・25日線: Yahoo Finance（yfinance）
- 財務指標: IRBANKの配布CSVのみ
- 信用倍率: JPX「銘柄別信用取引週末残高」の最新公開ファイル

旧版との互換性
- tickers.txt を読み込む
- OFFSET / MAX_TICKERS による分割実行に対応
- metrics.csv の列名と並びを維持
- 最新営業日の日足が取れない場合は metrics.csv を更新しない
"""

from __future__ import annotations

import csv
import io
import math
import os
import re
import sys
import tempfile
import time
import unicodedata
from html import unescape
import zipfile
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import requests
import yfinance as yf
import pdfplumber
from bs4 import BeautifulSoup

SCRIPT_VERSION = "YAHOO_FREE_R8_20260725"



# ====== 設定 ======
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
JPX_MARGIN_PAGE = "https://www.jpx.co.jp/markets/statistics-equities/margin/05.html"
JPX_MARGIN_PAGE_EN = "https://www.jpx.co.jp/english/markets/statistics-equities/margin/05.html"

CSV_PL = "fy-profit-and-loss.csv"
CSV_BS = "fy-balance-sheet.csv"
CSV_DIV = "fy-stock-dividend.csv"
CSV_QQ = "qq-yoy-operating-income.csv"
CSV_PS = "fy-per-share.csv"
CSV_ALL = "fy-data-all.csv"

OUTPUT_COLUMNS = [
    "code",
    "per",
    "pbr",
    "roe_pct",
    "equity_ratio_pct",
    "dividend_yield_pct",
    "op_income_yoy_pct",
    "credit_ratio",
    "vol5",
    "vol25",
    "volratio_5_25",
    "deviation_25ma_pct",
]

JST = ZoneInfo("Asia/Tokyo")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
YAHOO_PERIOD = os.getenv("YAHOO_PERIOD", "2y")
YAHOO_CHUNK_SIZE = max(1, int(os.getenv("YAHOO_CHUNK_SIZE", "40")))
YAHOO_RETRIES = max(1, int(os.getenv("YAHOO_RETRIES", "3")))
IRBANK_RETRIES = max(1, int(os.getenv("IRBANK_RETRIES", "3")))
MARKET_DATA_READY_TIME = os.getenv("MARKET_DATA_READY_TIME", "16:15")
STRICT_JPX = os.getenv("STRICT_JPX", "0").strip() == "1"
JPX_MARGIN_URL_OVERRIDE = os.getenv("JPX_MARGIN_URL", "").strip()
MIN_JPX_PARSED_ROWS = max(100, int(os.getenv("MIN_JPX_PARSED_ROWS", "100")))

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en-US;q=0.8,en;q=0.6",
        "Cache-Control": "no-cache",
    }
)


# ====== 共通ヘルパー ======
def polite_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def normalize_code_line(line: str) -> str:
    token = re.split(r"[\s,\t]+", line.strip())[0] if line else ""
    token = unicodedata.normalize("NFKC", token)
    return re.sub(r"[^0-9A-Za-z]", "", token).upper()


def yahoo_symbol(code: str) -> str:
    return f"{code}.T"


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    text = unicodedata.normalize("NFKC", str(value)).strip()
    if text in {"", "-", "--", "---", "None", "null", "nan", "NaN", "－", "―"}:
        return None

    negative = text.startswith(("△", "▲")) or (
        text.startswith("(") and text.endswith(")")
    )
    text = text.replace(",", "")
    text = text.replace("△", "-").replace("▲", "-")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None

    try:
        number = float(match.group(0))
    except (TypeError, ValueError):
        return None

    if negative and number > 0:
        number = -number
    return number if math.isfinite(number) else None


def safe_div(numerator: Any, denominator: Any, multiplier: float = 1.0) -> float | None:
    n = safe_float(numerator)
    d = safe_float(denominator)
    if n is None or d in (None, 0):
        return None
    return n / d * multiplier


def output_value(value: Any, digits: int = 4) -> str | int | float:
    number = safe_float(value)
    if number is None:
        return ""
    rounded = round(number, digits)
    if float(rounded).is_integer():
        return int(rounded)
    return rounded


def parse_ready_time(value: str) -> dt_time:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", value.strip())
    if not match:
        raise ValueError("MARKET_DATA_READY_TIME must be HH:MM, e.g. 16:15")
    hour, minute = int(match.group(1)), int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("MARKET_DATA_READY_TIME is outside the valid clock range")
    return dt_time(hour=hour, minute=minute)


# ====== 東証営業日の判定 ======
def expected_market_date(now_jst: datetime | None = None) -> date:
    """
    本日が東証営業日の場合は、引け後の反映待ち時刻を過ぎてから本日を返す。
    土日祝日は、直近の東証営業日を返す。
    """
    now_jst = now_jst or datetime.now(JST)
    today = now_jst.date()
    ready_time = parse_ready_time(MARKET_DATA_READY_TIME)

    calendar = xcals.get_calendar("XTKS")
    start = pd.Timestamp(today - timedelta(days=30))
    end = pd.Timestamp(today)
    sessions = calendar.sessions_in_range(start, end)
    session_dates = {pd.Timestamp(session).date() for session in sessions}

    if today in session_dates:
        if now_jst.time().replace(tzinfo=None) < ready_time:
            raise RuntimeError(
                f"本日の東証日足が確定する前です。"
                f"{today.isoformat()} {ready_time.strftime('%H:%M')} JST以降に実行してください。"
            )
        return today

    past_sessions = [session_date for session_date in session_dates if session_date <= today]
    if not past_sessions:
        raise RuntimeError("直近の東証営業日を判定できませんでした")
    return max(past_sessions)


# ====== Yahoo Finance（日足・出来高） ======
def _extract_symbol_frame(downloaded: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if downloaded is None or downloaded.empty:
        return pd.DataFrame()

    frame: pd.DataFrame
    if isinstance(downloaded.columns, pd.MultiIndex):
        level0 = {str(value) for value in downloaded.columns.get_level_values(0)}
        level1 = {str(value) for value in downloaded.columns.get_level_values(1)}

        if symbol in level0:
            frame = downloaded[symbol].copy()
        elif symbol in level1:
            frame = downloaded.xs(symbol, axis=1, level=1).copy()
        else:
            return pd.DataFrame()
    else:
        frame = downloaded.copy()

    if frame.empty:
        return frame

    frame = frame.dropna(how="all")
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame[frame.index.notna()].sort_index()
    return frame


def download_yahoo_chunk(symbols: list[str]) -> dict[str, pd.DataFrame]:
    last_error: Exception | None = None

    for attempt in range(1, YAHOO_RETRIES + 1):
        try:
            downloaded = yf.download(
                tickers=symbols,
                period=YAHOO_PERIOD,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                actions=True,
                threads=False,
                # 価格補修機能は追加依存が必要なため無効化。
                repair=False,
                keepna=False,
                progress=False,
                timeout=REQUEST_TIMEOUT,
                multi_level_index=True,
            )

            result = {
                symbol: _extract_symbol_frame(downloaded, symbol)
                for symbol in symbols
            }
            if any(not frame.empty for frame in result.values()):
                return result
            raise RuntimeError("Yahoo Finance returned no usable rows")

        except Exception as exc:  # yfinance側の例外型変更にも耐える
            last_error = exc
            print(
                f"[WARN] Yahoo download attempt {attempt}/{YAHOO_RETRIES}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            polite_sleep(2.0 * attempt)

    raise RuntimeError(f"Yahoo Finance download failed: {last_error}")


def download_yahoo_all(codes: list[str]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    symbols = [yahoo_symbol(code) for code in codes]

    for start in range(0, len(symbols), YAHOO_CHUNK_SIZE):
        chunk = symbols[start : start + YAHOO_CHUNK_SIZE]
        frames.update(download_yahoo_chunk(chunk))
        if start + YAHOO_CHUNK_SIZE < len(symbols):
            polite_sleep(1.5)

    return frames


def yahoo_metrics(
    code: str,
    frame: pd.DataFrame,
    expected_date: date,
) -> dict[str, Any]:
    if frame.empty:
        raise RuntimeError(f"{code}: Yahoo Financeの日足が0件です")
    if "Close" not in frame.columns or "Volume" not in frame.columns:
        raise RuntimeError(
            f"{code}: Yahoo Financeの必要列がありません: {list(frame.columns)}"
        )

    work = frame.copy()
    work["Close"] = pd.to_numeric(work["Close"], errors="coerce")
    work["Volume"] = pd.to_numeric(work["Volume"], errors="coerce")
    if "Dividends" in work.columns:
        work["Dividends"] = pd.to_numeric(
            work["Dividends"],
            errors="coerce",
        ).fillna(0)
    work = work[work["Close"].notna() & (work["Close"] > 0)].copy()
    if work.empty:
        raise RuntimeError(f"{code}: Yahoo Financeに有効な終値がありません")

    latest_timestamp = pd.Timestamp(work.index.max())
    if latest_timestamp.tzinfo is not None:
        latest_timestamp = latest_timestamp.tz_convert(JST).tz_localize(None)
    latest_date = latest_timestamp.date()

    if latest_date != expected_date:
        raise RuntimeError(
            f"{code}: stale Yahoo data "
            f"expected={expected_date.isoformat()} received={latest_date.isoformat()}"
        )

    closes = work["Close"]
    volumes = work["Volume"].fillna(0)
    latest_price = float(closes.iloc[-1])

    vol5 = int(round(float(volumes.tail(5).mean()))) if len(volumes) >= 5 else None
    vol25 = int(round(float(volumes.tail(25).mean()))) if len(volumes) >= 25 else None
    volume_ratio = safe_div(vol5, vol25)

    deviation_25ma = None
    if len(closes) >= 25:
        ma25 = float(closes.tail(25).mean())
        deviation_25ma = safe_div(latest_price - ma25, ma25, 100.0)

    trailing_dividend = None
    if "Dividends" in work.columns:
        cutoff = pd.Timestamp(expected_date - timedelta(days=370))
        dividend_rows = work.loc[work.index >= cutoff, "Dividends"]
        trailing_dividend = float(dividend_rows.sum())
        if abs(trailing_dividend) < 1e-12:
            trailing_dividend = 0.0

    return {
        "latest_date": latest_date,
        "latest_price": latest_price,
        "trailing_dividend": trailing_dividend,
        "vol5": vol5,
        "vol25": vol25,
        "volratio_5_25": volume_ratio,
        "deviation_25ma_pct": deviation_25ma,
    }


# ====== IRBANK CSV（HTMLは使用しない） ======
def _norm_label(value: Any) -> str:
    text = "" if value is None else unicodedata.normalize("NFKC", str(value))
    text = re.sub(r"（.*?）|\(.*?\)", "", text)
    return re.sub(r"[\s　,/％%円¥\-–—―]", "", text)


EPS_KEYS = [
    "EPS",
    "EPS(円)",
    "EPS（円）",
    "1株当たり利益",
    "1株当たり当期純利益",
    "1株当たり当期純利益(円)",
    "1株当たり当期純利益（円）",
    "1株当たり純利益",
]
BPS_KEYS = [
    "BPS",
    "BPS(円)",
    "BPS（円）",
    "1株当たり純資産",
    "1株当たり純資産(円)",
    "1株当たり純資産（円）",
    "1株純資産",
]
DPS_KEYS = [
    "DPS",
    "年間配当",
    "1株配当",
    "1株配当金",
    "配当金",
    "配当(円)",
    "配当（円）",
    "1株当たり配当金",
]
EQ_KEYS = [
    "自己資本",
    "自己資本合計",
    "株主資本",
    "株主資本合計",
    "純資産",
    "純資産合計",
    "純資産の部合計",
]
AS_KEYS = ["総資産", "資産合計", "資産総額", "資産の部合計"]
NI_KEYS = [
    "当期純利益",
    "親会社株主に帰属する当期純利益",
    "親会社株主に帰属する純利益",
    "純利益",
]
ROE_KEYS = ["ROE", "自己資本利益率"]
EQR_KEYS = ["自己資本比率"]


def _label_matches(value: Any, keys: list[str]) -> bool:
    heading = _norm_label(value)
    if not heading:
        return False
    normalized_keys = [_norm_label(key) for key in keys]
    return any(
        key and (key == heading or key in heading)
        for key in normalized_keys
    )


def _label_matches_exact(value: Any, keys: list[str]) -> bool:
    heading = _norm_label(value)
    if not heading:
        return False
    return heading in {_norm_label(key) for key in keys if _norm_label(key)}


def _looks_like_period(value: Any) -> bool:
    """
    年度・年月・日付を財務数値として誤読しない。
    """
    if value is None:
        return False

    text = unicodedata.normalize("NFKC", str(value)).strip()
    compact = re.sub(r"\s+", "", text)

    if re.fullmatch(r"20\d{2}", compact):
        return True
    if re.search(
        r"20\d{2}(?:[/.\-年])\d{1,2}(?:(?:[/.\-月])\d{1,2})?(?:日|期)?",
        compact,
    ):
        return True
    if re.fullmatch(r"(?:FY)?20\d{2}(?:Q[1-4]|[1-4]Q)?", compact, re.IGNORECASE):
        return True
    return False


def _numeric_cell(value: Any) -> float | None:
    if _looks_like_period(value):
        return None
    return safe_float(value)


def _iter_metric_candidates(
    rows: list[list[str]],
    keys: list[str],
    *,
    exact: bool,
):
    matcher = _label_matches_exact if exact else _label_matches

    # A: 項目が縦に並ぶ形式
    for row in rows:
        if not row or not matcher(row[0], keys):
            continue
        for value in reversed(row[1:]):
            number = _numeric_cell(value)
            if number is not None:
                yield number, value

    # B: 項目がヘッダー列に並ぶ形式
    header_limit = min(8, len(rows))
    for header_index in range(header_limit):
        header = rows[header_index]
        for column, heading in enumerate(header):
            if not matcher(heading, keys):
                continue
            for data_row in reversed(rows[header_index + 1:]):
                if column >= len(data_row):
                    continue
                value = data_row[column]
                number = _numeric_cell(value)
                if number is not None:
                    yield number, value


def metric_value(
    rows: list[list[str]] | None,
    keys: list[str],
) -> float | None:
    """
    IRBANK CSVの縦型・横型に対応。
    完全一致を先に調べ、年度セルは採用しない。
    """
    if not rows:
        return None

    for exact in (True, False):
        for number, _ in _iter_metric_candidates(rows, keys, exact=exact):
            return number
    return None


DIVIDEND_EXACT_KEYS = [
    "DPS",
    "年間配当",
    "年間配当金",
    "1株配当",
    "1株配当金",
    "1株当たり配当金",
    "1株当たり年間配当金",
    "配当合計",
    "合計",
]


def dividend_per_share(rows: list[list[str]] | None) -> float | None:
    """
    配当CSV専用。
    大見出しを年度列と誤認せず、年間合計列を優先する。
    """
    if not rows:
        return None

    for number, _ in _iter_metric_candidates(
        rows,
        DIVIDEND_EXACT_KEYS,
        exact=True,
    ):
        if 0 <= number <= 100000:
            return number

    narrow_keys = [
        "DPS",
        "年間配当",
        "1株配当",
        "1株当たり配当金",
    ]
    for number, _ in _iter_metric_candidates(rows, narrow_keys, exact=False):
        if 0 <= number <= 100000:
            return number

    non_period_numbers: list[float] = []
    for row in rows:
        for cell in row:
            number = _numeric_cell(cell)
            if number is not None:
                non_period_numbers.append(number)
    if non_period_numbers and all(number == 0 for number in non_period_numbers):
        return 0.0

    return None

def get_csv(code: str, path: str) -> list[list[str]] | None:
    """数字4桁・英数字コードの両方を試す。404は欠損として扱う。"""
    url = IR_CSV.format(code=code, path=path)

    for attempt in range(1, IRBANK_RETRIES + 1):
        try:
            response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 404:
                print(f"[MISS] {url} -> HTTP 404", flush=True)
                return None
            if response.status_code != 200:
                print(
                    f"[WARN] {url} -> HTTP {response.status_code} "
                    f"attempt={attempt}/{IRBANK_RETRIES}",
                    flush=True,
                )
            else:
                # IRBANKのCSVはUTF-8。apparent_encodingの誤判定を避ける。
                response.encoding = "utf-8-sig"
                rows = list(csv.reader(io.StringIO(response.text)))
                if len(rows) >= 2:
                    print(f"[OK] {url} rows={len(rows)}", flush=True)
                    return rows
                print(f"[WARN] {url} -> CSV too short", flush=True)
        except requests.RequestException as exc:
            print(
                f"[WARN] {url} -> {type(exc).__name__}: {exc} "
                f"attempt={attempt}/{IRBANK_RETRIES}",
                flush=True,
            )

        polite_sleep(1.5 * attempt)

    print(f"[FAIL] {url}", flush=True)
    return None


def _first_available_metric(
    sources: list[list[list[str]] | None],
    keys: list[str],
) -> float | None:
    for rows in sources:
        value = metric_value(rows, keys)
        if value is not None:
            return value
    return None


def fetch_financial_values(code: str) -> dict[str, float | None]:
    pl = get_csv(code, CSV_PL)
    bs = get_csv(code, CSV_BS)
    dividend = get_csv(code, CSV_DIV)
    per_share = get_csv(code, CSV_PS)

    # 個別CSVにない項目を一括CSVで補完する。必要なときだけ1回取得。
    need_all = any(
        value is None
        for value in (
            metric_value(pl, EPS_KEYS),
            metric_value(pl, NI_KEYS),
            metric_value(bs, BPS_KEYS),
            metric_value(bs, EQ_KEYS),
            metric_value(bs, AS_KEYS),
            dividend_per_share(dividend),
        )
    )
    all_rows = get_csv(code, CSV_ALL) if need_all else None

    eps = _first_available_metric([pl, per_share, all_rows], EPS_KEYS)
    bps = _first_available_metric([bs, per_share, all_rows], BPS_KEYS)
    profit = _first_available_metric([pl, all_rows], NI_KEYS)
    equity = _first_available_metric([bs, all_rows], EQ_KEYS)
    assets = _first_available_metric([bs, all_rows], AS_KEYS)
    dps = dividend_per_share(dividend)
    if dps is None:
        dps = _first_available_metric([per_share, all_rows], DPS_KEYS)
    roe_pct = _first_available_metric([pl, all_rows], ROE_KEYS)
    equity_ratio_pct = _first_available_metric([bs, all_rows], EQR_KEYS)

    found = {
        "eps": eps,
        "bps": bps,
        "profit": profit,
        "equity": equity,
        "assets": assets,
        "dps": dps,
        "roe_pct": roe_pct,
        "equity_ratio_pct": equity_ratio_pct,
    }
    print(
        "[DEBUG-IRBANK] "
        + code
        + " "
        + " ".join(
            f"{key}={'yes' if value is not None else 'no'}"
            for key, value in found.items()
        ),
        flush=True,
    )
    return found


def fetch_opinc_yoy(code: str) -> float | None:
    rows = get_csv(code, CSV_QQ)
    if not rows:
        return None

    # 専用CSVは先頭列が期間、2列目が前年比。
    for row in reversed(rows[1:]):
        if len(row) < 2:
            continue
        value = row[1]
        if _looks_like_period(value):
            continue
        number = safe_float(value)
        if number is not None:
            return number

    # レイアウト変更時の限定フォールバック。
    for exact in (True, False):
        for number, raw in _iter_metric_candidates(
            rows,
            [
                "営業利益前年同期比",
                "営業利益前年比",
                "前年同期比",
                "前年比",
            ],
            exact=exact,
        ):
            if not _looks_like_period(raw):
                return number

    return None


# ====== JPX週次信用残高 ======
def _parse_date_score(text: str) -> int:
    normalized = unicodedata.normalize("NFKC", text)
    patterns = [
        r"(20\d{2})[年/_\-.]?(\d{1,2})[月/_\-.]?(\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            try:
                parsed = date(
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3)),
                )
                return int(parsed.strftime("%Y%m%d"))
            except ValueError:
                pass
    return 0


def _clean_embedded_url(value: str, base_url: str) -> str:
    cleaned = unescape(value.strip().strip("'\""))
    cleaned = cleaned.replace("\\/", "/")
    cleaned = re.sub(
        r"\\u002[fF]",
        "/",
        cleaned,
    )
    cleaned = cleaned.rstrip("\\")
    return urljoin(base_url, cleaned)


STATIC_EXTENSIONS = (
    ".css",
    ".js",
    ".mjs",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".map",
    ".xml",
)


def _is_static_asset_url(url: str) -> bool:
    path = url.split("?", 1)[0].lower()
    return path.endswith(STATIC_EXTENSIONS)


def _candidate_urls_from_html(
    html_text: str,
    base_url: str,
) -> list[tuple[int, int, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    raw_candidates: list[tuple[str, str, int]] = []

    for order, tag in enumerate(
        soup.find_all(["a", "button", "iframe", "object", "embed"])
    ):
        context = tag.get_text(" ", strip=True)
        parent = tag.find_parent(["tr", "li", "section", "article", "div"])
        if parent is not None:
            parent_text = parent.get_text(" ", strip=True)
            if len(parent_text) <= 500:
                context = f"{context} {parent_text}"

        for attribute in (
            "href",
            "src",
            "data",
            "data-href",
            "data-url",
            "data-download",
            "data-file",
            "onclick",
        ):
            value = tag.get(attribute)
            if value:
                raw_candidates.append((str(value), context, order))

    normalized_html = unescape(html_text).replace("\\/", "/")
    normalized_html = re.sub(r"\\u002[fF]", "/", normalized_html)
    embedded_pattern = re.compile(
        r"""(?P<url>
            (?:https?://|/|\.\.?/)
            [^\s"'<>]+?
            (?:
                \.(?:xlsx?|csv|zip|pdf)(?:\?[^\s"'<>]*)?
                |
                (?:-att|_att)/[^\s"'<>]+
            )
        )""",
        re.VERBOSE | re.IGNORECASE,
    )
    for order, match in enumerate(
        embedded_pattern.finditer(normalized_html),
        start=100000,
    ):
        raw_candidates.append((match.group("url"), "", order))

    extension_scores = {
        ".xlsx": 80,
        ".xls": 70,
        ".csv": 60,
        ".zip": 50,
        ".pdf": 40,
    }

    result: list[tuple[int, int, str]] = []
    seen: set[str] = set()

    for raw_url, context, order in raw_candidates:
        possible_values = [raw_url]
        possible_values.extend(re.findall(r"""["']([^"']+)["']""", raw_url))

        for possible in possible_values:
            url = _clean_embedded_url(possible, base_url)
            if not url.startswith(("http://", "https://")):
                continue
            if url in seen or url.rstrip("/") == base_url.rstrip("/"):
                continue
            if _is_static_asset_url(url):
                continue

            lower_path = url.split("?", 1)[0].lower()
            extension = next(
                (ext for ext in extension_scores if lower_path.endswith(ext)),
                "",
            )

            normalized_context = _norm_label(f"{context} {url}")
            has_margin_context = any(
                key in normalized_context
                for key in (
                    "信用取引週末残高",
                    "銘柄別信用",
                    "endofweekoutstandingmargin",
                    "applicationbased",
                    "申込分",
                )
            )
            has_attachment_path = any(
                marker in lower_path
                for marker in (
                    "-att/",
                    "_att/",
                    "/download/",
                    "/files/",
                )
            )

            if not extension and not has_attachment_path:
                continue

            date_score = _parse_date_score(f"{context} {url}")
            score = extension_scores.get(extension, 10)
            if has_margin_context:
                score += 100
            if has_attachment_path:
                score += 25

            seen.add(url)
            result.append((date_score, score, url))

    return result

def discover_jpx_margin_candidates() -> list[str]:
    if JPX_MARGIN_URL_OVERRIDE:
        return [JPX_MARGIN_URL_OVERRIDE]

    all_candidates: list[tuple[int, int, str]] = []
    visited_pages: set[str] = set()
    page_queue = [JPX_MARGIN_PAGE, JPX_MARGIN_PAGE_EN]

    # JPXページはJavaScript生成の場合があるため、iframe先も1階層だけ確認。
    while page_queue and len(visited_pages) < 8:
        page_url = page_queue.pop(0)
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)

        response = SESSION.get(page_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        all_candidates.extend(
            _candidate_urls_from_html(response.text, page_url)
        )

        soup = BeautifulSoup(response.text, "html.parser")
        for iframe in soup.find_all("iframe", src=True):
            iframe_url = urljoin(page_url, iframe.get("src", ""))
            if iframe_url.startswith("https://www.jpx.co.jp/"):
                page_queue.append(iframe_url)

    if not all_candidates:
        raise RuntimeError(
            "JPXページから信用残高のダウンロード候補を発見できませんでした"
        )

    all_candidates.sort(reverse=True)
    ordered: list[str] = []
    seen: set[str] = set()
    for _, _, url in all_candidates:
        if url not in seen:
            ordered.append(url)
            seen.add(url)
    return ordered


def _payload_kind(url: str, content_type: str, content: bytes) -> str:
    lower_url = url.split("?", 1)[0].lower()
    lower_type = content_type.lower()

    if content.startswith(b"%PDF") or "application/pdf" in lower_type or lower_url.endswith(".pdf"):
        return "pdf"
    if content.startswith(b"\xD0\xCF\x11\xE0") or lower_url.endswith(".xls"):
        return "excel"
    if content.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                names = set(archive.namelist())
                if "[Content_Types].xml" in names and any(
                    name.startswith("xl/") for name in names
                ):
                    return "excel"
        except Exception:
            pass
        return "zip"
    if (
        "csv" in lower_type
        or lower_url.endswith(".csv")
        or content[:200].count(b",") >= 2
    ):
        return "csv"
    if "spreadsheet" in lower_type or "excel" in lower_type or lower_url.endswith(".xlsx"):
        return "excel"
    return "unknown"


def _normalize_jpx_security_code(value: Any) -> str:
    """
    JPX PDFの新証券コードは、普通株式の場合
    3674 -> 36740、215A -> 215A0 のように末尾0が付く。
    metrics.csv/tickers.txtの4文字コードへ戻す。
    """
    code = normalize_code_line(str(value))
    if len(code) == 5 and code.endswith("0"):
        return code[:-1]
    return code


def _parse_jpx_pdf_text(content: bytes) -> dict[str, float]:
    """
    JPX週末残高PDFの本文行を直接解析する。

    行の主な並び:
      銘柄名 36740 JP... 売残高 売前週比 買残高 買前週比 ...
    """
    ratios: dict[str, float] = {}

    line_pattern = re.compile(
        r"(?P<code>[0-9A-Z]{5})\s+"
        r"JP[0-9A-Z]{10}\s+"
        r"(?P<short>[\d,]+)\s+"
        r"(?:(?:▲|△|-)\s*)?[\d,]+\s+"
        r"(?P<long>[\d,]+)(?:\s|$)"
    )

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            page_text = unicodedata.normalize("NFKC", page_text)

            for line in page_text.splitlines():
                match = line_pattern.search(line)
                if not match:
                    continue

                code = _normalize_jpx_security_code(match.group("code"))
                if not re.fullmatch(r"[0-9A-Z]{4}", code):
                    continue

                short_balance = safe_float(match.group("short"))
                long_balance = safe_float(match.group("long"))
                ratio = safe_div(long_balance, short_balance)

                if ratio is not None and ratio >= 0:
                    ratios[code] = ratio

    if "3674" in ratios:
        print(
            f"[DEBUG-JPX-3674] credit_ratio={round(ratios['3674'], 4)}",
            flush=True,
        )

    return ratios


def _read_pdf_tables(content: bytes) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                if table:
                    frames.append(pd.DataFrame(table))
    return frames


def _read_jpx_payload(
    url: str,
    content_type: str,
    content: bytes,
) -> list[pd.DataFrame]:
    kind = _payload_kind(url, content_type, content)
    frames: list[pd.DataFrame] = []

    if kind == "csv":
        for encoding in ("cp932", "utf-8-sig", "utf-8"):
            try:
                frames.append(
                    pd.read_csv(
                        io.BytesIO(content),
                        header=None,
                        encoding=encoding,
                    )
                )
                return frames
            except Exception:
                continue
        raise RuntimeError("JPX CSVを読み込めませんでした")

    if kind == "pdf":
        return _read_pdf_tables(content)

    if kind == "zip":
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for name in archive.namelist():
                payload = archive.read(name)
                lower_name = name.lower()
                if lower_name.endswith((".xlsx", ".xls")):
                    excel = pd.read_excel(
                        io.BytesIO(payload),
                        sheet_name=None,
                        header=None,
                    )
                    frames.extend(excel.values())
                elif lower_name.endswith(".csv"):
                    for encoding in ("cp932", "utf-8-sig", "utf-8"):
                        try:
                            frames.append(
                                pd.read_csv(
                                    io.BytesIO(payload),
                                    header=None,
                                    encoding=encoding,
                                )
                            )
                            break
                        except Exception:
                            continue
                elif lower_name.endswith(".pdf"):
                    frames.extend(_read_pdf_tables(payload))
        return frames

    if kind == "excel":
        excel = pd.read_excel(
            io.BytesIO(content),
            sheet_name=None,
            header=None,
        )
        frames.extend(excel.values())
        return frames

    raise RuntimeError(
        f"JPX候補のファイル形式を判定できません: "
        f"content-type={content_type or 'unknown'} url={url}"
    )


def _header_text(frame: pd.DataFrame, row: int, column: int) -> str:
    parts: list[str] = []
    for offset in range(4, -1, -1):
        source_row = row - offset
        if source_row < 0:
            continue
        value = frame.iat[source_row, column]
        if pd.isna(value):
            continue
        text = _norm_label(value)
        if text:
            parts.append(text)
    return "".join(parts)


def _find_jpx_columns(
    frame: pd.DataFrame,
) -> tuple[int, int, int, int] | None:
    max_header_rows = min(50, len(frame))

    for row in range(max_header_rows):
        headers = [
            _header_text(frame, row, col)
            for col in range(frame.shape[1])
        ]

        code_candidates = [
            col
            for col, value in enumerate(headers)
            if (
                "銘柄コード" in value
                or value.endswith("コード")
                or value == "コード"
                or value.lower().endswith("code")
            )
        ]
        short_candidates = [
            col
            for col, value in enumerate(headers)
            if any(
                key in value
                for key in (
                    "売残高",
                    "売残",
                    "売り残高",
                    "売り残",
                    "short",
                )
            )
        ]
        long_candidates = [
            col
            for col, value in enumerate(headers)
            if any(
                key in value
                for key in (
                    "買残高",
                    "買残",
                    "買い残高",
                    "買い残",
                    "long",
                )
            )
        ]

        if not code_candidates or not short_candidates or not long_candidates:
            continue

        def column_score(column: int, side: str) -> tuple[int, int]:
            value = headers[column]
            score = 0
            if "合計" in value or "総" in value or "total" in value.lower():
                score += 30
            if side == "short" and value.endswith(("売残高", "売残")):
                score += 10
            if side == "long" and value.endswith(("買残高", "買残")):
                score += 10
            if "制度" in value or "一般" in value:
                score -= 5
            return score, column

        code_col = code_candidates[0]
        short_col = max(
            short_candidates,
            key=lambda col: column_score(col, "short"),
        )
        long_col = max(
            long_candidates,
            key=lambda col: column_score(col, "long"),
        )
        return row, code_col, short_col, long_col

    return None


def _parse_jpx_frame(frame: pd.DataFrame) -> dict[str, float]:
    frame = frame.replace({"\n": " "}, regex=True)
    columns = _find_jpx_columns(frame)
    if columns is None:
        return {}

    header_row, code_col, short_col, long_col = columns
    ratios: dict[str, float] = {}

    for row in range(header_row + 1, len(frame)):
        raw_code = frame.iat[row, code_col]
        code = _normalize_jpx_security_code(raw_code)
        if not re.fullmatch(r"[0-9A-Z]{4}", code):
            continue

        short_balance = safe_float(frame.iat[row, short_col])
        long_balance = safe_float(frame.iat[row, long_col])
        ratio = safe_div(long_balance, short_balance)
        if ratio is not None and ratio >= 0:
            ratios[code] = ratio

    return ratios


def fetch_jpx_credit_ratios() -> tuple[dict[str, float], str]:
    errors: list[str] = []

    try:
        candidates = discover_jpx_margin_candidates()
        print(
            f"[DEBUG-JPX] filtered_download_candidates={len(candidates)}",
            flush=True,
        )
        for preview in candidates[:5]:
            print(f"[DEBUG-JPX-CANDIDATE] {preview}", flush=True)

        for url in candidates:
            try:
                response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").lower()
                if any(
                    blocked in content_type
                    for blocked in (
                        "text/css",
                        "javascript",
                        "image/",
                        "font/",
                    )
                ):
                    continue
                if (
                    "text/html" in content_type
                    and _payload_kind(url, content_type, response.content)
                    == "unknown"
                ):
                    continue

                kind = _payload_kind(
                    url,
                    content_type,
                    response.content,
                )

                best: dict[str, float] = {}

                # 現行JPX週末残高はPDF。表抽出より本文行の方が安定する。
                if kind == "pdf":
                    parsed_text = _parse_jpx_pdf_text(response.content)

                    if len(parsed_text) >= MIN_JPX_PARSED_ROWS:
                        print(
                            f"[OK] JPX credit ratios rows={len(parsed_text)} "
                            f"source=pdf_text url={url}",
                            flush=True,
                        )
                        return parsed_text, url

                    if parsed_text:
                        print(
                            f"[WARN] JPX PDF text parse was too small: "
                            f"parsed_rows={len(parsed_text)} "
                            f"minimum={MIN_JPX_PARSED_ROWS} url={url}",
                            flush=True,
                        )
                        best = parsed_text

                # Excel/CSV/ZIPおよびPDF表抽出のフォールバック。
                frames = _read_jpx_payload(
                    url,
                    content_type,
                    response.content,
                )
                for frame in frames:
                    parsed = _parse_jpx_frame(frame)
                    if len(parsed) > len(best):
                        best = parsed

                if len(best) >= MIN_JPX_PARSED_ROWS:
                    print(
                        f"[OK] JPX credit ratios rows={len(best)} url={url}",
                        flush=True,
                    )
                    return best, url

                if best:
                    print(
                        f"[WARN] JPX candidate rejected: "
                        f"parsed_rows={len(best)} minimum={MIN_JPX_PARSED_ROWS} "
                        f"url={url}",
                        flush=True,
                    )

            except Exception as exc:
                errors.append(
                    f"{url}: {type(exc).__name__}: {exc}"
                )

        detail = errors[-1] if errors else "解析可能な候補なし"
        raise RuntimeError(
            "JPX信用残高ファイルを取得・解析できませんでした。"
            f" last={detail}"
        )

    except Exception as exc:
        message = (
            f"JPX credit ratios unavailable: "
            f"{type(exc).__name__}: {exc}"
        )
        if STRICT_JPX:
            raise RuntimeError(message) from exc
        print(f"[WARN] {message}", flush=True)
        return {}, ""


# ====== Main ======
def read_codes() -> list[str]:
    with open("tickers.txt", "r", encoding="utf-8") as file:
        raw = [line for line in file if line.strip()]

    codes = [normalize_code_line(line) for line in raw]
    codes = [code for code in codes if code]
    codes = list(dict.fromkeys(codes))

    offset = int(os.getenv("OFFSET", "0"))
    limit = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        return codes[offset : offset + limit]
    if offset > 0:
        return codes[offset:]
    return codes


def build_row(
    code: str,
    market: dict[str, Any],
    credit_ratios: dict[str, float],
) -> list[Any]:
    latest_price = market["latest_price"]
    financial = fetch_financial_values(code)

    per = safe_div(latest_price, financial["eps"])
    pbr = safe_div(latest_price, financial["bps"])
    roe_pct = financial["roe_pct"]
    if roe_pct is None:
        roe_pct = safe_div(
            financial["profit"],
            financial["equity"],
            100.0,
        )
    equity_ratio_pct = financial["equity_ratio_pct"]
    if equity_ratio_pct is None:
        equity_ratio_pct = safe_div(
            financial["equity"],
            financial["assets"],
            100.0,
        )
    dividend_per_share_value = financial["dps"]
    if dividend_per_share_value is None:
        dividend_per_share_value = market.get("trailing_dividend")
        if dividend_per_share_value is not None:
            print(
                f"[DEBUG-DIVIDEND] {code} source=yahoo_ttm "
                f"dps={dividend_per_share_value}",
                flush=True,
            )

    dividend_yield_pct = safe_div(
        dividend_per_share_value,
        latest_price,
        100.0,
    )
    if dividend_yield_pct is not None and not (0 <= dividend_yield_pct <= 30):
        print(
            f"[WARN] {code} rejected abnormal dividend yield: "
            f"dps={dividend_per_share_value} price={latest_price} "
            f"yield={dividend_yield_pct}",
            flush=True,
        )
        dividend_yield_pct = None

    op_yoy = fetch_opinc_yoy(code)
    credit_ratio = credit_ratios.get(code)

    return [
        code,
        output_value(per),
        output_value(pbr),
        output_value(roe_pct),
        output_value(equity_ratio_pct),
        output_value(dividend_yield_pct),
        output_value(op_yoy),
        output_value(credit_ratio),
        output_value(market["vol5"], digits=0),
        output_value(market["vol25"], digits=0),
        output_value(market["volratio_5_25"]),
        output_value(market["deviation_25ma_pct"]),
    ]


def write_metrics_atomically(rows: list[list[Any]]) -> None:
    output_path = Path("metrics.csv")
    output_directory = output_path.parent.resolve()

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            dir=output_directory,
            prefix="metrics_",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            writer = csv.writer(temporary_file)
            writer.writerow(OUTPUT_COLUMNS)
            writer.writerows(rows)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())

        os.replace(temporary_path, output_path)
        print("metrics.csv written", flush=True)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink(missing_ok=True)


def main() -> int:
    print("[START] YAHOO_FREE_R8_20260725", flush=True)
    try:
        codes = read_codes()
        print(f"[CONFIG] script_version={SCRIPT_VERSION}", flush=True)
        print(f"Total tickers to process in this shard: {len(codes)}", flush=True)

        if not codes:
            print("[FATAL] tickers.txtに処理対象がありません", flush=True)
            return 1

        expected_date = expected_market_date()
        print(
            f"[CONFIG] source=YahooFinance/IRBANK-CSV/JPX "
            f"expected_market_date={expected_date.isoformat()} "
            f"strict_jpx={STRICT_JPX}",
            flush=True,
        )

        yahoo_frames = download_yahoo_all(codes)

        # Yahooの日付を全銘柄で先に検証する。
        # 1件でも古ければ、IRBANK取得やmetrics.csv更新へ進まない。
        market_metrics: dict[str, dict[str, Any]] = {}
        validation_errors: list[str] = []
        for index, code in enumerate(codes, 1):
            print(f"[{index}/{len(codes)}] {code} Yahoo validation", flush=True)
            symbol = yahoo_symbol(code)
            try:
                market_metrics[code] = yahoo_metrics(
                    code,
                    yahoo_frames.get(symbol, pd.DataFrame()),
                    expected_date,
                )
                print(
                    f"[OK] {code} Yahoo date="
                    f"{market_metrics[code]['latest_date'].isoformat()} "
                    f"price={output_value(market_metrics[code]['latest_price'])}",
                    flush=True,
                )
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                validation_errors.append(error)
                print(f"[ERROR] {error}", flush=True)

        if validation_errors:
            print(
                f"[FATAL] Yahoo Financeの最新日付を確認できない銘柄が"
                f"{len(validation_errors)}件あります。metrics.csvは更新しません。",
                flush=True,
            )
            return 1

        credit_ratios, _ = fetch_jpx_credit_ratios()

        rows: list[list[Any]] = []
        for index, code in enumerate(codes, 1):
            print(f"[{index}/{len(codes)}] {code} financial metrics", flush=True)
            row = build_row(code, market_metrics[code], credit_ratios)
            rows.append(row)
            filled = sum(1 for value in row[1:] if value not in ("", None))
            print(
                f"[OK] {code} filled={filled}/{len(OUTPUT_COLUMNS) - 1} "
                f"credit={'yes' if code in credit_ratios else 'no'}",
                flush=True,
            )
            polite_sleep(0.4)

        write_metrics_atomically(rows)
        return 0

    except Exception as exc:
        print(f"[FATAL] {type(exc).__name__}: {exc}", flush=True)
        print("metrics.csvは更新していません。", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
