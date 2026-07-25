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
from bs4 import BeautifulSoup

SCRIPT_VERSION = "YAHOO_FREE_R4_20260725"



# ====== 設定 ======
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
JPX_MARGIN_PAGE = "https://www.jpx.co.jp/markets/statistics-equities/margin/05.html"

CSV_PL = "fy-profit-and-loss.csv"
CSV_BS = "fy-balance-sheet.csv"
CSV_DIV = "fy-stock-dividend.csv"
CSV_QQ = "qq-yoy-operating-income.csv"
CSV_PS = "fy-per-share.csv"

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
YAHOO_PERIOD = os.getenv("YAHOO_PERIOD", "6mo")
YAHOO_CHUNK_SIZE = max(1, int(os.getenv("YAHOO_CHUNK_SIZE", "40")))
YAHOO_RETRIES = max(1, int(os.getenv("YAHOO_RETRIES", "3")))
IRBANK_RETRIES = max(1, int(os.getenv("IRBANK_RETRIES", "3")))
MARKET_DATA_READY_TIME = os.getenv("MARKET_DATA_READY_TIME", "16:15")
STRICT_JPX = os.getenv("STRICT_JPX", "0").strip() == "1"
JPX_MARGIN_URL_OVERRIDE = os.getenv("JPX_MARGIN_URL", "").strip()

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

    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace(",", "").strip()
    text = re.sub(r"[％%円¥倍株]", "", text)
    if text in {"", "-", "--", "---", "None", "null", "nan", "NaN"}:
        return None

    try:
        number = float(text)
    except (TypeError, ValueError):
        return None

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
                actions=False,
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

    return {
        "latest_date": latest_date,
        "latest_price": latest_price,
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
NI_KEYS = ["当期純利益", "親会社株主に帰属する当期純利益", "純利益"]


def row_index_by_keys(rows: list[list[str]] | None, keys: list[str]) -> int | None:
    if not rows:
        return None

    normalized_keys = [_norm_label(key) for key in keys]
    for index, row in enumerate(rows):
        if not row:
            continue
        heading = _norm_label(row[0])
        if not heading:
            continue
        if any(key and (key in heading or heading in key) for key in normalized_keys):
            return index
    return None


def last_num_in_row(
    rows: list[list[str]] | None,
    row_index: int | None,
) -> float | None:
    if not rows or row_index is None:
        return None

    for value in reversed(rows[row_index][1:]):
        number = safe_float(value)
        if number is not None:
            return number
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
                response.encoding = response.apparent_encoding or "utf-8"
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


def fetch_eps_bps_profit_equity_assets_dps(
    code: str,
) -> tuple[float | None, float | None, float | None, float | None, float | None, float | None]:
    pl = get_csv(code, CSV_PL)
    bs = get_csv(code, CSV_BS)
    dividend = get_csv(code, CSV_DIV)
    per_share = get_csv(code, CSV_PS)

    eps = last_num_in_row(pl, row_index_by_keys(pl, EPS_KEYS))
    profit = last_num_in_row(pl, row_index_by_keys(pl, NI_KEYS))
    if eps is None:
        eps = last_num_in_row(per_share, row_index_by_keys(per_share, EPS_KEYS))

    bps = last_num_in_row(bs, row_index_by_keys(bs, BPS_KEYS))
    equity = last_num_in_row(bs, row_index_by_keys(bs, EQ_KEYS))
    assets = last_num_in_row(bs, row_index_by_keys(bs, AS_KEYS))
    if bps is None:
        bps = last_num_in_row(per_share, row_index_by_keys(per_share, BPS_KEYS))

    dps = last_num_in_row(dividend, row_index_by_keys(dividend, DPS_KEYS))
    if dps is None:
        dps = last_num_in_row(per_share, row_index_by_keys(per_share, DPS_KEYS))

    return eps, bps, profit, equity, assets, dps


def fetch_opinc_yoy(code: str) -> float | None:
    rows = get_csv(code, CSV_QQ)
    if not rows:
        return None

    # 旧コードとの互換を優先し、最新行の2列目から先にある最初の数値を返す。
    for row in reversed(rows[1:]):
        for value in row[1:]:
            number = safe_float(value)
            if number is not None:
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
                return int(date(int(match.group(1)), int(match.group(2)), int(match.group(3))).strftime("%Y%m%d"))
            except ValueError:
                pass
    return 0


def discover_jpx_margin_file() -> str:
    if JPX_MARGIN_URL_OVERRIDE:
        return JPX_MARGIN_URL_OVERRIDE

    response = SESSION.get(JPX_MARGIN_PAGE, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    candidates: list[tuple[int, int, str]] = []
    preferred_extensions = {".xlsx": 5, ".xls": 4, ".csv": 3, ".zip": 2}

    for order, anchor in enumerate(soup.find_all("a", href=True)):
        href = anchor.get("href", "").strip()
        absolute_url = urljoin(JPX_MARGIN_PAGE, href)
        clean_path = absolute_url.split("?", 1)[0].lower()
        extension = next(
            (ext for ext in preferred_extensions if clean_path.endswith(ext)),
            None,
        )
        if extension is None:
            continue

        context = " ".join(
            [
                anchor.get_text(" ", strip=True),
                href,
                anchor.parent.get_text(" ", strip=True) if anchor.parent else "",
            ]
        )
        date_score = _parse_date_score(context)
        candidates.append((date_score, preferred_extensions[extension] * 10000 + order, absolute_url))

    if not candidates:
        raise RuntimeError("JPXページから信用残高のExcel/CSVリンクを発見できませんでした")

    candidates.sort()
    return candidates[-1][2]


def _read_jpx_payload(url: str, content: bytes) -> list[pd.DataFrame]:
    lower_url = url.split("?", 1)[0].lower()
    frames: list[pd.DataFrame] = []

    if lower_url.endswith(".csv"):
        for encoding in ("cp932", "utf-8-sig", "utf-8"):
            try:
                frames.append(pd.read_csv(io.BytesIO(content), header=None, encoding=encoding))
                return frames
            except Exception:
                continue
        raise RuntimeError("JPX CSVを読み込めませんでした")

    if lower_url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for name in archive.namelist():
                if name.lower().endswith((".xlsx", ".xls")):
                    payload = archive.read(name)
                    excel = pd.read_excel(io.BytesIO(payload), sheet_name=None, header=None)
                    frames.extend(excel.values())
                elif name.lower().endswith(".csv"):
                    payload = archive.read(name)
                    for encoding in ("cp932", "utf-8-sig", "utf-8"):
                        try:
                            frames.append(pd.read_csv(io.BytesIO(payload), header=None, encoding=encoding))
                            break
                        except Exception:
                            continue
        return frames

    excel = pd.read_excel(io.BytesIO(content), sheet_name=None, header=None)
    frames.extend(excel.values())
    return frames


def _header_text(frame: pd.DataFrame, row: int, column: int) -> str:
    parts: list[str] = []
    for offset in range(3, -1, -1):
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


def _find_jpx_columns(frame: pd.DataFrame) -> tuple[int, int, int, int] | None:
    max_header_rows = min(30, len(frame))

    for row in range(max_header_rows):
        headers = [_header_text(frame, row, col) for col in range(frame.shape[1])]

        code_candidates = [
            col for col, text in enumerate(headers)
            if "銘柄コード" in text or text.endswith("コード") or text == "コード"
        ]
        short_candidates = [
            col for col, text in enumerate(headers)
            if any(key in text for key in ("売残高", "売残", "売り残高", "売り残"))
        ]
        long_candidates = [
            col for col, text in enumerate(headers)
            if any(key in text for key in ("買残高", "買残", "買い残高", "買い残"))
        ]

        if not code_candidates or not short_candidates or not long_candidates:
            continue

        def column_score(column: int, side: str) -> tuple[int, int]:
            text = headers[column]
            score = 0
            if "合計" in text or "総" in text:
                score += 20
            if side == "short" and text.endswith(("売残高", "売残")):
                score += 10
            if side == "long" and text.endswith(("買残高", "買残")):
                score += 10
            if "制度" in text or "一般" in text:
                score -= 5
            return score, column

        code_col = code_candidates[0]
        short_col = max(short_candidates, key=lambda col: column_score(col, "short"))
        long_col = max(long_candidates, key=lambda col: column_score(col, "long"))
        return row, code_col, short_col, long_col

    return None


def _parse_jpx_frame(frame: pd.DataFrame) -> dict[str, float]:
    columns = _find_jpx_columns(frame)
    if columns is None:
        return {}

    header_row, code_col, short_col, long_col = columns
    ratios: dict[str, float] = {}

    for row in range(header_row + 1, len(frame)):
        code = normalize_code_line(str(frame.iat[row, code_col]))
        if not re.fullmatch(r"[0-9A-Z]{4}", code):
            continue

        short_balance = safe_float(frame.iat[row, short_col])
        long_balance = safe_float(frame.iat[row, long_col])
        ratio = safe_div(long_balance, short_balance)
        if ratio is not None and ratio >= 0:
            ratios[code] = ratio

    return ratios


def fetch_jpx_credit_ratios() -> tuple[dict[str, float], str]:
    try:
        url = discover_jpx_margin_file()
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        frames = _read_jpx_payload(url, response.content)

        best: dict[str, float] = {}
        for frame in frames:
            parsed = _parse_jpx_frame(frame)
            if len(parsed) > len(best):
                best = parsed

        if not best:
            raise RuntimeError("JPX信用残高ファイルの列を判定できませんでした")

        print(f"[OK] JPX credit ratios rows={len(best)} url={url}", flush=True)
        return best, url

    except Exception as exc:
        message = f"JPX credit ratios unavailable: {type(exc).__name__}: {exc}"
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
    eps, bps, profit, equity, assets, dps = fetch_eps_bps_profit_equity_assets_dps(code)

    per = safe_div(latest_price, eps)
    pbr = safe_div(latest_price, bps)
    roe_pct = safe_div(profit, equity, 100.0)
    equity_ratio_pct = safe_div(equity, assets, 100.0)
    dividend_yield_pct = safe_div(dps, latest_price, 100.0)
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
    print("[START] YAHOO_FREE_R4_20260725", flush=True)
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
