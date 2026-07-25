# -*- coding: utf-8 -*-
"""
Japanese stock metrics collector

Primary source:
  - J-Quants API V2 (price, volume, financial summary)
Fallback source:
  - IRBANK CSV files (financial metrics / operating-profit YoY)

This version intentionally does NOT scrape Kabutan or IRBANK HTML pages.
Those pages can reject GitHub Actions requests with HTTP 403/405.
"""

from __future__ import annotations

import csv
import io
import math
import os
import re
import sys
import time
import unicodedata
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
import jquantsapi


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

IR_CSV_URL = "https://f.irbank.net/files/{code}/{path}"
IR_FILES = {
    "pl": "fy-profit-and-loss.csv",
    "bs": "fy-balance-sheet.csv",
    "div": "fy-stock-dividend.csv",
    "qq_op_yoy": "qq-yoy-operating-income.csv",
}

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "180"))
SLEEP_SECONDS = float(os.getenv("JQUANTS_SLEEP_SECONDS", "1.0"))
ENABLE_MARGIN = os.getenv("ENABLE_JQUANTS_MARGIN", "0").strip() == "1"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "jp-stocks-automation/2.0",
        "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.8,en;q=0.6",
    }
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def normalize_code_line(line: str) -> str:
    token = re.split(r"[\s,\t]+", line.strip())[0] if line else ""
    token = unicodedata.normalize("NFKC", token)
    return re.sub(r"[^0-9A-Za-z]", "", token).upper()


def is_numeric4(code: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", code))


def to_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    text = str(value).replace(",", "").strip()
    if text in {"", "-", "－", "—", "–", "―", "None", "null", "nan", "NaN"}:
        return None

    text = re.sub(r"[％%円¥倍株]", "", text).strip()
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None

    return number if math.isfinite(number) else None


def safe_ratio(numerator: Any, denominator: Any, multiplier: float = 1.0) -> float | None:
    n = to_float(numerator)
    d = to_float(denominator)
    if n is None or d in (None, 0):
        return None
    return n / d * multiplier


def normalize_percentage(value: Any) -> float | None:
    """
    J-Quants ratio fields may be represented as either a decimal ratio
    or a percentage depending on the field/version. Normalize to percent.
    """
    number = to_float(value)
    if number is None:
        return None
    if -1.5 <= number <= 1.5:
        number *= 100.0
    return number


def output_value(value: Any, digits: int = 4) -> str | int | float:
    number = to_float(value)
    if number is None:
        return ""
    rounded = round(number, digits)
    if float(rounded).is_integer():
        return int(rounded)
    return rounded


def latest_numeric(df: pd.DataFrame, column: str, *, positive: bool = False) -> float | None:
    if df.empty or column not in df.columns:
        return None

    for value in reversed(df[column].tolist()):
        number = to_float(value)
        if number is None:
            continue
        if positive and number <= 0:
            continue
        return number
    return None


def sort_financials(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    result = df.copy()
    for column in ("DiscDate", "CurPerEn", "CurFYEn"):
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], errors="coerce")

    sort_columns = [
        column
        for column in ("DiscDate", "DiscTime", "CurPerEn")
        if column in result.columns
    ]
    if sort_columns:
        result = result.sort_values(sort_columns, na_position="first")
    return result.reset_index(drop=True)


# ---------------------------------------------------------------------------
# J-Quants V2
# ---------------------------------------------------------------------------

def create_jquants_client() -> jquantsapi.ClientV2:
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "JQUANTS_API_KEY is not set. "
            "Add it to GitHub Actions repository secrets and expose it to this step."
        )
    return jquantsapi.ClientV2(api_key=api_key)


def fetch_jquants_bars(client: jquantsapi.ClientV2, code: str) -> pd.DataFrame:
    start = (date.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end = date.today().strftime("%Y%m%d")

    df = client.get_eq_bars_daily(
        code=code,
        from_yyyymmdd=start,
        to_yyyymmdd=end,
    )
    if df is None or df.empty:
        raise RuntimeError("J-Quants returned no daily price rows")

    result = df.copy()
    if "Date" in result.columns:
        result["Date"] = pd.to_datetime(result["Date"], errors="coerce")
        result = result.sort_values("Date")

    close_column = "AdjC" if "AdjC" in result.columns else "C"
    volume_column = "AdjVo" if "AdjVo" in result.columns else "Vo"

    if close_column not in result.columns:
        raise RuntimeError("J-Quants daily bars have no close-price column")
    if volume_column not in result.columns:
        raise RuntimeError("J-Quants daily bars have no volume column")

    result["_close"] = pd.to_numeric(result[close_column], errors="coerce")
    result["_volume"] = pd.to_numeric(result[volume_column], errors="coerce")
    result = result[result["_close"] > 0].copy()

    if result.empty:
        raise RuntimeError("J-Quants daily bars contain no valid close prices")

    return result.reset_index(drop=True)


def calculate_price_metrics(bars: pd.DataFrame) -> dict[str, float | int | None]:
    closes = bars["_close"].dropna()
    volumes = bars["_volume"].fillna(0)

    latest_price = to_float(closes.iloc[-1]) if not closes.empty else None

    vol5 = None
    vol25 = None
    volume_ratio = None
    deviation_25ma = None

    if len(volumes) >= 5:
        vol5 = float(volumes.tail(5).mean())

    if len(volumes) >= 25:
        vol25 = float(volumes.tail(25).mean())

    if vol5 is not None and vol25 not in (None, 0):
        volume_ratio = vol5 / vol25

    if len(closes) >= 25:
        latest_25 = closes.tail(25)
        ma25 = float(latest_25.mean())
        if ma25 > 0 and latest_price is not None:
            deviation_25ma = (latest_price / ma25 - 1.0) * 100.0

    return {
        "latest_price": latest_price,
        "vol5": int(round(vol5)) if vol5 is not None else None,
        "vol25": int(round(vol25)) if vol25 is not None else None,
        "volratio_5_25": volume_ratio,
        "deviation_25ma_pct": deviation_25ma,
    }


def fetch_jquants_financials(
    client: jquantsapi.ClientV2,
    code: str,
) -> pd.DataFrame:
    # The cursor method avoids the deprecation warning on get_fin_summary().
    df, _ = client.get_fin_summary_cursor(code=code)
    if df is None:
        return pd.DataFrame()
    return sort_financials(df)


def latest_fy_row(df: pd.DataFrame) -> pd.Series | None:
    if df.empty:
        return None

    if "CurPerType" in df.columns:
        types = df["CurPerType"].astype(str).str.upper()
        fy = df[types.isin({"FY", "4Q", "ANNUAL"})]
        if not fy.empty:
            return fy.iloc[-1]

    return df.iloc[-1]


def calculate_jquants_op_yoy(df: pd.DataFrame) -> float | None:
    """
    Compare the latest operating profit with the previous disclosure
    for the same period type (1Q/2Q/3Q/FY).
    """
    if df.empty or "OP" not in df.columns:
        return None

    work = df.copy()
    work["_op"] = pd.to_numeric(work["OP"], errors="coerce")
    work = work[work["_op"].notna()].copy()
    if work.empty:
        return None

    if "CurPerEn" in work.columns:
        work["_period_end"] = pd.to_datetime(work["CurPerEn"], errors="coerce")
    else:
        work["_period_end"] = pd.NaT

    if "DiscDate" in work.columns:
        work["_disc_date"] = pd.to_datetime(work["DiscDate"], errors="coerce")
    else:
        work["_disc_date"] = pd.NaT

    work = work.sort_values(["_period_end", "_disc_date"])

    # Keep the latest revision for each accounting period.
    if work["_period_end"].notna().any():
        work = work.drop_duplicates(subset=["_period_end"], keep="last")

    latest = work.iloc[-1]
    period_type = str(latest.get("CurPerType", "")).upper()

    if period_type and "CurPerType" in work.columns:
        same_type = work[work["CurPerType"].astype(str).str.upper() == period_type]
    else:
        same_type = work

    if len(same_type) < 2:
        return None

    current = to_float(same_type.iloc[-1]["_op"])
    previous = to_float(same_type.iloc[-2]["_op"])
    if current is None or previous in (None, 0):
        return None

    return (current / previous - 1.0) * 100.0


def calculate_jquants_financial_metrics(
    df: pd.DataFrame,
    latest_price: float | None,
) -> dict[str, float | None]:
    if df.empty:
        return {
            "per": None,
            "pbr": None,
            "roe_pct": None,
            "equity_ratio_pct": None,
            "dividend_yield_pct": None,
            "op_income_yoy_pct": None,
        }

    latest = df.iloc[-1]
    fy_row = latest_fy_row(df)

    # Forecast EPS is used first because market PER is normally forward-looking.
    forecast_eps = latest_numeric(df, "FEPS", positive=True)
    actual_eps = None
    if fy_row is not None:
        actual_eps = to_float(fy_row.get("EPS"))
    if actual_eps is None:
        actual_eps = latest_numeric(df, "EPS", positive=True)

    eps_for_per = forecast_eps if forecast_eps is not None else actual_eps
    bps = latest_numeric(df, "BPS", positive=True)

    per = safe_ratio(latest_price, eps_for_per)
    pbr = safe_ratio(latest_price, bps)

    roe = None
    if fy_row is not None:
        profit = to_float(fy_row.get("NP"))
        equity = to_float(fy_row.get("Eq"))
        roe = safe_ratio(profit, equity, 100.0)

        if roe is None:
            fy_eps = to_float(fy_row.get("EPS"))
            fy_bps = to_float(fy_row.get("BPS"))
            roe = safe_ratio(fy_eps, fy_bps, 100.0)

    equity_ratio = normalize_percentage(latest_numeric(df, "EqAR"))

    forecast_dividend = latest_numeric(df, "FDivAnn")
    actual_dividend = latest_numeric(df, "DivAnn")
    dividend = (
        forecast_dividend
        if forecast_dividend is not None
        else actual_dividend
    )
    dividend_yield = safe_ratio(dividend, latest_price, 100.0)

    return {
        "per": per,
        "pbr": pbr,
        "roe_pct": roe,
        "equity_ratio_pct": equity_ratio,
        "dividend_yield_pct": dividend_yield,
        "op_income_yoy_pct": calculate_jquants_op_yoy(df),
    }


def fetch_credit_ratio(
    client: jquantsapi.ClientV2,
    code: str,
) -> float | None:
    """
    Optional. J-Quants margin-interest data requires Standard plan or higher.
    Enable with ENABLE_JQUANTS_MARGIN=1.
    """
    if not ENABLE_MARGIN:
        return None

    start = (date.today() - timedelta(days=60)).strftime("%Y%m%d")
    end = date.today().strftime("%Y%m%d")

    try:
        df = client.get_mkt_margin_interest(
            code=code,
            from_yyyymmdd=start,
            to_yyyymmdd=end,
        )
    except Exception as exc:
        print(f"[WARN] {code} margin data unavailable: {exc}", flush=True)
        return None

    if df is None or df.empty:
        return None

    result = df.copy()
    if "Date" in result.columns:
        result["Date"] = pd.to_datetime(result["Date"], errors="coerce")
        result = result.sort_values("Date")

    latest = result.iloc[-1]
    # Credit ratio = outstanding margin purchases / outstanding margin sales.
    return safe_ratio(latest.get("LongVol"), latest.get("ShrtVol"))


# ---------------------------------------------------------------------------
# IRBANK CSV fallback
# ---------------------------------------------------------------------------

def normalize_label(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"（.*?）|\(.*?\)", "", text)
    return re.sub(r"[\s　,/％%円¥\-–—―]", "", text)


EPS_KEYS = [
    "EPS",
    "1株当たり利益",
    "1株当たり当期純利益",
    "1株当たり純利益",
]
BPS_KEYS = [
    "BPS",
    "1株当たり純資産",
    "1株純資産",
]
DPS_KEYS = [
    "1株配当",
    "1株配当金",
    "配当金",
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
ASSET_KEYS = [
    "総資産",
    "資産合計",
    "資産総額",
    "資産の部合計",
]
PROFIT_KEYS = [
    "当期純利益",
    "親会社株主に帰属する当期純利益",
    "純利益",
]


def get_irbank_csv(code: str, filename: str) -> list[list[str]] | None:
    if not is_numeric4(code):
        return None

    url = IR_CSV_URL.format(code=code, path=filename)
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        print(f"[WARN] {code} IRBANK CSV request failed: {filename}: {exc}", flush=True)
        return None

    if response.status_code == 404:
        return None
    if response.status_code != 200:
        print(
            f"[WARN] {code} IRBANK CSV HTTP {response.status_code}: {filename}",
            flush=True,
        )
        return None

    try:
        rows = list(csv.reader(io.StringIO(response.text)))
    except csv.Error as exc:
        print(f"[WARN] {code} invalid IRBANK CSV {filename}: {exc}", flush=True)
        return None

    return rows if len(rows) >= 2 else None


def row_index_by_keys(rows: list[list[str]] | None, keys: list[str]) -> int | None:
    if not rows:
        return None

    normalized_keys = [normalize_label(key) for key in keys]
    for index, row in enumerate(rows):
        if not row:
            continue
        heading = normalize_label(row[0])
        if not heading:
            continue
        if any(key and (key in heading or heading in key) for key in normalized_keys):
            return index
    return None


def last_number_in_row(
    rows: list[list[str]] | None,
    row_index: int | None,
) -> float | None:
    if not rows or row_index is None:
        return None

    for value in reversed(rows[row_index][1:]):
        number = to_float(value)
        if number is not None:
            return number
    return None


def irbank_operating_profit_yoy(rows: list[list[str]] | None) -> float | None:
    if not rows:
        return None

    for row in reversed(rows[1:]):
        # IRBANK's qq-yoy file normally stores the YoY value in column 2.
        candidates = row[1:] if len(row) > 1 else row
        for value in candidates:
            number = to_float(value)
            if number is not None:
                return number
    return None


def fetch_irbank_fallback(code: str) -> dict[str, float | None]:
    if not is_numeric4(code):
        return {}

    pl = get_irbank_csv(code, IR_FILES["pl"])
    bs = get_irbank_csv(code, IR_FILES["bs"])
    div = get_irbank_csv(code, IR_FILES["div"])
    qq = get_irbank_csv(code, IR_FILES["qq_op_yoy"])

    eps = last_number_in_row(pl, row_index_by_keys(pl, EPS_KEYS))
    profit = last_number_in_row(pl, row_index_by_keys(pl, PROFIT_KEYS))

    bps = last_number_in_row(bs, row_index_by_keys(bs, BPS_KEYS))
    equity = last_number_in_row(bs, row_index_by_keys(bs, EQ_KEYS))
    assets = last_number_in_row(bs, row_index_by_keys(bs, ASSET_KEYS))

    dividend = last_number_in_row(div, row_index_by_keys(div, DPS_KEYS))

    return {
        "eps": eps,
        "bps": bps,
        "profit": profit,
        "equity": equity,
        "assets": assets,
        "dividend": dividend,
        "op_income_yoy_pct": irbank_operating_profit_yoy(qq),
    }


def merge_irbank_fallback(
    metrics: dict[str, float | None],
    irbank: dict[str, float | None],
    latest_price: float | None,
) -> dict[str, float | None]:
    result = dict(metrics)

    if result.get("per") is None:
        result["per"] = safe_ratio(latest_price, irbank.get("eps"))

    if result.get("pbr") is None:
        result["pbr"] = safe_ratio(latest_price, irbank.get("bps"))

    if result.get("roe_pct") is None:
        result["roe_pct"] = safe_ratio(
            irbank.get("profit"),
            irbank.get("equity"),
            100.0,
        )

    if result.get("equity_ratio_pct") is None:
        result["equity_ratio_pct"] = safe_ratio(
            irbank.get("equity"),
            irbank.get("assets"),
            100.0,
        )

    if result.get("dividend_yield_pct") is None:
        result["dividend_yield_pct"] = safe_ratio(
            irbank.get("dividend"),
            latest_price,
            100.0,
        )

    # IRBANK's dedicated quarterly YoY CSV is preferred when available.
    if irbank.get("op_income_yoy_pct") is not None:
        result["op_income_yoy_pct"] = irbank["op_income_yoy_pct"]

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def read_codes() -> list[str]:
    with open("tickers.txt", "r", encoding="utf-8") as file:
        codes = [
            normalize_code_line(line)
            for line in file
            if line.strip()
        ]

    codes = [code for code in codes if code]
    codes = list(dict.fromkeys(codes))

    offset = int(os.getenv("OFFSET", "0"))
    limit = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        return codes[offset : offset + limit]
    if offset > 0:
        return codes[offset:]
    return codes


def blank_row(code: str) -> dict[str, Any]:
    row = {column: "" for column in OUTPUT_COLUMNS}
    row["code"] = code
    return row


def collect_one(
    client: jquantsapi.ClientV2,
    code: str,
) -> dict[str, Any]:
    bars = fetch_jquants_bars(client, code)
    price_metrics = calculate_price_metrics(bars)
    latest_price = to_float(price_metrics["latest_price"])

    financial_df = fetch_jquants_financials(client, code)
    financial_metrics = calculate_jquants_financial_metrics(
        financial_df,
        latest_price,
    )

    irbank = fetch_irbank_fallback(code)
    financial_metrics = merge_irbank_fallback(
        financial_metrics,
        irbank,
        latest_price,
    )

    credit_ratio = fetch_credit_ratio(client, code)

    row = {
        "code": code,
        "per": output_value(financial_metrics.get("per")),
        "pbr": output_value(financial_metrics.get("pbr")),
        "roe_pct": output_value(financial_metrics.get("roe_pct")),
        "equity_ratio_pct": output_value(
            financial_metrics.get("equity_ratio_pct")
        ),
        "dividend_yield_pct": output_value(
            financial_metrics.get("dividend_yield_pct")
        ),
        "op_income_yoy_pct": output_value(
            financial_metrics.get("op_income_yoy_pct")
        ),
        "credit_ratio": output_value(credit_ratio),
        "vol5": output_value(price_metrics.get("vol5"), digits=0),
        "vol25": output_value(price_metrics.get("vol25"), digits=0),
        "volratio_5_25": output_value(
            price_metrics.get("volratio_5_25")
        ),
        "deviation_25ma_pct": output_value(
            price_metrics.get("deviation_25ma_pct")
        ),
    }

    latest_date = ""
    if "Date" in bars.columns and not bars.empty:
        date_value = bars.iloc[-1]["Date"]
        if not pd.isna(date_value):
            latest_date = pd.Timestamp(date_value).strftime("%Y-%m-%d")

    filled = sum(
        1
        for key, value in row.items()
        if key != "code" and value not in ("", None)
    )
    print(
        f"[OK] {code} data_date={latest_date or 'unknown'} "
        f"filled={filled}/{len(OUTPUT_COLUMNS) - 1} "
        f"price={output_value(latest_price)}",
        flush=True,
    )
    return row


def write_metrics(rows: list[dict[str, Any]]) -> None:
    with open("metrics.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print("metrics.csv written", flush=True)


def main() -> int:
    codes = read_codes()
    print(f"Total tickers to process in this shard: {len(codes)}", flush=True)
    print(
        f"[CONFIG] JQUANTS_API_KEY exists="
        f"{bool(os.getenv('JQUANTS_API_KEY', '').strip())} "
        f"margin_enabled={ENABLE_MARGIN}",
        flush=True,
    )

    if not codes:
        write_metrics([])
        return 0

    try:
        client = create_jquants_client()
    except Exception as exc:
        print(f"[FATAL] {exc}", flush=True)
        return 2

    rows: list[dict[str, Any]] = []
    failures = 0

    for index, code in enumerate(codes, 1):
        print(f"[{index}/{len(codes)}] {code} start", flush=True)
        try:
            rows.append(collect_one(client, code))
        except Exception as exc:
            failures += 1
            print(f"[ERROR] {code}: {type(exc).__name__}: {exc}", flush=True)
            rows.append(blank_row(code))

        if index < len(codes) and SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)

    write_metrics(rows)

    if failures == len(codes):
        print("[FATAL] Every ticker failed; refusing to report a successful run.", flush=True)
        return 1

    if failures:
        print(f"[WARN] completed with {failures} failed ticker(s)", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
