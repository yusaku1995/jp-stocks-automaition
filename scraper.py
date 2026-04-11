# -*- coding: utf-8 -*-
# jp-stocks-automation scraper (Kabutan/IRBANK + XPath) — supports alphanumeric codes like 215A

import os, io, csv, re, time, random, requests
from lxml import html as LH
from datetime import date, timedelta

# ====== Base headers / helpers ======
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
]
def _headers():
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
    }

RETRIES = 6

# ====== Endpoints ======
IR_CSV  = "https://f.irbank.net/files/{code}/{path}"
IR_HTML = "https://irbank.net/{code}"
STOOQ   = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"
KABU_KABUKA   = "https://kabutan.jp/stock/kabuka?code={code}&ashi=day&page={page}"
JQ_DAILY_QUOTES   = "https://api.jquants.com/v1/prices/daily_quotes"
JQ_FINS_STATEMENTS = "https://api.jquants.com/v1/fins/statements"

CSV_PL="fy-profit-and-loss.csv"
CSV_BS="fy-balance-sheet.csv"
CSV_DIV="fy-stock-dividend.csv"
CSV_QQ="qq-yoy-operating-income.csv"
CSV_PS="fy-per-share.csv"

def polite_sleep(sec: float) -> None:
    time.sleep(sec + random.uniform(0.1, 0.6))

def safe_div(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0:
            return ""
        return a / b
    except:
        return ""

def to_pct(x):
    try:
        return float(x) * 100.0
    except:
        return ""

def normalize_code_line(line: str) -> str:
    token = re.split(r"[\s,\t]+", line.strip())[0] if line else ""
    try:
        import unicodedata
        token = unicodedata.normalize("NFKC", token)
    except:
        pass
    return token.strip().upper()

def is_numeric4(code: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", code))

# ====== IRBANK CSV helpers ======
def _norm_label(s):
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r'（.*?）', '', s)
    s = re.sub(r'\(.*?\)',  '', s)
    s = re.sub(r'[\s　,/％%円¥\-–—]', '', s)
    return s

EPS_KEYS=["EPS","EPS(円)","EPS（円）","1株当たり利益","1株当たり当期純利益","1株当たり当期純利益(円)","1株当たり当期純利益（円）","1株当たり純利益"]
BPS_KEYS=["BPS","BPS(円)","BPS（円）","1株当たり純資産","1株当たり純資産(円)","1株当たり純資産（円）","1株純資産"]
DPS_KEYS=["1株配当","1株配当金","配当金","配当(円)","配当（円）","1株当たり配当金"]
EQ_KEYS = ["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計","純資産の部合計"]
AS_KEYS = ["総資産","資産合計","資産総額","資産の部合計"]
NI_KEYS =["当期純利益","親会社株主に帰属する当期純利益","純利益"]

def row_index_by_keys(rows, keys):
    if not rows:
        return None
    norm_keys = [_norm_label(k) for k in keys]
    for i, r in enumerate(rows):
        if not r: continue
        head = _norm_label(r[0])
        if not head: continue
        for nk in norm_keys:
            if nk and (nk in head or head in nk):
                return i
    return None

def last_num_in_row(rows, ridx):
    if ridx is None:
        return ""
    r = rows[ridx]
    for x in reversed(r[1:]):
        if x is None:
            continue
        s = str(x).replace(',', '').strip()
        if s in ("", "-", "—", "–", "―"):
            continue
        try:
            return float(s)
        except:
            continue
    return ""

def get_csv(code, path):
    if not is_numeric4(code):
        print(f"[SKIP] IRBANK CSV likely missing for non-4digit: {code}", flush=True)
        return None

    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=_headers(), timeout=20)
            ctype = r.headers.get("Content-Type", "")

            # 404 は「そのCSVが存在しない」可能性が高いので即終了
            if r.status_code == 404:
                print(f"[MISS] {url} -> HTTP 404", flush=True)
                return None

            if not r.ok:
                print(f"[WARN] {url} -> HTTP {r.status_code}", flush=True)

            elif "text/csv" not in ctype and "application/octet-stream" not in ctype:
                head = (r.text or "")[:200].replace("\n", " ")
                print(f"[WARN] {url} -> non-CSV ({ctype}). head='{head}'", flush=True)

            else:
                rows = list(csv.reader(io.StringIO(r.text)))
                if len(rows) >= 2:
                    print(f"[OK] {url} rows={len(rows)}", flush=True)
                    polite_sleep(2.0)
                    return rows
                else:
                    print(f"[WARN] {url} CSV too short", flush=True)

        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)

        polite_sleep(2 + 2*i)

    print(f"[FAIL] {url} retried {RETRIES}x", flush=True)
    return None

def fetch_eps_bps_profit_equity_assets_dps(code):
    pl = get_csv(code, CSV_PL)
    bs = get_csv(code, CSV_BS)
    dv = get_csv(code, CSV_DIV)
    ps = get_csv(code, CSV_PS)

    eps = bps = ni = eq = assets = dps = ""

    if pl:
        eps = last_num_in_row(pl, row_index_by_keys(pl, EPS_KEYS))
        ni  = last_num_in_row(pl, row_index_by_keys(pl, NI_KEYS))
    if eps == "" and ps:
        eps = last_num_in_row(ps, row_index_by_keys(ps, EPS_KEYS))

    if bs:
        bps    = last_num_in_row(bs, row_index_by_keys(bs, BPS_KEYS))
        eq     = last_num_in_row(bs, row_index_by_keys(bs, EQ_KEYS))
        assets = last_num_in_row(bs, row_index_by_keys(bs, AS_KEYS))
    if bps == "" and ps:
        bps = last_num_in_row(ps, row_index_by_keys(ps, BPS_KEYS))

    if dv:
        dps = last_num_in_row(dv, row_index_by_keys(dv, DPS_KEYS))
    if dps == "" and ps:
        dps = last_num_in_row(ps, row_index_by_keys(ps, DPS_KEYS))

    return eps, bps, ni, eq, assets, dps

# ===== op_income_yoy フォールバック追加 =====

def _extract_yoy_from_text(url):
    try:
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code != 200 or not r.text:
            print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
            return ""
        doc = LH.fromstring(r.text)
        text = doc.text_content()
        text = re.sub(r"\s+", " ", text)
        m = re.search(
            r"(営業利益|営業益)\s*.*?(前年同期比|前年比|前比)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)\s*%",
            text
        )
        if m:
            return m.group(3)
    except Exception as e:
        print(f"[ERR] yoy parse {url} -> {e}", flush=True)
    return ""


def fetch_opinc_yoy(code):
    # 1) IRBANK CSV
    qq = get_csv(code, CSV_QQ)
    if qq:
        for row in reversed(qq[1:]):
            if len(row) <= 1:
                continue
            s = re.sub(r'[^0-9.\-]', '', row[1] or "")
            if s in ("", "-", ".", "-."):
                continue
            return s

    # 2) Kabutan 財務ページから近傍%抽出
    url = KABU_FINANCE.format(code=code)

    def _get_text(url, xp):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code != 200 or not r.text:
                return ""
            doc = LH.fromstring(r.text)
            nodes = doc.xpath(xp)
            if not nodes:
                return ""
            parts = []
            for n in nodes:
                parts.append(n if isinstance(n, str) else n.text_content())
            text = " ".join(parts).strip()
            return re.sub(r"\s+", " ", text)
        except Exception:
            return ""

    candidates = [
        "//tr[.//*[contains(normalize-space(.),'営業利益')]]",
        "//*[self::tr or self::li][.//*[contains(normalize-space(.),'営業利益')]]",
    ]
    pct_re = re.compile(r"([+\-]?\d+(?:\.\d+)?)\s*%")
    for xp in candidates:
        row_txt = _get_text(url, xp)
        if row_txt:
            near_re = re.compile(r"(前年同期比|前年比|前比)[^%]{0,40}?([+\-]?\d+(?:\.\d+)?)\s*%")
            m = near_re.search(row_txt)
            if m:
                return m.group(2)
            m2 = pct_re.search(row_txt)
            if m2:
                return m2.group(1)

    overview_txt = _get_text(KABU_OVERVIEW.format(code=code), "//*[contains(text(),'営業利益')]/ancestor::*[self::tr or self::li][1]")
    if overview_txt:
        m = re.search(r"(前年同期比|前年比|前比)[^%]{0,40}?([+\-]?\d+(?:\.\d+)?)\s*%", overview_txt)
        if m:
            return m.group(2)
        m2 = pct_re.search(overview_txt)
        if m2:
            return m2.group(1)

    # 3) その他フォールバック
    for url in (KABU_FINANCE.format(code=code), KABU_OVERVIEW.format(code=code)):
        v = _extract_yoy_from_text(url)
        if v != "":
            return v
    v = _extract_yoy_from_text(IR_HTML.format(code=code))
    if v != "":
        return v
    return ""

# ====== HTML helpers ======
_num_re = re.compile(r"(-?\d+(?:\.\d+)?)")

def _get_first_text_by_xpath(url, xp):
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                doc = LH.fromstring(r.text)
                nodes = doc.xpath(xp)
                if nodes:
                    parts = []
                    for n in nodes:
                        parts.append(n if isinstance(n, str) else n.text_content())
                    text = " ".join(parts).strip()
                    text = re.sub(r"\s+", " ", text)
                    return text
                else:
                    print(f"[WARN] XPath no match: {url} :: {xp}", flush=True)
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.5 + i)
    return ""

def _fetch_text(url):
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                return r.text
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.0 + i)
    return ""

def _fetch_text_from_dom(url):
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                doc = LH.fromstring(r.text)
                return doc.text_content()
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.0 + i)
    return ""

def _num_only(s):
    if not s:
        return ""
    m = _num_re.search(s)
    return m.group(1) if m else ""

def _num_pct_sane(s):
    v = _num_only(s)
    if v == "": return ""
    try:
        f = float(v)
        if abs(f) > 1000:
            return ""
        return str(f)
    except:
        return ""
def _jquants_headers():
    token = os.getenv("JQUANTS_ID_TOKEN", "").strip()
    if not token:
        return None
    return {"Authorization": f"Bearer {token}"}

def _jquants_get(url, params):
    headers = _jquants_headers()
    if not headers:
        print(f"[DEBUG-JQ] no token for {url}", flush=True)
        return None
    try:
        r = requests.get(url, headers=headers, params=params, timeout=25)
        print(f"[DEBUG-JQ] {url} status={r.status_code} params={params}", flush=True)
        if r.status_code == 200:
            return r.json()
        print(f"[WARN] J-Quants {url} -> HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"[ERR] J-Quants {url} -> {e}", flush=True)
    return None

def _to_float_or_blank(x):
    if x is None:
        return ""
    s = str(x).replace(",", "").strip()
    if s in ("", "-", "None", "null"):
        return ""
    try:
        return float(s)
    except Exception:
        return ""

# ====== Kabutan overview/finance quick getters ======
def kabu_per(code):
    return _num_only(_get_first_text_by_xpath(
        KABU_OVERVIEW.format(code=code),
        "//th[contains(.,'PER')]/following-sibling::td[1]"
    ))

def kabu_pbr(code):
    return _num_only(_get_first_text_by_xpath(
        KABU_OVERVIEW.format(code=code),
        "//th[contains(.,'PBR')]/following-sibling::td[1]"
    ))

def kabu_roe_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    candidates = [
        "//th[.//text()[contains(.,'ROE')]]/following-sibling::td[1]",
        "//*[self::th or self::*][contains(normalize-space(.),'ROE')]/following::*[1]",
        "//*[contains(text(),'ROE')][1]/following::text()[1]",
    ]
    for xp in candidates:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_pct_sane(t)
        if v != "":
            return v
    url_f = KABU_FINANCE.format(code=code)
    for xp in [
        "//th[.//text()[contains(.,'ROE')]]/following-sibling::td[1]",
        "//*[contains(text(),'ROE')]/following::td[1]",
    ]:
        t = _get_first_text_by_xpath(url_f, xp)
        v = _num_pct_sane(t)
        if v != "":
            return v
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'ROE')])[1]/following::text()[1]")
    return _num_pct_sane(t)

def kabu_divy_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    for xp in [
        "//th[.//text()[contains(.,'配当利回り')]]/following-sibling::td[1]",
        "//*[self::th or self::*][contains(normalize-space(.),'配当利回り')]/following::*[1]",
        "//*[contains(text(),'配当利回り')][1]/following::text()[1]",
    ]:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_pct_sane(t)
        if v != "":
            return v
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'配当利回り')])[1]/following::text()[1]")
    return _num_pct_sane(t)

def kabu_credit(code):
    t = _get_first_text_by_xpath(
            KABU_OVERVIEW.format(code=code),
            "//*[contains(text(),'信用倍率')][1]/following::text()[1]"
        ) or _get_first_text_by_xpath(
            KABU_OVERVIEW.format(code=code),
            "//th[contains(.,'信用倍率')]/following-sibling::td[1]"
        )
    return _num_only(t)

# === 追加: 株探の財務表から行マッチ → 最新数値を取る ===
def _kabu_pick_latest_number(url, label_keywords):
    """
    株探/財務ページの表から、対象行のデータセルだけを見て左端の数値を返す。
    """
    try:
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code != 200 or not r.text:
            print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
            return ""

        doc = LH.fromstring(r.text)

        for tr in doc.xpath("//tr[td or th]"):
            cells = tr.xpath("./th|./td")
            texts = [re.sub(r"\s+", " ", c.text_content().strip()) for c in cells]
            if not texts:
                continue

            # 先頭セルだけを項目名として判定
            row_label = texts[0]
            if not any(k in row_label for k in label_keywords):
                continue

            # 先頭は項目名なので除外
            for c in cells[1:]:
                s = re.sub(r"[^\d\.\-]", "", c.text_content())
                if s not in ("", "-", ".", "-."):
                    try:
                        v = float(s)
                        if v > 0:
                            return v
                    except Exception:
                        pass

        return ""

    except Exception as e:
        print(f"[ERR] _kabu_pick_latest_number {url} -> {e}", flush=True)
        return ""

# --- 置換版: 自己資本比率（%） 多段フォールバック + 計算 ---

def jquants_equity_ratio_pct(code):
    """
    J-Quants の財務情報から自己資本比率を取得。
    まず EquityToAssetRatio を直接使い、
    無ければ Equity / TotalAssets * 100 を計算する。
    """
    data = _jquants_get(JQ_FINS_STATEMENTS, {"code": code})
    if not data:
        return ""

    rows = data.get("statements", [])
    if not rows:
        return ""

    def _sort_key(r):
        return (
            r.get("CurrentFiscalYearEndDate", ""),
            r.get("DisclosedDate", ""),
            r.get("DisclosedTime", ""),
        )

    fy_rows = [r for r in rows if str(r.get("TypeOfCurrentPeriod", "")).upper() == "FY"]
    target_rows = sorted(fy_rows or rows, key=_sort_key, reverse=True)

    for row in target_rows:
        for k in ("EquityToAssetRatio", "NonConsolidatedEquityToAssetRatio"):
            v = _num_pct_sane(row.get(k, ""))
            if v != "":
                return v

        eq = _to_float_or_blank(row.get("Equity"))
        ta = _to_float_or_blank(row.get("TotalAssets"))
        if eq == "" or ta == "":
            eq = _to_float_or_blank(row.get("NonConsolidatedEquity"))
            ta = _to_float_or_blank(row.get("NonConsolidatedTotalAssets"))

        if eq != "" and ta not in ("", 0):
            try:
                ratio = float(eq) / float(ta) * 100.0
                if 0 <= ratio <= 100:
                    return str(round(ratio, 2))
            except Exception:
                pass

    return ""

def kabutan_equity_ratio_from_finance_table(code):
    print(f"[DEBUG-EQR-FUNC] ACTIVE kabutan_equity_ratio_from_finance_table {code}", flush=True)
    """
    株探 finance ページ下部の『財務 〖実績〗』表から、
    最新行の自己資本比率を取得する。
    215A なら最新行は『連 2025.10 ... 43.2 ...』なので 43.2 を返したい。
    """
    url = KABU_FINANCE.format(code=code)
    try:
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code != 200 or not r.text:
            print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
            return ""

        doc = LH.fromstring(r.text)
        text = doc.text_content()
        text = re.sub(r"\s+", " ", text)

        # 財務【実績】のヘッダ以降から実績行だけ拾う
        row_pat = re.compile(
            r"(単|連|U|I)\s+"
            r"(\d{4}\.\d{2}\*?)\s+"
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 1株純資産
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 自己資本比率
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 総資産
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 自己資本
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 剰余金
            r"([+\-]?\d[\d,]*(?:\.\d+)?|－|-)\s+"   # 有利子負債倍率
            r"(\d{2}/\d{2}/\d{2}|－|-)"             # 発表日
        )

        rows = list(row_pat.finditer(text))
        if not rows:
            print(f"[WARN] no finance data row found: {url}", flush=True)
            return ""

        latest = rows[-1]
        eqr = latest.group(4)

        print(f"[DEBUG-EQR-ROW] latest_row={latest.group(0)}", flush=True)
        print(f"[DEBUG-EQR-VAL] eqr={eqr}", flush=True)

        if eqr not in ("", "-", "－"):
            return eqr.replace(",", "")

        return ""

    except Exception as e:
        print(f"[ERR] kabutan_equity_ratio_from_finance_table {url} -> {e}", flush=True)
        return ""

def kabu_equity_ratio_pct(code):
    # 1) 株探 finance ページの『財務〖実績〗』表から直接取得
    v = kabutan_equity_ratio_from_finance_table(code)
    if v != "":
        return v

    def _try_xpaths(url, xps):
        for xp in xps:
            t = _get_first_text_by_xpath(url, xp)
            vv = _num_pct_sane(t)
            if vv != "":
                return vv
        return ""

    def _try_regex(url):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                m = re.search(r"自己資本比率[^%]{0,80}?([+\-]?\d+(?:\.\d+)?)\s*%", r.text)
                if m:
                    return _num_pct_sane(m.group(1))
        except Exception:
            pass
        return ""

    # 2) 既存の直接取得フォールバック
    url_f = KABU_FINANCE.format(code=code)
    xps_f = [
        "//th[contains(.,'自己資本比率')]/following-sibling::td[1]",
        "//tr[.//*[contains(normalize-space(.),'自己資本比率')]]/*[self::td][1]",
        "//*[contains(text(),'自己資本比率')]/following::td[1]",
    ]
    v = _try_xpaths(url_f, xps_f)
    if v != "":
        return v

    url_o = KABU_OVERVIEW.format(code=code)
    xps_o = [
        "//*[contains(text(),'自己資本比率')][1]/following::text()[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li][1]/*[self::td or self::dd][1]",
    ]
    v = _try_xpaths(url_o, xps_o)
    if v != "":
        return v

    v = _try_regex(url_f)
    if v != "":
        return v

    v = _try_regex(url_o)
    if v != "":
        return v

    return ""


    url_f = KABU_FINANCE.format(code=code)
    xps_f = [
        "//tr[.//*[contains(normalize-space(.),'自己資本比率')]]/*[self::td or self::th][last()]",
        "//th[contains(.,'自己資本比率')]/following-sibling::td[1]",
        "//*[contains(text(),'自己資本比率')]/following::td[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li or self::dl or self::div][1]/*[self::td or self::dd][1]",
        "//*[contains(normalize-space(.),'自己資本比率') and (contains(.,'連結') or contains(.,'単体') or contains(.,'国内') or contains(.,'国際'))]/following::*[self::td or self::dd][1]"
    ]
    v = _try_xpaths(url_f, xps_f)
    if v != "":
        return v

    url_o = KABU_OVERVIEW.format(code=code)
    xps_o = [
        "//*[contains(text(),'自己資本比率')][1]/following::text()[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li][1]/*[self::td or self::dd][1]"
    ]
    v = _try_xpaths(url_o, xps_o)
    if v != "":
        return v

    v = _try_regex(url_f)
    if v != "":
        return v
    v = _try_regex(url_o)
    if v != "":
        return v

    url_ir = IR_HTML.format(code=code)
    xps_ir = [
        "(//*[contains(text(),'自己資本比率')])[1]/following::text()[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li or self::dl or self::div][1]/*[self::td or self::dd][1]"
    ]
    v = _try_xpaths(url_ir, xps_ir)
    if v != "":
        return v

    v = _try_regex(url_ir)
    if v != "":
        return v

    return ""

def ir_pbr(code):
    return _num_only(_get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'PBR')])[1]/following::text()[1]"))

def ir_credit(code):
    return _num_only(_get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'信用倍率')])[1]/following::text()[1]"))

# ====== Volumes (5/25) ======
def stooq_vols_any(code):
    url = STOOQ.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=15)
            if r.status_code == 200 and "\n" in r.text:
                rows = [x.split(',') for x in r.text.strip().splitlines()][1:]
                vols = []
                for x in rows:
                    if len(x) >= 6:
                        try:
                            vols.append(int(x[5]))
                        except:
                            vols.append(0)
                print(f"[OK] {url} days={len(rows)}", flush=True)
                vol5 = vol25 = vratio = ""
                if len(vols) >= 25:
                    vol5  = int(sum(vols[-5:]) / 5)
                    vol25 = int(sum(vols[-25:]) / 25)
                    vratio = (vol5/vol25) if vol25 else ""
                return vol5, vol25, vratio
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1 + i)
    print(f"[FAIL] {url}", flush=True)
    return "", "", ""

def kabutan_vols_any(code):
    vols = []
    page = 1
    while len(vols) < 25 and page <= 5:
        url = KABU_KABUKA.format(code=code, page=page)
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                doc = LH.fromstring(r.text)
                tables = doc.xpath("//table[contains(@class,'stock_kabuka') or contains(@class,'kabuka')]")
                found = False
                for tb in tables:
                    rows = tb.xpath(".//tr[td]")
                    for tr in rows:
                        tds = [re.sub(r"\s+", " ", td.text_content().strip()) for td in tr.xpath("./td")]
                        if len(tds) < 6:
                            continue
                        vtxt = tds[-1]  # 最終列=出来高
                        vnum = re.sub(r"[^\d]", "", vtxt)
                        if vnum != "":
                            vols.append(int(vnum))
                            found = True
                if not found:
                    print(f"[WARN] Kabutan vols: table not parsed on page {page} for {code}", flush=True)
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] Kabutan vols fetch {url} -> {e}", flush=True)
        page += 1
        polite_sleep(1.0)
    if len(vols) >= 25:
        vol5  = int(sum(vols[:5]) / 5)
        vol25 = int(sum(vols[:25]) / 25)
        vratio = (vol5/vol25) if vol25 else ""
        return vol5, vol25, vratio
    return "", "", ""

def get_vols(code):
    v5, v25, vr = stooq_vols_any(code)
    if v5 == "" or v25 == "":
        v5, v25, vr = kabutan_vols_any(code)
    return v5, v25, vr

def jquants_closes_any(code):
    """
    J-Quants の日次株価から調整済み終値を取得。
    返り値は古い→新しい順。
    """
    date_from = (date.today() - timedelta(days=120)).isoformat()

    data = _jquants_get(JQ_DAILY_QUOTES, {
        "code": code,
        "from": date_from,
    })
    if not data:
        return []

    rows = data.get("daily_quotes", [])
    if not rows:
        return []

    rows = sorted(rows, key=lambda x: x.get("Date", ""))

    closes = []
    for row in rows:
        v = _to_float_or_blank(row.get("AdjustmentClose"))
        if v == "":
            v = _to_float_or_blank(row.get("Close"))
        if v != "" and v > 0:
            closes.append(v)

    if len(closes) >= 25:
        return closes
    return []

# ====== 25MA 乖離率（Stooq優先 → 株探フォールバック） ======
def stooq_closes_any(code):
    """StooqのCSVから終値列を取得。怪しい系列は捨てる。"""
    url = STOOQ.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=15)
            if r.status_code == 200 and "\n" in r.text:
                rows = [x.split(',') for x in r.text.strip().splitlines()][1:]
                closes = []
                for x in rows:
                    if len(x) >= 5:
                        try:
                            closes.append(float(x[4]))
                        except:
                            pass

                if closes:
                    # 妥当性チェック:
                    # 終値なら通常は大半が正の値で、極端に小さい値の並びにはなりにくい
                    positive_count = sum(1 for v in closes if v > 0)
                    max_abs = max(abs(v) for v in closes) if closes else 0

                    # 明らかに終値系列としておかしい場合は捨てる
                    if positive_count < max(5, len(closes) // 2) or max_abs < 50:
                        print(f"[WARN] Stooq closes look invalid for {code}: closes={closes[:10]}...", flush=True)
                        return []

                    return closes
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1 + i)
    return []

def kabutan_closes_any(code):
    """株探『過去の株価』から終値列だけを安全に集める。返り値は先頭が最新。"""
    closes = []
    page = 1

    while len(closes) < 25 and page <= 5:
        url = KABU_KABUKA.format(code=code, page=page)
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code != 200 or not r.text:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
                page += 1
                polite_sleep(1.0)
                continue

            doc = LH.fromstring(r.text)
            tables = doc.xpath("//table")
            found = False

            for tb in tables:
                rows = tb.xpath(".//tr")
                if not rows:
                    continue

                header_idx = None
                close_idx = None

                for ridx, tr in enumerate(rows[:5]):  # 最初の数行だけヘッダー候補として見る
                    cells = tr.xpath("./th|./td")
                    headers = [re.sub(r'\s+', ' ', c.text_content().strip()) for c in cells]
                    if any("終値" in h for h in headers):
                        header_idx = ridx
                        for idx, h in enumerate(headers):
                            if "終値" in h:
                                close_idx = idx
                                break
                        break

                if header_idx is None or close_idx is None:
                    continue

                for tr in rows[header_idx + 1:]:
                    tds = [re.sub(r'\s+', ' ', td.text_content().strip()) for td in tr.xpath("./td")]
                    if len(tds) <= close_idx:
                        continue

                    ctxt = tds[close_idx]
                    cnum = re.sub(r"[^\d.\-]", "", ctxt)
                    if cnum not in ("", "-", ".", "-."):
                        try:
                            v = float(cnum)
                            if v > 0:
                                closes.append(v)
                                found = True
                        except Exception:
                            pass

            if not found:
                print(f"[WARN] Kabutan closes: table not parsed on page {page} for {code}", flush=True)

        except Exception as e:
            print(f"[ERR] Kabutan closes fetch {url} -> {e}", flush=True)

        page += 1
        polite_sleep(1.0)

    return closes

def kabutan_dev25_from_trend(code):
    """
    株探の基本情報ページの『株価トレンド』欄から
    25日線の乖離率を直接取得する。
    """
    url = KABU_OVERVIEW.format(code=code)
    try:
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code != 200 or not r.text:
            print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
            return ""

        doc = LH.fromstring(r.text)
        text = re.sub(r"\s+", " ", doc.text_content())

        # 株価トレンド欄の 5/25/75/200日線 と 4つの% を拾う
        m = re.search(
            r"5日線\s*25日線\s*75日線\s*200日線\s*"
            r"([+\-]?\d+(?:\.\d+)?)％\s*"
            r"([+\-]?\d+(?:\.\d+)?)％\s*"
            r"([+\-]?\d+(?:\.\d+)?)％\s*"
            r"([+\-]?\d+(?:\.\d+)?)％",
            text
        )
        if m:
            return m.group(2)  # 2番目が25日線

    except Exception as e:
        print(f"[ERR] kabutan_dev25_from_trend {url} -> {e}", flush=True)

    return ""

def calc_deviation_25ma(code):
    """
    25MA乖離率[%] の優先順位:
      1) 株探『株価トレンド』欄の25日線乖離率を直接取得
      2) J-Quants
      3) Stooq
      4) 株探の時系列再計算
    """

    # 1) 株探の『株価トレンド』欄を最優先
    v = kabutan_dev25_from_trend(code)
    if v != "":
        print(f"[DEBUG-25MA] {code} source=kabutan_trend dev25={v}", flush=True)
        return v

    # 2) J-Quants
    closes = jquants_closes_any(code)
    if len(closes) >= 25:
        recent_25 = closes[-25:]
        last = recent_25[-1]
        ma25 = sum(recent_25) / 25.0
        print(f"[DEBUG-25MA] {code} source=jquants last={last} ma25={ma25} closes={recent_25}", flush=True)
        if ma25 > 0:
            dev = (last / ma25 - 1.0) * 100.0
            if -80 <= dev <= 80:
                return str(round(dev, 4))

    # 3) Stooq
    closes = stooq_closes_any(code)
    if len(closes) >= 25:
        recent_25 = closes[-25:]
        last = recent_25[-1]
        ma25 = sum(recent_25) / 25.0
        print(f"[DEBUG-25MA] {code} source=stooq last={last} ma25={ma25} closes={recent_25}", flush=True)
        if ma25 > 0:
            dev = (last / ma25 - 1.0) * 100.0
            if -80 <= dev <= 80:
                return str(round(dev, 4))

    # 4) 株探の時系列再計算
    closes = kabutan_closes_any(code)
    if len(closes) >= 25:
        recent_25 = closes[:25]
        last = recent_25[0]
        ma25 = sum(recent_25) / 25.0
        print(f"[DEBUG-25MA] {code} source=kabutan last={last} ma25={ma25} closes={recent_25}", flush=True)
        if ma25 > 0:
            dev = (last / ma25 - 1.0) * 100.0
            if -80 <= dev <= 80:
                return str(round(dev, 4))

    return ""

# ====== Main ======
def main():
    with open("tickers.txt", "r", encoding="utf-8") as f:
        raw = [line for line in f if line.strip()]
    codes = [normalize_code_line(x) for x in raw]
    codes = [re.sub(r"[^\w]", "", c) for c in codes]
    codes = list(dict.fromkeys(codes))  # de-dup

    offset = int(os.getenv("OFFSET", "0"))
    limit  = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        codes = codes[offset:offset+limit]

    print(f"Total tickers to process in this shard: {len(codes)}", flush=True)
    print(f"[DEBUG-JQ] token_exists={bool(os.getenv('JQUANTS_ID_TOKEN', '').strip())}", flush=True)

    out = []
    for i, code in enumerate(codes, 1):
        print(f"[{i}/{len(codes)}] {code} start", flush=True)

        # Volumes (必須)
        vol5, vol25, vratio = get_vols(code)

        # IRBANK CSV（算出バックアップ用）
        eps, bps, ni, eq, assets, dps = fetch_eps_bps_profit_equity_assets_dps(code)
        roe_pct_calc = ""
        eqr_pct_calc = ""
        if ni != "" and eq != "":
            roe = safe_div(ni, eq)
            roe_pct_calc = to_pct(roe) if roe != "" else ""
        if eq != "" and assets != "":
            eqr = safe_div(eq, assets)
            eqr_pct_calc = to_pct(eqr) if eqr != "" else ""

        # 株探/IRBANKから直接（優先）
        per      = kabu_per(code) or ""
        pbr      = kabu_pbr(code) or ir_pbr(code) or ""
        roe_pct  = kabu_roe_pct(code) or (str(roe_pct_calc) if roe_pct_calc != "" else "")
        eqr_pct  = jquants_equity_ratio_pct(code) or kabu_equity_ratio_pct(code) or (str(eqr_pct_calc) if eqr_pct_calc != "" else "")
        print(f"[DEBUG-EQR] {code} eqr%={eqr_pct} eq={eq} assets={assets} eqr_pct_calc={eqr_pct_calc}", flush=True)
        divy_pct = kabu_divy_pct(code)
        credit   = kabu_credit(code) or ir_credit(code) or ""

        # 25MA 乖離率（%）
        dev25_pct = calc_deviation_25ma(code)

        if i == 1:
            print(f"[DEBUG] {code} per={per} pbr={pbr} roe%={roe_pct} eqr%={eqr_pct} divy%={divy_pct} credit={credit} v5={vol5} v25={vol25} vr={vratio} dev25%={dev25_pct}", flush=True)

        op_yoy = fetch_opinc_yoy(code)

        out.append([
            code,
            per, pbr, roe_pct, eqr_pct, divy_pct,
            op_yoy, credit, vol5, vol25, vratio,
            dev25_pct
        ])
        polite_sleep(0.6)

    with open("metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "code","per","pbr","roe_pct","equity_ratio_pct",
            "dividend_yield_pct","op_income_yoy_pct","credit_ratio",
            "vol5","vol25","volratio_5_25",
            "deviation_25ma_pct"
        ])
        w.writerows(out)
    print("metrics.csv written", flush=True)

if __name__ == "__main__":
    main()
