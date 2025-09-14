# -*- coding: utf-8 -*-
# jp-stocks-automation scraper (Kabutan/IRBANK + XPath) — supports alphanumeric codes like 215A

import os, io, csv, re, time, random, requests
from lxml import html as LH

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
IR_CSV  = "https://f.irbank.net/files/{code}/{path}"               # CSVは4桁数字で安定（英字は多くが無い）
IR_HTML = "https://irbank.net/{code}"                               # HTMLは英字付きも可（フォールバックに使用）
STOOQ   = "https://stooq.com/q/d/l/?s={code}.jp&i=d"               # ★英字付きでもユーザー環境で取れていた想定でそのまま使用
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"
KABU_KABUKA   = "https://kabutan.jp/stock/kabuka?code={code}&ashi=day&page={page}"

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
EQ_KEYS =["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計"]
AS_KEYS =["総資産","資産合計","資産総額"]
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
    # CSVは4桁数字で安定。英字付きは多くが存在しないため試行しても404が多い。
    if not is_numeric4(code):
        print(f"[SKIP] IRBANK CSV likely missing for non-4digit: {code}", flush=True)
        return None
    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=_headers(), timeout=20)
            ctype = r.headers.get("Content-Type","")
            if not r.ok:
                print(f"[WARN] {url} -> HTTP {r.status_code}", flush=True)
            elif "text/csv" not in ctype and "application/octet-stream" not in ctype:
                head=(r.text or "")[:200].replace("\n"," ")
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

def fetch_opinc_yoy(code):
    qq = get_csv(code, CSV_QQ)
    if not qq:
        return ""
    for row in reversed(qq[1:]):
        if len(row) <= 1:
            continue
        s = re.sub(r'[^0-9.\-]', '', row[1] or "")
        if s in ("", "-", ".", "-."):
            continue
        return s
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

def _num_only(s):
    if not s:
        return ""
    m = _num_re.search(s)
    return m.group(1) if m else ""

def _num_pct_sane(s):
    """数値文字列を%として採用。±1000% を超える異常値は空にする。"""
    v = _num_only(s)
    if v == "": return ""
    try:
        f = float(v)
        if abs(f) > 1000:
            return ""    # 異常な巨大値は無効化
        return str(f)
    except:
        return ""

# ====== Kabutan ======
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
    # IRBANK fallback
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'ROE')])[1]/following::text()[1]")
    return _num_pct_sane(t)

def kabu_divy_pct(code):
    # ％をそのまま取る（再計算しない）
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
    # IRBANK fallback
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

def kabu_equity_ratio_pct(code):
    url = KABU_FINANCE.format(code=code)
    for xp in [
        "//th[.//text()[contains(.,'自己資本比率')]]/following-sibling::td[1]",
        "//*[contains(text(),'自己資本比率')]/following::td[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li][1]/*[self::td or self::dd][1]",
    ]:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_pct_sane(t)
        if v != "":
            return v
    # 概要ページ側
    t = _get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), "//*[contains(text(),'自己資本比率')][1]/following::text()[1]")
    v = _num_pct_sane(t)
    if v != "":
        return v
    # IRBANK fallback（テキスト直取り）
    txt = _get_first_text_by_xpath(IR_HTML.format(code=code), "//*[contains(text(),'自己資本比率')][1]/following::text()[1]")
    if txt == "":
        # ページにより位置が違うことがあるので全文から正規表現で
        try:
            r = requests.get(IR_HTML.format(code=code), headers=_headers(), timeout=20)
            if r.status_code == 200 and r.text:
                m = re.search(r"自己資本比率[^0-9\-]*(\-?\d+(?:\.\d+)?)\s*%", r.text)
                if m:
                    return _num_pct_sane(m.group(1))
        except Exception:
            pass
    return _num_pct_sane(txt)

def ir_pbr(code):
    return _num_only(_get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'PBR')])[1]/following::text()[1]"))

def ir_credit(code):
    return _num_only(_get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'信用倍率')])[1]/following::text()[1]"))

# ====== Volumes (5/25) ======
def stooq_vols_any(code):
    """Stooqにそのまま投げる（215A など英字付きもユーザー環境で取得可想定）"""
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
    """株探『過去の株価』から出来高25営業日ぶん以上を収集（フォールバック用）"""
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
                        vtxt = tds[-1]
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
        eqr_pct  = kabu_equity_ratio_pct(code) or (str(eqr_pct_calc) if eqr_pct_calc != "" else "")
        divy_pct = kabu_divy_pct(code)  # %をそのまま
        credit   = kabu_credit(code) or ir_credit(code) or ""

        if i == 1:
            print(f"[DEBUG] {code} per={per} pbr={pbr} roe%={roe_pct} eqr%={eqr_pct} divy%={divy_pct} credit={credit} v5={vol5} v25={vol25} vr={vratio}", flush=True)

        op_yoy = fetch_opinc_yoy(code)  # 英字付きは空のことが多い

        out.append([
            code,
            per, pbr, roe_pct, eqr_pct, divy_pct,
            op_yoy, credit, vol5, vol25, vratio
        ])
        polite_sleep(0.6)

    with open("metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "code","per","pbr","roe_pct","equity_ratio_pct",
            "dividend_yield_pct","op_income_yoy_pct","credit_ratio",
            "vol5","vol25","volratio_5_25"
        ])
        w.writerows(out)
    print("metrics.csv written", flush=True)

if __name__ == "__main__":
    main()
