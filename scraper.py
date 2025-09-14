import os, io, csv, re, time, requests
from lxml import html as LH

# =====================
# 設定
# =====================

HEADERS = {
    # CSV/HTML 両方取りに行けるよう Accept を広めに
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv, text/plain;q=0.95, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

RETRIES = 6  # リトライ回数（IRBANKの制限対策）

IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
MARGIN = "https://irbank.net/{code}/margin"
IR_HTML = "https://irbank.net/{code}"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE = "https://kabutan.jp/stock/finance?code={code}"

CSV_PL = "fy-profit-and-loss.csv"   # 損益 (EPS, 当期純利益)
CSV_BS = "fy-balance-sheet.csv"     # 貸借 (自己資本, 総資産, BPS)
CSV_DIV= "fy-stock-dividend.csv"    # 配当 (DPS)
CSV_QQ_YOY_OP = "qq-yoy-operating-income.csv"  # 営業利益 YoY

# =====================
# ユーティリティ
# =====================

def polite_sleep(sec): time.sleep(sec)

def _headers(): return HEADERS

_num_re = re.compile(r"(-?\d+(?:\.\d+)?)")

def _num_only(s: str) -> str:
    if not s: return ""
    m = _num_re.search(s)
    return m.group(1) if m else ""

def _pct_or_num(text: str) -> str:
    if not text: return ""
    t = text.strip()
    if t in ("", "-", "—", "–", "―"): return ""
    return _num_only(t)

def _pct_sanitize(p: str, lo: float, hi: float) -> str:
    # %系の異常値は空に
    if p == "": return ""
    try:
        x = float(p)
        if x < lo or x > hi: return ""
        return str(x)
    except: return ""

def _html_get(url, tries=3, timeout=25):
    for i in range(tries):
        try:
            r = requests.get(url, headers=_headers(), timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.2 + i*0.8)
    return ""

def _get_first_text_by_xpath(url, xp):
    html = _html_get(url)
    if not html:
        print(f"[WARN] HTML empty: {url}", flush=True)
        return ""
    try:
        doc = LH.fromstring(html)
        nodes = doc.xpath(xp)
        if nodes:
            parts = []
            for n in nodes:
                if isinstance(n, str): parts.append(n)
                else: parts.append(n.text_content())
            text = " ".join(parts).strip()
            text = re.sub(r"\s+", " ", text)
            return text
        else:
            print(f"[WARN] XPath no match: {url} :: {xp}", flush=True)
            return ""
    except Exception as e:
        print(f"[ERR] XPath parse {url} -> {e}", flush=True)
        return ""

def _cell_num_from_table(url: str, header_tokens: list[str]) -> str:
    """
    th/td だけでなく dt/dd 構造も対象にし、セル“内”の文字列から数値抽出。
    header_tokens の各語をすべて含む見出しを探す。
    """
    cond = " and ".join([f"contains(normalize-space(.), '{t}')" for t in header_tokens])

    # 1) table: th → 右隣の td
    xp1 = f"//th[{cond}]/following-sibling::td[1]"
    t = _get_first_text_by_xpath(url, xp1)
    if t:
        v = _pct_or_num(t)
        if v != "": return v

    # 2) dl定義リスト: dt → 直後の dd
    xp2 = f"//dt[{cond}]/following-sibling::dd[1]"
    t = _get_first_text_by_xpath(url, xp2)
    if t:
        v = _pct_or_num(t)
        if v != "": return v

    # 3) li/グリッドなど: 見出しノードの直近兄弟
    xp3 = f"(//*[self::th or self::dt or self::*][{cond}])[1]/following-sibling::*[1]"
    t = _get_first_text_by_xpath(url, xp3)
    if t:
        v = _pct_or_num(t)
        if v != "": return v

    return ""

def safe_div(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0: return ""
        return a/b
    except:
        return ""

def to_pct(x):
    try:
        return float(x)*100.0
    except:
        return ""

# =====================
# IRBANK CSV
# =====================

def get_csv(code, path):
    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            ctype = r.headers.get("Content-Type", "")
            if r.ok and ("text/csv" in ctype or "application/octet-stream" in ctype):
                rows = list(csv.reader(io.StringIO(r.text)))
                if len(rows) >= 1:
                    return rows
            else:
                # “表示制限中”などの場合はHTMLが返ることがある
                preview = (r.text or "")[:120].replace("\n"," ")
                print(f"[WARN] {url} -> {r.status_code} {ctype} head='{preview}'", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        time.sleep(2 + 1.5*i)
    print(f"[FAIL] {url} retried {RETRIES}x", flush=True)
    return None

def row_index_by_keys(rows, keys):
    if not rows: return None
    norm_keys = [re.sub(r'[\s　,/％%円¥\-–—()]', '', str(k)) for k in keys]
    for i, r in enumerate(rows):
        if not r: continue
        head = re.sub(r'[\s　,/％%円¥\-–—()]', '', str(r[0] or ""))
        if not head: continue
        for nk in norm_keys:
            if nk and (nk in head or head in nk):
                return i
    return None

def last_num_in_row(rows, ridx):
    if ridx is None: return ""
    r = rows[ridx]
    for x in reversed(r[1:]):  # 2列目以降が数値データ列
        if x is None: continue
        s = str(x).replace(',', '').strip()
        if s in ("", "-", "—", "–", "―"): continue
        try:
            return float(s)
        except:
            continue
    return ""

# ラベル候補
EPS_KEYS = ["EPS","EPS(円)","EPS（円）","1株当たり利益","1株当たり当期純利益","1株当たり当期純利益(円)","1株当たり当期純利益（円）","1株当たり純利益"]
BPS_KEYS = ["BPS","BPS(円)","BPS（円）","1株当たり純資産","1株当たり純資産(円)","1株当たり純資産（円）","1株純資産"]
NI_KEYS  = ["当期純利益","親会社株主に帰属する当期純利益","純利益"]
EQ_KEYS  = ["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計"]
AS_KEYS  = ["総資産","資産合計","資産総額"]
DPS_KEYS = ["1株配当","1株配当金","配当金","配当(円)","配当（円）","1株当たり配当金"]

def fetch_eps_bps_equity_assets_dps(code):
    pl = get_csv(code, CSV_PL)
    bs = get_csv(code, CSV_BS)
    dv = get_csv(code, CSV_DIV)

    eps = bps = netinc = equity = assets = dps = ""

    if pl:
        eps_row = row_index_by_keys(pl, EPS_KEYS)
        ni_row  = row_index_by_keys(pl, NI_KEYS)
        eps     = last_num_in_row(pl, eps_row)
        netinc  = last_num_in_row(pl, ni_row)

    if bs:
        bps_row = row_index_by_keys(bs, BPS_KEYS)
        eq_row  = row_index_by_keys(bs, EQ_KEYS)
        as_row  = row_index_by_keys(bs, AS_KEYS)
        bps     = last_num_in_row(bs, bps_row)
        equity  = last_num_in_row(bs, eq_row)
        assets  = last_num_in_row(bs, as_row)

    if dv:
        dps_row = row_index_by_keys(dv, DPS_KEYS)
        dps     = last_num_in_row(dv, dps_row)

    return eps, bps, netinc, equity, assets, dps

def fetch_opinc_yoy(code):
    qq = get_csv(code, CSV_QQ_YOY_OP)
    if not qq: return ""
    col = [row[1] for row in qq[1:] if len(row) > 1]
    for v in reversed(col):
        s = re.sub(r'[^0-9.\-]', '', v or "")
        if s not in ("", "-", ".", "-."): return s
    return ""

# =====================
# Stooq & Kabutan Close / Credit
# =====================

def stooq_close_vols(code):
    if not re.fullmatch(r"\d{4}", code):
        print(f"[SKIP] Stooq not supported for {code}", flush=True)
        return None,"","",""
    url = STOOQ.format(code=code)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and "\n" in r.text:
            lines = [x.split(',') for x in r.text.strip().splitlines()]
            if len(lines) >= 2:
                rows = lines[1:]
                vols = [int(x[5]) for x in rows if len(x) >= 6 and x[5].isdigit()]
                close = None
                for x in reversed(rows):
                    if len(x) >= 6:
                        try: close = float(x[4]); break
                        except: pass
                vol5  = int(sum(vols[-5:])/5)  if len(vols) >= 5  else ""
                vol25 = int(sum(vols[-25:])/25) if len(vols) >= 25 else ""
                vratio= (vol5/vol25) if (vol5 and vol25) else ""
                return close, vol5, vol25, vratio
        print(f"[WARN] Stooq no rows/HTTP {r.status_code}: {url}", flush=True)
    except Exception as e:
        print(f"[ERR] Stooq {code}: {e}", flush=True)
    return None,"","",""

def kabu_close(code):
    # Kabutan 概要の「現在値」を拾う（英字付きティッカー用フォールバック）
    url = KABU_OVERVIEW.format(code=code)
    # dt/dd（現在値）
    t = _cell_num_from_table(url, ["現在値"])
    if t != "": 
        try: return float(t)
        except: return None
    # 代替：th→td
    txt = _get_first_text_by_xpath(url, "//th[contains(.,'現在値')]/following-sibling::td[1]")
    v = _pct_or_num(txt)
    if v != "":
        try: return float(v)
        except: return None
    return None

def fetch_credit_ratio(code):
    # IRBANK（最優先）
    url = MARGIN.format(code=code)
    html = _html_get(url)
    if html:
        m = re.search(r"信用倍率[^0-9]*(\d+(?:\.\d+)?)倍", html)
        if m: return m.group(1)

    # Kabutan フォールバック（概要に出ることがある）
    url2 = KABU_OVERVIEW.format(code=code)
    v = _cell_num_from_table(url2, ["信用", "倍率"])
    if v != "": return v

    return ""

# =====================
# Kabutan（指標のセル限定抽出）
# =====================

def kabu_per(code):
    url = KABU_OVERVIEW.format(code=code)
    v = _cell_num_from_table(url, ["PER"])
    if v != "": return v
    t = _get_first_text_by_xpath(url, "//th[contains(.,'PER')]/following-sibling::td[1]")
    return _pct_or_num(t)

def kabu_pbr(code):
    url = KABU_OVERVIEW.format(code=code)
    v = _cell_num_from_table(url, ["PBR"])
    if v != "": return v
    t = _get_first_text_by_xpath(url, "//th[contains(.,'PBR')]/following-sibling::td[1]")
    return _pct_or_num(t)

def kabu_roe_pct(code):
    for url in (KABU_OVERVIEW.format(code=code), KABU_FINANCE.format(code=code)):
        v = _cell_num_from_table(url, ["ROE"])
        if v != "": return _pct_sanitize(v, lo=-200, hi=200)
