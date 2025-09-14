# -*- coding: utf-8 -*-
# jp-stocks-automation scraper (Kabutan/IRBANK + XPath, uses codes exactly as given; supports 215A 等)
import os, io, csv, re, time, random, requests
from lxml import html as LH  # XPath 用

# ========= 基本設定 =========
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

# 取得元（コードは“そのまま”使う）
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
IR_HTML= "https://irbank.net/{code}"
STOOQ  = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"

# IRBANK CSVパス
CSV_PL="fy-profit-and-loss.csv"
CSV_BS="fy-balance-sheet.csv"
CSV_DIV="fy-stock-dividend.csv"
CSV_QQ="qq-yoy-operating-income.csv"
CSV_PS="fy-per-share.csv"

# ========= ユーティリティ =========
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

# ========= IRBANK CSV 用：見出しマッチ支援 =========
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
        if not r:
            continue
        head = _norm_label(r[0])
        if not head:
            continue
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

# ========= IRBANK CSV 取得（入力コードを“そのまま”使う）=========
def get_csv(code, path):
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
                    polite_sleep(1.5)
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

# ========= Stooq（日足・出来高）=========
def stooq_close_vols(code):
    """
    入力コードを“そのまま”使う（215A など英字付きは Stooq 側で失敗し得る）。
    失敗しても後段で Kabutan から close を補完するのでOK。
    """
    url = STOOQ.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=15)
            if r.status_code == 200 and "\n" in r.text:
                rows = [x.split(',') for x in r.text.strip().splitlines()][1:]
                if not rows:
                    break
                close = None
                for x in reversed(rows):
                    if len(x) >= 5:
                        try:
                            close = float(x[4]); break
                        except:
                            pass
                vols = []
                for x in rows:
                    if len(x) >= 6:
                        try:
                            vols.append(int(x[5]))
                        except:
                            vols.append(0)
                print(f"[OK] {url} days={len(rows)} close={close}", flush=True)
                vol5 = vol25 = vratio = ""
                if len(vols) >= 25:
                    vol5  = int(sum(vols[-5:]) / 5)
                    vol25 = int(sum(vols[-25:]) / 25)
                    vratio = (vol5/vol25) if vol25 else ""
                return close, vol5, vol25, vratio
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1 + i)
    print(f"[FAIL] {url}", flush=True)
    return None, "", "", ""

# ========= HTML + XPath（Sheets と同等の抽出）=========
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
                        if isinstance(n, str):
                            parts.append(n)
                        else:
                            parts.append(n.text_content())
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

# Kabutan overview（PER/PBR/ROE/配当利回り/信用倍率/現在値）
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
        v = _num_only(t)
        if v != "":
            return v
    url_f = KABU_FINANCE.format(code=code)
    candidates_f = [
        "//th[.//text()[contains(.,'ROE')]]/following-sibling::td[1]",
        "//*[contains(text(),'ROE')]/following::td[1]",
    ]
    for xp in candidates_f:
        t = _get_first_text_by_xpath(url_f, xp)
        v = _num_only(t)
        if v != "":
            return v
    url_ir = IR_HTML.format(code=code)
    t = _get_first_text_by_xpath(url_ir, "(//*[contains(text(),'ROE')])[1]/following::text()[1]")
    return _num_only(t)

def kabu_divy_pct(code):
    u
