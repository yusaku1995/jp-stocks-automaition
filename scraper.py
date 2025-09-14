import os, io, csv, re, time, requests
from lxml import html as LH

# =====================
# 設定
# =====================

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

RETRIES = 6  # リトライ回数

IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
MARGIN = "https://irbank.net/{code}/margin"
IR_HTML = "https://irbank.net/{code}"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE = "https://kabutan.jp/stock/finance?code={code}"

CSV_PL = "fy-profit-and-loss.csv"
CSV_BS = "fy-balance-sheet.csv"
CSV_DIV= "fy-stock-dividend.csv"
CSV_QQ_YOY_OP = "qq-yoy-operating-income.csv"

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
    if p == "": return ""
    try:
        x = float(p)
        if x < lo or x > hi: return ""
        return str(x)
    except: return ""

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
                        if isinstance(n, str): parts.append(n)
                        else: parts.append(n.text_content())
                    text = " ".join(parts).strip()
                    text = re.sub(r"\s+", " ", text)
                    return text
                else:
                    print(f"[WARN] XPath no match: {url} :: {xp}", flush=True)
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.2 + i*0.6)
    return ""

def _cell_num_from_table(url: str, header_tokens: list[str]) -> str:
    cond = " and ".join([f"contains(normalize-space(.), '{t}')" for t in header_tokens])
    xp1 = f"//th[{cond}]/following-sibling::td[1]"
    t = _get_first_text_by_xpath(url, xp1)
    if t:
        v = _pct_or_num(t)
        if v != "": return v
    xp2 = f"//dt[{cond}]/following-sibling::dd[1]"
    t = _get_first_text_by_xpath(url, xp2)
    if t:
        v = _pct_or_num(t)
        if v != "": return v
    xp3 = f"(//*[self::th or self::dt or self::*][{cond}])[1]/following-sibling::*[1]"
    t = _get_first_text_by_xpath(url, xp3)
    if t:
        v = _pct_or_num(t)
        if v != "": return v
    return ""

# =====================
# Kabutan extractors
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
        v = _cell_num_from_table(url, ["自己資本利益率"])
        if v != "": return _pct_sanitize(v, lo=-200, hi=200)
        t = _get_first_text_by_xpath(url, "//*[contains(normalize-space(.),'ROE')]/following::td[1]")
        v = _pct_or_num(t)
        if v != "": return _pct_sanitize(v, lo=-200, hi=200)
    url_ir = IR_HTML.format(code=code)
    t = _get_first_text_by_xpath(url_ir, "(//*[contains(text(),'ROE') or contains(text(),'自己資本利益率')])[1]/following::text()[1]")
    return _pct_sanitize(_pct_or_num(t), lo=-200, hi=200)

def kabu_equity_ratio_pct(code):
    for url in (KABU_FINANCE.format(code=code), KABU_OVERVIEW.format(code=code)):
        v = _cell_num_from_table(url, ["自己資本比率"])
        if v != "": return _pct_sanitize(v, lo=0, hi=100)
        t = _get_first_text_by_xpath(url, "//*[contains(text(),'自己資本比率')]/following::td[1]")
        v = _pct_or_num(t)
        if v != "": return _pct_sanitize(v, lo=0, hi=100)
    url_ir = IR_HTML.format(code=code)
    t = _get_first_text_by_xpath(url_ir, "(//*[contains(text(),'自己資本比率')])[1]/following::text()[1]")
    return _pct_sanitize(_pct_or_num(t), lo=0, hi=100)

def kabu_divy_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    v = _cell_num_from_table(url, ["配当", "利回"])
    if v != "": return _pct_sanitize(v, lo=-10, hi=200)
    t = _get_first_text_by_xpath(url, "//th[contains(.,'配当利回り')]/following-sibling::td[1]")
    v = _pct_or_num(t)
    if v != "": return _pct_sanitize(v, lo=-10, hi=200)
    url_ir = IR_HTML.format(code=code)
    t = _get_first_text_by_xpath(url_ir, "(//*[contains(text(),'配当利回り')])[1]/following::text()[1]")
    return _pct_sanitize(_pct_or_num(t), lo=-10, hi=200)

# =====================
# IRBANK CSV
# =====================

def get_csv(code, path):
    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.ok and "text/csv" in r.headers.get("Content-Type",""):
                rows = list(csv.reader(io.StringIO(r.text)))
                if len(rows) >= 2: return rows
        except: pass
        time.sleep(2+i)
    return None

def fetch_opinc_yoy(code):
    qq = get_csv(code, CSV_QQ_YOY_OP)
    if not qq: return ""
    col = [row[1] for row in qq[1:] if len(row) > 1]
    for v in reversed(col):
        s = re.sub(r'[^0-9.\-]', '', v or "")
        if s not in ("", "-", ".", "-."): return s
    return ""

# =====================
# Stooq & Credit
# =====================

def stooq_close_vols(code):
    if not re.fullmatch(r"\d{4}", code):
        print(f"[SKIP] Stooq not supported for {code}", flush=True)
        return None,"","",""
    url = STOOQ.format(code=code)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            lines = [x.split(',') for x in r.text.strip().splitlines()]
            if len(lines) >= 2:
                rows = lines[1:]
                vols = [int(x[5]) for x in rows if len(x) >= 6 and x[5].isdigit()]
                close = None
                for x in reversed(rows):
                    if len(x) >= 6:
                        try: close = float(x[4]); break
                        except: pass
                vol5 = int(sum(vols[-5:])/5) if len(vols) >= 5 else ""
                vol25= int(sum(vols[-25:])/25) if len(vols) >= 25 else ""
                vratio= (vol5/vol25) if (vol5 and vol25) else ""
                return close, vol5, vol25, vratio
    except Exception as e:
        print(f"[ERR] Stooq {code}: {e}", flush=True)
    return None,"","",""

def fetch_credit_ratio(code):
    url = MARGIN.format(code=code)
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200:
            m = re.search(r"信用倍率[^0-9]*(\d+(?:\.\d+)?)倍", r.text)
            if m: return m.group(1)
    except: pass
    return ""

# =====================
# Main
# =====================

def main():
    with open("tickers.txt","r",encoding="utf-8") as f:
        codes=[x.strip() for x in f if x.strip()]
    offset=int(os.getenv("OFFSET","0"))
    limit=int(os.getenv("MAX_TICKERS","0"))
    if limit>0: codes=codes[offset:offset+limit]
    out=[]
    for i,code in enumerate(codes,1):
        print(f"[{i}/{len(codes)}] {code} start",flush=True)
        close,vol5,vol25,vratio=stooq_close_vols(code)
        per=kabu_per(code)
        pbr=kabu_pbr(code)
        roe=kabu_roe_pct(code)
        eqr=kabu_equity_ratio_pct(code)
        divy=kabu_divy_pct(code)
        opy=fetch_opinc_yoy(code)
        credit=fetch_credit_ratio(code)
        out.append([code,close or "",per,pbr,roe,eqr,divy,opy,credit,vol5,vol25,vratio])
    with open("metrics.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["code","close","per","pbr","roe_pct","equity_ratio_pct","dividend_yield_pct","op_income_yoy_pct","credit_ratio","vol5","vol25","volratio_5_25"])
        w.writerows(out)
    print("metrics.csv written",flush=True)

if __name__=="__main__":
    main()
