# -*- coding: utf-8 -*-
import os, io, csv, re, time, random, requests
from lxml import html as LH  # ← XPath で使う

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
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ  = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
IR_HTML= "https://irbank.net/{code}"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"

CSV_PL="fy-profit-and-loss.csv"; CSV_BS="fy-balance-sheet.csv"; CSV_DIV="fy-stock-dividend.csv"; CSV_QQ="qq-yoy-operating-income.csv"; CSV_PS="fy-per-share.csv"

EPS_KEYS=["EPS","EPS(円)","EPS（円）","1株当たり利益","1株当たり当期純利益","1株当たり当期純利益(円)","1株当たり当期純利益（円）","1株当たり純利益"]
BPS_KEYS=["BPS","BPS(円)","BPS（円）","1株当たり純資産","1株当たり純資産(円)","1株当たり純資産（円）","1株純資産"]
DPS_KEYS=["1株配当","1株配当金","配当金","配当(円)","配当（円）","1株当たり配当金"]
EQ_KEYS =["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計"]
AS_KEYS =["総資産","資産合計","資産総額"]
NI_KEYS =["当期純利益","親会社株主に帰属する当期純利益","純利益"]

def _norm_label(s):
    if s is None: return ""
    s=str(s)
    s=re.sub(r'（.*?）','',s); s=re.sub(r'\(.*?\)','',s); s=re.sub(r'[\s　,/％%円¥\-–—]','',s)
    return s
def row_index_by_keys(rows, keys):
    if not rows: return None
    K=[_norm_label(k) for k in keys]
    for i,r in enumerate(rows):
        if not r: continue
        h=_norm_label(r[0]); 
        if not h: continue
        for nk in K:
            if nk and (nk in h or h in nk): return i
    return None
def last_num_in_row(rows, ridx):
    if ridx is None: return ""
    for x in reversed(rows[ridx][1:]):
        if x is None: continue
        s=str(x).replace(',','').strip()
        if s in ("","-","—","–","―"): continue
        try: return float(s)
        except: pass
    return ""

def polite_sleep(sec): time.sleep(sec+random.uniform(0.1,0.6))
def safe_div(a,b):
    try:
        a=float(a); b=float(b); 
        if b==0: return ""
        return a/b
    except: return ""
def to_pct(x):
    try: return float(x)*100.0
    except: return ""

def is_supported_code(code): return bool(re.fullmatch(r"\d{4}", code))
def normalize_code_line(line):
    token=re.split(r"[\s,\t]+", line.strip())[0] if line else ""
    try:
        import unicodedata; token=unicodedata.normalize("NFKC", token)
    except: pass
    return token.strip().upper()

# ------------ IRBANK CSV -------------
def get_csv(code, path):
    url=IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r=requests.get(url, headers=_headers(), timeout=20)
            c=r.headers.get("Content-Type","")
            if not r.ok:
                print(f"[WARN] {url} -> HTTP {r.status_code}", flush=True)
            elif "text/csv" not in c and "application/octet-stream" not in c:
                head=(r.text or "")[:200].replace("\n"," ")
                print(f"[WARN] {url} -> non-CSV ({c}). head='{head}'", flush=True)
            else:
                rows=list(csv.reader(io.StringIO(r.text)))
                if len(rows)>=2:
                    print(f"[OK] {url} rows={len(rows)}", flush=True)
                    polite_sleep(2.0); return rows
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(2+2*i)
    print(f"[FAIL] {url} retried {RETRIES}x", flush=True); return None

def fetch_eps_bps_profit_equity_assets_dps(code):
    pl=get_csv(code,CSV_PL); bs=get_csv(code,CSV_BS); dv=get_csv(code,CSV_DIV); ps=get_csv(code,CSV_PS)
    eps=bps=ni=eq=assets=dps=""
    if pl:
        eps=last_num_in_row(pl, row_index_by_keys(pl, EPS_KEYS))
        ni =last_num_in_row(pl, row_index_by_keys(pl, NI_KEYS))
    if eps=="" and ps:
        eps=last_num_in_row(ps, row_index_by_keys(ps, EPS_KEYS))
    if bs:
        bps   =last_num_in_row(bs, row_index_by_keys(bs, BPS_KEYS))
        eq    =last_num_in_row(bs, row_index_by_keys(bs, EQ_KEYS))
        assets=last_num_in_row(bs, row_index_by_keys(bs, AS_KEYS))
    if bps=="" and ps:
        bps=last_num_in_row(ps, row_index_by_keys(ps, BPS_KEYS))
    if dv:
        dps=last_num_in_row(dv, row_index_by_keys(dv, DPS_KEYS))
    if dps=="" and ps:
        dps=last_num_in_row(ps, row_index_by_keys(ps, DPS_KEYS))
    return eps,bps,ni,eq,assets,dps

def fetch_opinc_yoy(code):
    qq=get_csv(code,CSV_QQ)
    if not qq: return ""
    for row in reversed(qq[1:]):
        if len(row)<=1: continue
        s=re.sub(r'[^0-9.\-]','',row[1] or "")
        if s in ("","-",".","-."): continue
        return s
    return ""

# ------------ Stooq --------------
def stooq_close_vols(code):
    url=STOOQ.format(code=code)
    for i in range(3):
        try:
            r=requests.get(url, headers=_headers(), timeout=15)
            if r.status_code==200 and "\n" in r.text:
                rows=[x.split(',') for x in r.text.strip().splitlines()][1:]
                close=None
                for x in reversed(rows):
                    if len(x)>=5:
                        try: close=float(x[4]); break
                        except: pass
                vols=[]
                for x in rows:
                    if len(x)>=6:
                        try: vols.append(int(x[5]))
                        except: vols.append(0)
                print(f"[OK] {url} days={len(rows)} close={close}", flush=True)
                vol5=vol25=vratio=""
                if len(vols)>=25:
                    vol5=int(sum(vols[-5:])/5); vol25=int(sum(vols[-25:])/25); vratio=(vol5/vol25) if vol25 else ""
                return close,vol5,vol25,vratio
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1+i)
    print(f"[FAIL] {url}", flush=True); return None,"","",""

# ------------ 共通：HTML + XPath 抽出 -------------
_num = re.compile(r"([0-9]+(?:\.[0-9]+)?)")
def _get_first_text_by_xpath(url, xpath_expr):
    for i in range(3):
        try:
            r=requests.get(url, headers=_headers(), timeout=25)
            if r.status_code==200 and r.text:
                doc=LH.fromstring(r.text)
                nodes=doc.xpath(xpath_expr)
                if nodes:
                    # normalize-space の代わりに Python 側でまとめて正規化
                    text=" ".join([n if isinstance(n,str) else n.text_content() for n in nodes]).strip()
                    text=re.sub(r"\s+"," ", text)
                    return text
                else:
                    print(f"[WARN] XPath no match: {url} :: {xpath_expr}", flush=True)
            else:
                print(f"[WARN] HTML HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] HTML fetch {url} -> {e}", flush=True)
        polite_sleep(1.5+i)
    return ""

def _num_only(s):
    if not s: return ""
    m=_num.search(s)
    return m.group(1) if m else ""

# ------------ Kabutan（XPathはシート式と同等） -------------
def kabu_per(code):
    xp = "//th[contains(.,'PER')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp)
    return _num_only(t)

def kabu_pbr(code):
    xp = "//th[contains(.,'PBR')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp)
    return _num_only(t)

def kabu_roe_pct(code):
    xp = "//th[contains(.,'ROE')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp)
    return _num_only(t)

def kabu_divy_pct(code):
    xp = "//th[contains(.,'配当利回り')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp)
    return _num_only(t)

def kabu_credit(code):
    # ページの構造変化に備えて2手用意
    xp1 = "//*[contains(text(),'信用倍率')][1]/following::text()[1]"
    xp2 = "//th[contains(.,'信用倍率')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp1) or \
      _get_first_text_by_xpath(KABU_OVERVIEW.format(code=code), xp2)
    return _num_only(t)

def kabu_equity_ratio_pct(code):
    xp = "//th[contains(.,'自己資本比率')]/following-sibling::td[1]"
    t=_get_first_text_by_xpath(KABU_FINANCE.format(code=code), xp)
    return _num_only(t)

# ------------ IRBANK（XPath フォールバック） -------------
def ir_credit(code):
    xp = "(//*[contains(text(),'信用倍率')])[1]/following::text()[1]"
    t=_get_first_text_by_xpath(IR_HTML.format(code=code), xp)
    return _num_only(t)

def ir_pbr(code):
    # ユーザーの式： (//*[contains(text(),'PBR（個')])[1]/following::text()[1]
    # ページ差分に備え、'PBR' 含むパターンで拾う
    xp = "(//*[contains(text(),'PBR')])[1]/following::text()[1]"
    t=_get_first_text_by_xpath(IR_HTML.format(code=code), xp)
    return _num_only(t)

# ------------ メイン -------------
def main():
    with open("tickers.txt","r",encoding="utf-8") as f:
        raw=[line for line in f if line.strip()]
    codes=[normalize_code_line(x) for x in raw]
    codes=[re.sub(r"[^\w]","",c) for c in codes]
    codes=list(dict.fromkeys(codes))
    filtered=[]
    for c in codes:
        if is_supported_code(c): filtered.append(c)
        else: print(f"[SKIP] Unsupported code: {c}", flush=True)

    offset=int(os.getenv("OFFSET","0")); limit=int(os.getenv("MAX_TICKERS","0"))
    if limit>0: filtered=filtered[offset:offset+limit]

    print(f"Total tickers to process in this shard: {len(filtered)}", flush=True)
    out=[]
    for i,code in enumerate(filtered,1):
        print(f"[{i}/{len(filtered)}] {code} start", flush=True)

        close,vol5,vol25,vratio = stooq_close_vols(code)
        eps,bps,ni,eq,assets,dps = fetch_eps_bps_profit_equity_assets_dps(code)

        per  = safe_div(close, eps) if (close is not None and eps!="") else ""
        pbr  = safe_div(close, bps) if (close is not None and bps!="") else ""
        roe  = safe_div(ni, eq) if (ni!="" and eq!="") else ""
        roe_pct  = to_pct(roe) if roe!="" else ""
        eqr      = safe_div(eq, assets) if (eq!="" and assets!="") else ""
        eqr_pct  = to_pct(eqr) if eqr!="" else ""
        dy   = safe_div(dps, close) if (dps!="" and close not in (None,"")) else ""
        dy_pct   = to_pct(dy) if dy!="" else ""

        # ---- XPath フォールバック（Sheets と同じ取り方）----
        # PER
        if per=="": 
            per = kabu_per(code) or ir_pbr(code) and ""  # PERはKabutan優先、IRはPBRしか拾えないので空
        # PBR
        if pbr=="": 
            pbr = kabu_pbr(code) or ir_pbr(code)
        # ROE%
        if roe_pct=="":
            roe_pct = kabu_roe_pct(code)
        # 自己資本比率%
        if eqr_pct=="":
            eqr_pct = kabu_equity_ratio_pct(code)
        # 配当利回り%
        if dy_pct=="":
            dy_pct = kabu_divy_pct(code)
        # 信用倍率
        credit = ir_credit(code) or kabu_credit(code)

        if i==1:
            print(f"[DEBUG] {code} eps={eps} bps={bps} ni={ni} eq={eq} as={assets} dps={dps}", flush=True)
            print(f"[DEBUG] per={per} pbr={pbr} roe%={roe_pct} eqr%={eqr_pct} divy%={dy_pct} credit={credit}", flush=True)

        out.append([code, close if close is not None else "", per, pbr, roe_pct, eqr_pct, dy_pct, fetch_opinc_yoy(code), credit, vol5, vol25, vratio])
        polite_sleep(0.6)

    with open("metrics.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["code","close","per","pbr","roe_pct","equity_ratio_pct","dividend_yield_pct","op_income_yoy_pct","credit_ratio","vol5","vol25","volratio_5_25"])
        w.writerows(out)
    print("metrics.csv written", flush=True)

if __name__=="__main__":
    main()
