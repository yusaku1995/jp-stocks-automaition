# -*- coding: utf-8 -*-
# jp-stocks-automation scraper (Kabutan/IRBANK + XPath, supports codes like 215A)
import os, io, csv, re, time, random
import requests
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

# 取得元
IR_CSV = "https://f.irbank.net/files/{code}/{path}"            # （数字4桁のみ）
IR_HTML= "https://irbank.net/{code}"                            # （英字付きもページあり）
STOOQ  = "https://stooq.com/q/d/l/?s={code}.jp&i=d"            # （数字4桁のみ）
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"        # （数字/英字OK）
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}" # （数字/英字OK）
KABU_KABUKA   = "https://kabutan.jp/stock/kabuka?code={code}&ashi=day&page=1"  # 日足 過去株価

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

def first_four_digits(code: str) -> str:
    """コード内の先頭4桁数字を返す（なければ空）。IRCSV/Stooq用"""
    m = re.search(r"(\d{4})", code or "")
    return m.group(1) if m else ""

# ========= コード整形 =========
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

# ========= IRBANK CSV 取得 =========
def get_csv(code, path):
    # IRBANK CSVは4桁数字のみ。英字付きは内部で4桁抽出して使う。
    num = first_four_digits(code)
    if not num:
        print(f"[SKIP] IRBANK CSV cannot derive 4-digit from: {code}", flush=True)
        return None
    url = IR_CSV.format(code=num, path=path)
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
    num = first_four_digits(code)
    if not num:
        print(f"[SKIP] Stooq cannot derive 4-digit from: {code}", flush=True)
        return None, "", "", ""
    url = STOOQ.format(code=num)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=15)
            if r.status_code == 200 and "\n" in r.text:
                rows = [x.split(',') for x in r.text.strip().splitlines()][1:]
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

# ========= Kabutan（過去株価で出来高フォールバック）=========
_num_re = re.compile(r"(-?\d+(?:\.\d+)?)")
_int_re = re.compile(r"(\d[\d,]*)")

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
        polite_sleep(1.2 + i*0.8)
    return ""

def _num_only(s):
    if not s:
        return ""
    m = _num_re.search(s)
    return m.group(1) if m else ""

def _int_only(s):
    if not s:
        return ""
    m = _int_re.search(s)
    return m.group(1).replace(",", "") if m else ""

def kabu_per(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//table//tr[th[normalize-space()='PER']]/td[1]",
        "//table//tr[th[contains(normalize-space(),'PER') and not(contains(normalize-space(),'PBR'))]]/td[1]",
        "//th[normalize-space()='PER']/following-sibling::td[1]",
        "//th[contains(normalize-space(),'PER')]/following-sibling::td[1]",
    ]
    for xp in xps:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_only(t)
        if v != "":
            return v
    return ""

def kabu_pbr(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//table//tr[th[normalize-space()='PBR']]/td[1]",
        "//table//tr[th[contains(normalize-space(),'PBR') and not(contains(normalize-space(),'PER'))]]/td[1]",
        "//th[normalize-space()='PBR']/following-sibling::td[1]",
        "//th[contains(normalize-space(),'PBR')]/following-sibling::td[1]",
    ]
    for xp in xps:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_only(t)
        if v != "":
            return v
    # IRBANK保険
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'PBR')])[1]/following::text()[1]")
    return _num_only(t)

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
    # 財務ページ側
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
    # IRBANK フォールバック
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'ROE')])[1]/following::text()[1]")
    return _num_only(t)

def kabu_divy_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    candidates = [
        "//th[.//text()[contains(.,'配当利回り')]]/following-sibling::td[1]",
        "//*[self::th or self::*][contains(normalize-space(.),'配当利回り')]/following::*[1]",
        "//*[contains(text(),'配当利回り')][1]/following::text()[1]",
    ]
    for xp in candidates:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_only(t)
        if v != "":
            return v
    # IRBANK フォールバック
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'配当利回り')])[1]/following::text()[1]")
    return _num_only(t)

def kabu_credit(code):
    # まずKabutan
    t = _get_first_text_by_xpath(
            KABU_OVERVIEW.format(code=code),
            "//*[contains(text(),'信用倍率')][1]/following::text()[1]"
        ) or _get_first_text_by_xpath(
            KABU_OVERVIEW.format(code=code),
            "//th[contains(.,'信用倍率')]/following-sibling::td[1]"
        )
    v = _num_only(t)
    if v != "":
        return v
    # IRBANK保険
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'信用倍率')])[1]/following::text()[1]")
    return _num_only(t)

def kabu_equity_ratio_pct(code):
    # 自己資本比率（％）
    url = KABU_FINANCE.format(code=code)
    candidates = [
        "//table//tr[th[contains(normalize-space(),'自己資本比率')]]/td[1]",
        "//th[.//text()[contains(.,'自己資本比率')]]/following-sibling::td[1]",
        "//*[contains(text(),'自己資本比率')]/following::td[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li][1]/*[self::td or self::dd][1]",
    ]
    for xp in candidates:
        t = _get_first_text_by_xpath(url, xp)
        v = _num_only(t)
        if v != "":
            return v
    # 概要ページ側に表示される場合の保険
    url_o = KABU_OVERVIEW.format(code=code)
    t = _get_first_text_by_xpath(url_o, "//*[contains(text(),'自己資本比率')][1]/following::text()[1]")
    v = _num_only(t)
    if v != "":
        return v
    # IRBANK フォールバック
    t = _get_first_text_by_xpath(IR_HTML.format(code=code), "(//*[contains(text(),'自己資本比率')])[1]/following::text()[1]")
    return _num_only(t)

def kabu_vols(code):
    """
    Kabutan 過去株価（page=1の日足）から出来高を直近から25件まで収集。
    5日/25日平均と比率を返す。失敗時は空。
    """
    url = KABU_KABUKA.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                doc = LH.fromstring(r.text)

                # 1) ヘッダーから「出来高」列のindexを特定
                header_cells = doc.xpath("//table[contains(@class,'stock_kabuka')]//thead//tr[1]/*")
                vol_idx = None
                if header_cells:
                    for idx, h in enumerate(header_cells, start=1):
                        txt = (h.text_content() or "").strip()
                        if "出来高" in txt:
                            vol_idx = idx
                            break
                # 2) ボディから vol列の値を最大25件取得
                vols = []
                rows = doc.xpath("//table[contains(@class,'stock_kabuka')]//tbody//tr[td]")
                for tr in rows:
                    if vol_idx is not None:
                        tds = tr.xpath("./td")
                        if len(tds) >= vol_idx:
                            raw = tds[vol_idx-1].text_content().strip()
                        else:
                            continue
                    else:
                        # 保険：行テキストから数値が多いセルっぽいものを総当り
                        tds = tr.xpath("./td")
                        raw = ""
                        if tds:
                            # 一番数字が多そうな末尾セルを候補に
                            raw = tds[-2 if len(tds)>=2 else -1].text_content().strip()
                    iv = _int_only(raw)
                    if iv != "":
                        try:
                            vols.append(int(iv))
                        except:
                            pass
                    if len(vols) >= 25:
                        break
                if vols:
                    vol5 = vol25 = vratio = ""
                    if len(vols) >= 5:
                        vol5 = int(sum(vols[:5]) / 5)
                    if len(vols) >= 25:
                        vol25 = int(sum(vols[:25]) / 25)
                    if vol5 != "" and vol25 not in ("", 0):
                        try:
                            vratio = float(vol5) / float(vol25)
                        except:
                            vratio = ""
                    return vol5, vol25, vratio
                else:
                    print(f"[WARN] Kabutan vols not found rows: {url}", flush=True)
            else:
                print(f"[WARN] Kabutan kabuka HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] Kabutan kabuka {url} -> {e}", flush=True)
        polite_sleep(1.2 + i*0.8)
    return "", "", ""

# ========= メイン =========
def main():
    with open("tickers.txt", "r", encoding="utf-8") as f:
        raw = [line for line in f if line.strip()]
    codes = [normalize_code_line(x) for x in raw]
    codes = [re.sub(r"[^\w]", "", c) for c in codes]
    codes = list(dict.fromkeys(codes))  # 重複除去

    # シャーディング
    offset = int(os.getenv("OFFSET", "0"))
    limit  = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        codes = codes[offset:offset+limit]

    print(f"Total tickers to process in this shard: {len(codes)}", flush=True)

    out = []
    for i, code in enumerate(codes, 1):
        print(f"[{i}/{len(codes)}] {code} start", flush=True)

        # 価格・出来高（まずStooq。ダメならKabutan過去株価）
        close, vol5, vol25, vratio = stooq_close_vols(code)
        if vol5 == "" or vol25 == "" or vratio == "":
            kv5, kv25, kvr = kabu_vols(code)
            if vol5 == "": vol5 = kv5
            if vol25 == "": vol25 = kv25
            if vratio == "": vratio = kvr

        # IRBANK CSV（内部的に4桁数字抽出して使用）
        eps, bps, ni, eq, assets, dps = fetch_eps_bps_profit_equity_assets_dps(code)

        # まず CSV で算出を試みる（per/pbr/roe/eq比率のみ）
        per     = safe_div(close, eps) if (close is not None and eps != "") else ""
        pbr     = safe_div(close, bps) if (close is not None and bps != "") else ""
        roe     = safe_div(ni, eq) if (ni != "" and eq != "") else ""
        roe_pct = to_pct(roe) if roe != "" else ""
        eqr     = safe_div(eq, assets) if (eq != "" and assets != "") else ""
        eqr_pct = to_pct(eqr) if eqr != "" else ""

        # ===== HTML フォールバック =====
        if per == "":       per = kabu_per(code) or ""
        if pbr == "":       pbr = kabu_pbr(code) or ""
        # --- PERとPBRが同値ならPBR取り違え疑い：もう一度厳格XPathで取り直し
        try:
            if per != "" and pbr != "" and abs(float(per) - float(pbr)) < 1e-9:
                p2 = kabu_pbr(code)
                if p2 != "" and p2 != per:
                    pbr = p2
        except:
            pass

        if roe_pct == "":   roe_pct = kabu_roe_pct(code) or ""
        if eqr_pct == "":   eqr_pct = kabu_equity_ratio_pct(code) or ""

        # 配当利回りはKabutan優先（計算はしない：異常値防止）
        dy_pct = kabu_divy_pct(code) or ""

        # 信用倍率（Kabutan→IRBANK保険）
        credit = kabu_credit(code) or ""

        # 営業利益YoY（IR CSVのみ。英字付きは空のことが多い）
        op_yoy = fetch_opinc_yoy(code)

        if i == 1:
            print(f"[DEBUG] {code} eps={eps} bps={bps} ni={ni} eq={eq} as={assets} dps={dps}", flush=True)
            print(f"[DEBUG] per={per} pbr={pbr} roe%={roe_pct} eqr%={eqr_pct} divy%={dy_pct} credit={credit}", flush=True)
            print(f"[DEBUG] vol5={vol5} vol25={vol25} vratio={vratio}", flush=True)

        out.append([
            code,
            (close if close is not None else ""),   # closeはシート側管理でも一応出力は保持
            per, pbr, roe_pct, eqr_pct, dy_pct,
            op_yoy, credit, vol5, vol25, vratio
        ])
        polite_sleep(0.6)

    with open("metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "code","close","per","pbr","roe_pct","equity_ratio_pct",
            "dividend_yield_pct","op_income_yoy_pct","credit_ratio",
            "vol5","vol25","volratio_5_25"
        ])
        w.writerows(out)
    print("metrics.csv written", flush=True)

if __name__ == "__main__":
    main()
