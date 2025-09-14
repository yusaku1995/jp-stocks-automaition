# -*- coding: utf-8 -*-
# jp-stocks-automation scraper
# 取得するもの：
#  - PER / PBR / ROE% / 自己資本比率% / 配当利回り% / 営業利益YoY% / 信用倍率
#  - 出来高(5日平均, 25日平均, 比率)
# 価格(終値)は出力しません。ティッカーは英字付き(例: 215A)もそのまま扱います。

import os, io, csv, re, time, random, requests, math
from datetime import datetime, timedelta, timezone
from lxml import html as LH  # XPath

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
SLEEP_BASE = 0.6

# 取得元（コードは“そのまま”使う：英字付きも可）
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
IR_HTML= "https://irbank.net/{code}"
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"
STOOQ  = "https://stooq.com/q/d/l/?s={code}.jp&i=d"  # 失敗したらYahooにフォールバック
YF_CSV = "https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true"

# IRBANK CSV（営業利益YoYのみ使用）
CSV_QQ="qq-yoy-operating-income.csv"

# ========= 小物 =========
def polite_sleep(sec: float) -> None:
    time.sleep(sec + random.uniform(0.1, 0.5))

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
        polite_sleep(1.0 + i*0.7)
    return ""

def _num_only(s):
    if not s:
        return ""
    m = _num_re.search(s)
    return m.group(1) if m else ""

def normalize_code_line(line: str) -> str:
    token = re.split(r"[\s,\t]+", line.strip())[0] if line else ""
    try:
        import unicodedata
        token = unicodedata.normalize("NFKC", token)
    except:
        pass
    return token.strip().upper()

# ========= IRBANK CSV（営業利益YoY）=========
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
                    polite_sleep(0.8)
                    return rows
                else:
                    print(f"[WARN] {url} CSV too short", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1.0 + 0.8*i)
    print(f"[FAIL] {url} retried {RETRIES}x", flush=True)
    return None

def fetch_opinc_yoy(code):
    qq = get_csv(code, CSV_QQ)
    if not qq:
        return ""
    for row in reversed(qq[1:]):
        if len(row) <= 1:
            continue
        s = re.sub(r'[^0-9.\-]', '', (row[1] or ""))
        if s in ("", "-", ".", "-."):
            continue
        return s
    return ""

# ========= Kabutan/IRBANK（HTML, XPath）=========
def kabu_per(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//th[contains(.,'PER')]/following-sibling::td[1]",
        "//*[contains(normalize-space(.),'PER')]/following::*[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    return ""

def kabu_pbr(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//th[contains(.,'PBR')]/following-sibling::td[1]",
        "//*[contains(normalize-space(.),'PBR')]/following::*[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    # IRBANK保険
    return _num_only(_get_first_text_by_xpath(
        IR_HTML.format(code=code), "(//*[contains(text(),'PBR')])[1]/following::text()[1]"
    ))

def kabu_roe_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//th[.//text()[contains(.,'ROE')]]/following-sibling::td[1]",
        "//*[self::th or self::*][contains(normalize-space(.),'ROE')]/following::*[1]",
        "//*[contains(text(),'ROE')][1]/following::text()[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    url_f = KABU_FINANCE.format(code=code)
    xps_f = [
        "//th[.//text()[contains(.,'ROE')]]/following-sibling::td[1]",
        "//*[contains(text(),'ROE')]/following::td[1]",
    ]
    for xp in xps_f:
        v = _num_only(_get_first_text_by_xpath(url_f, xp))
        if v != "": return v
    return _num_only(_get_first_text_by_xpath(
        IR_HTML.format(code=code), "(//*[contains(text(),'ROE')])[1]/following::text()[1]"
    ))

def kabu_divy_pct(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//th[.//text()[contains(.,'配当利回り')]]/following-sibling::td[1]",
        "//*[self::th or self::*][contains(normalize-space(.),'配当利回り')]/following::*[1]",
        "//*[contains(text(),'配当利回り')][1]/following::text()[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    return _num_only(_get_first_text_by_xpath(
        IR_HTML.format(code=code), "(//*[contains(text(),'配当利回り')])[1]/following::text()[1]"
    ))

def kabu_credit(code):
    url = KABU_OVERVIEW.format(code=code)
    xps = [
        "//*[contains(text(),'信用倍率')][1]/following::text()[1]",
        "//th[contains(.,'信用倍率')]/following-sibling::td[1]",
        "//dt[contains(.,'信用倍率')]/following-sibling::dd[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    return _num_only(_get_first_text_by_xpath(
        IR_HTML.format(code=code), "(//*[contains(text(),'信用倍率')])[1]/following::text()[1]"
    ))

def kabu_equity_ratio_pct(code):
    url = KABU_FINANCE.format(code=code)
    xps = [
        "//th[.//text()[contains(.,'自己資本比率')]]/following-sibling::td[1]",
        "//*[contains(text(),'自己資本比率')]/following::td[1]",
        "//*[contains(text(),'自己資本比率')]/ancestor::*[self::tr or self::li][1]/*[self::td or self::dd][1]",
        "//dt[contains(.,'自己資本比率')]/following-sibling::dd[1]",
    ]
    for xp in xps:
        v = _num_only(_get_first_text_by_xpath(url, xp))
        if v != "": return v
    # 概要ページ側の保険
    url_o = KABU_OVERVIEW.format(code=code)
    v = _num_only(_get_first_text_by_xpath(url_o, "//*[contains(text(),'自己資本比率')][1]/following::text()[1]"))
    if v != "": return v
    # IRBANK保険
    return _num_only(_get_first_text_by_xpath(
        IR_HTML.format(code=code), "(//*[contains(text(),'自己資本比率')])[1]/following::text()[1]"
    ))

# ========= 出来高（Stooq -> Yahoo CSV フォールバック）=========
def _vol_5_25(vols):
    vols = [int(v) for v in vols if isinstance(v, (int, float, str)) and str(v).isdigit()]
    if len(vols) < 5:
        return "", "", ""
    v5 = int(sum(vols[-5:]) / 5)
    v25 = int(sum(vols[-25:]) / 25) if len(vols) >= 25 else ""
    ratio = (v5 / v25) if (isinstance(v25, int) and v25 != 0) else ""
    return v5, v25, ratio

def fetch_volumes_stooq(code):
    url = STOOQ.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=12)
            if r.status_code == 200 and "\n" in r.text:
                rows = [x.split(',') for x in r.text.strip().splitlines()][1:]
                vols = []
                for x in rows:
                    if len(x) >= 6:
                        try:
                            vols.append(int(x[5]))
                        except:
                            pass
                if vols:
                    print(f"[OK] Stooq vols {code}: days={len(vols)}", flush=True)
                    return _vol_5_25(vols)
            else:
                print(f"[WARN] Stooq HTTP {r.status_code}: {url}", flush=True)
        except Exception as e:
            print(f"[ERR] Stooq {code} -> {e}", flush=True)
        polite_sleep(0.8 + i*0.6)
    return "", "", ""

def fetch_volumes_yahoo(code):
    # Yahooは {code}.T を試す（英字付きもそのまま）
    symbol = f"{code}.T"
    # 直近200日分をDL
    tz = timezone.utc
    p2 = int(datetime.now(tz=tz).timestamp())
    p1 = int((datetime.now(tz=tz) - timedelta(days=220)).timestamp())
    url = YF_CSV.format(symbol=symbol, p1=p1, p2=p2)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=20)
            if r.status_code == 200 and "Date,Open,High,Low,Close" in r.text:
                rows = list(csv.reader(io.StringIO(r.text)))
                hdr = rows[0]
                try:
                    idx_vol = hdr.index("Volume")
                except ValueError:
                    print(f"[WARN] Yahoo CSV no Volume col: {symbol}", flush=True)
                    return "", "", ""
                vols = []
                for row in rows[1:]:
                    if len(row) > idx_vol:
                        s = row[idx_vol].strip()
                        if s and s.isdigit():
                            vols.append(int(s))
                if vols:
                    print(f"[OK] Yahoo vols {symbol}: days={len(vols)}", flush=True)
                    return _vol_5_25(vols)
            else:
                print(f"[WARN] Yahoo HTTP {r.status_code}: {symbol}", flush=True)
        except Exception as e:
            print(f"[ERR] Yahoo {symbol} -> {e}", flush=True)
        polite_sleep(0.9 + i*0.6)
    return "", "", ""

def fetch_volumes(code):
    v5, v25, vr = fetch_volumes_stooq(code)
    if v5 == "" or v25 == "":
        v5b, v25b, vrb = fetch_volumes_yahoo(code)
        v5 = v5 if v5 != "" else v5b
        v25 = v25 if v25 != "" else v25b
        vr = vr if vr != "" else vrb
    return v5, v25, vr

# ========= メイン =========
def main():
    with open("tickers.txt", "r", encoding="utf-8") as f:
        raw = [line for line in f if line.strip()]
    codes = [normalize_code_line(x) for x in raw]
    # 記号だけ除去して重複削除（フォーマット制限なし）
    codes = [re.sub(r"[^\w]", "", c) for c in codes]
    filtered = list(dict.fromkeys(codes))

    # シャーディング
    offset = int(os.getenv("OFFSET", "0"))
    limit  = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        filtered = filtered[offset:offset+limit]

    print(f"Total tickers to process in this shard: {len(filtered)}", flush=True)

    out = []
    for i, code in enumerate(filtered, 1):
        print(f"[{i}/{len(filtered)}] {code} start", flush=True)

        # 指標（HTML）
        per   = kabu_per(code) or ""
        pbr   = kabu_pbr(code) or ""
        roe_p = kabu_roe_pct(code) or ""
        eq_p  = kabu_equity_ratio_pct(code) or ""
        dy_p  = kabu_divy_pct(code) or ""
        cr    = kabu_credit(code) or ""
        opy   = fetch_opinc_yoy(code)  # 取れなければ空

        # 出来高
        v5, v25, vr = fetch_volumes(code)

        if i == 1:
            print(f"[DEBUG] {code} per={per} pbr={pbr} roe%={roe_p} eqr%={eq_p} divy%={dy_p} credit={cr} op_yoy={opy} v5={v5} v25={v25} vr={vr}", flush=True)

        out.append([code, per, pbr, roe_p, eq_p, dy_p, opy, cr, v5, v25, vr])
        polite_sleep(SLEEP_BASE)

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
