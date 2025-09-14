import os, io, csv, re, time, requests
import requests

import re

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    # 括弧内を削除（全角/半角）
    s = re.sub(r'（.*?）', '', s)
    s = re.sub(r'\(.*?\)',  '', s)
    # 記号・空白を削除
    s = re.sub(r'[\s　,/％%円¥\-–—]', '', s)
    return s

def row_index_by_keys(rows, keys):
    """1列目(行見出し)を正規化して keys（正規化済み）と部分一致で探す"""
    if not rows:
        return None
    norm_keys = [_norm(k) for k in keys]
    for i, r in enumerate(rows):
        if not r:
            continue
        head = _norm(r[0])
        if not head:
            continue
        for nk in norm_keys:
            # どちらかに含まれていればOK（“1株当たり当期純利益(円)” などを吸収）
            if nk and (nk in head or head in nk):
                return i
    return None

def last_num_in_row(rows, ridx):
    """その行の右端から数値を拾う（カンマ/空白/記号除去）"""
    if ridx is None:
        return ""
    r = rows[ridx]
    # 2列目以降が年度データ
    for x in reversed(r[1:]):
        if x is None:
            continue
        s = str(x).replace(',', '').strip()
        if s in ("", "-", "—", "–"):
            continue
        try:
            return float(s)
        except:
            continue
    return ""


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv, text/plain;q=0.9, */*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
RETRIES = 6  # リトライ回数を増やす

IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
MARGIN = "https://irbank.net/{code}/margin"

CSV_PL = "fy-profit-and-loss.csv"
CSV_BS = "fy-balance-sheet.csv"
CSV_DIV= "fy-stock-dividend.csv"
CSV_QQ_YOY_OP = "qq-yoy-operating-income.csv"

# 既存の CSV_* 定義の下に追記
CSV_PS = "fy-per-share.csv"   # ← 1株あたり（EPS/BPS/DPS）が載る

# 既存のキー配列を少し拡張（置き換えでもOK）
EPS_KEYS = ["EPS","EPS(円)","EPS（円）","1株当たり利益","1株当たり当期純利益","1株当たり当期純利益(円)","1株当たり当期純利益（円）","1株当たり純利益"]
BPS_KEYS = ["BPS","BPS(円)","BPS（円）","1株当たり純資産","1株当たり純資産(円)","1株当たり純資産（円）","1株純資産"]
DPS_KEYS = ["1株配当","1株配当金","配当金","配当(円)","配当（円）","1株当たり配当金"]

# 純資産（自己資本）/総資産のラベルも広げる
EQ_KEYS  = ["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計"]
AS_KEYS  = ["総資産","資産合計","資産総額"]
NI_KEYS  = ["当期純利益","親会社株主に帰属する当期純利益","純利益"]


def get_csv(code, path):
    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            ctype = r.headers.get("Content-Type","")
            if not r.ok:
                print(f"[WARN] {url} -> HTTP {r.status_code}", flush=True)
            elif "text/csv" not in ctype and "application/octet-stream" not in ctype:
                preview = (r.text or "")[:200].replace("\n"," ")
                print(f"[WARN] {url} -> non-CSV ({ctype}). head='{preview}'", flush=True)
            else:
                rows = list(csv.reader(io.StringIO(r.text)))
                if len(rows) >= 2:
                    print(f"[OK] {url} rows={len(rows)}", flush=True)
                    time.sleep(3)  # ★ 取得成功でも一呼吸（礼儀）
                    return rows
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        time.sleep(3 + 2*i)  # ★ リトライごとに待ち時間を延ばす
    print(f"[FAIL] {url} retried {RETRIES}x", flush=True)
    return None


def row_index_by_keys(rows, keys):
    # rows: CSV全体（list of lists）
    # 1列目(=行見出し)に keys のいずれかが含まれる行番号を返す
    if not rows: 
        return None
    for i, r in enumerate(rows):
        if not r: 
            continue
        head = (r[0] or "").strip()
        for k in keys:
            if k in head:
                return i
    return None

def last_num_in_row(rows, ridx):
    # ridx 行の右側から最後に出現する数値を返す（カンマ除去・空白除去）
    if ridx is None:
        return ""
    r = rows[ridx]
    # 2列目以降が年度ごとの値なので右から探す
    for x in reversed(r[1:]):
        s = (x or "").replace(",", "").strip()
        try:
            if s == "" or s in ["-", "—", "–"]:
                continue
            return float(s)
        except:
            continue
    return ""


def col_index(header_row, keys):
    # find first header that matches any of keys (exact or contains)
    for k in keys:
        for i, h in enumerate(header_row):
            if h.strip() == k or k in h:
                return i
    return None

def last_num(rows, idx):
    # take last non-empty numeric from column idx (skip header)
    num = ""
    for row in rows[1:][::-1]:
        if idx is not None and idx < len(row):
            s = re.sub(r'[^0-9.\-]', '', row[idx])
            if s not in ("", "-", ".", "-."):
                num = s
                break
    return num

def fetch_eps_bps_profit_equity_assets_dps(code):
    pl = get_csv(code, CSV_PL)    # 損益
    bs = get_csv(code, CSV_BS)    # 貸借
    dv = get_csv(code, CSV_DIV)   # 配当（年次）
    ps = get_csv(code, CSV_PS)    # ★ 1株あたり（EPS/BPS/DPS）

    eps = bps = netinc = equity = assets = dps = ""

    # ---- 損益（EPS / 純利益） ----
    if pl:
        eps_row = row_index_by_keys(pl, EPS_KEYS)
        ni_row  = row_index_by_keys(pl, NI_KEYS)
        eps     = last_num_in_row(pl, eps_row)
        netinc  = last_num_in_row(pl, ni_row)

    # ★ EPSが空なら per-share から拾う
    if (eps == "") and ps:
        eps_row_ps = row_index_by_keys(ps, EPS_KEYS)
        eps        = last_num_in_row(ps, eps_row_ps)

    # ---- 貸借（BPS / 自己資本 / 総資産）----
    if bs:
        bps_row = row_index_by_keys(bs, BPS_KEYS)
        eq_row  = row_index_by_keys(bs, EQ_KEYS)
        as_row  = row_index_by_keys(bs, AS_KEYS)
        bps     = last_num_in_row(bs, bps_row)
        equity  = last_num_in_row(bs, eq_row)
        assets  = last_num_in_row(bs, as_row)

    # ★ BPSが空なら per-share から拾う
    if (bps == "") and ps:
        bps_row_ps = row_index_by_keys(ps, BPS_KEYS)
        bps        = last_num_in_row(ps, bps_row_ps)

    # ---- 配当（DPS）----
    if dv:
        dps_row = row_index_by_keys(dv, DPS_KEYS)
        dps     = last_num_in_row(dv, dps_row)

    # ★ DPSが空なら per-share から拾う
    if (dps == "") and ps:
        dps_row_ps = row_index_by_keys(ps, DPS_KEYS)
        dps        = last_num_in_row(ps, dps_row_ps)

    return eps, bps, netinc, equity, assets, dps




def fetch_opinc_yoy(code):
    qq = get_csv(code, CSV_QQ_YOY_OP)
    if not qq: return ""
    col = [row[1] for row in qq[1:] if len(row) > 1]  # Col2
    for v in reversed(col):
        s = re.sub(r'[^0-9.\-]', '', v or "")
        if s not in ("", "-", ".", "-."):
            return s
    return ""

def stooq_close_vols(code):
    url = STOOQ.format(code=code)
    for i in range(3):
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
                            try:
                                close = float(x[4]); break
                            except: pass
                    print(f"[OK] {url} days={len(rows)} close={close}", flush=True)
                    vol5 = vol25 = vratio = ""
                    if len(vols) >= 25:
                        vol5  = int(sum(vols[-5:]) / 5)
                        vol25 = int(sum(vols[-25:]) / 25)
                        vratio = (vol5/vol25) if vol25 else ""
                    return close, vol5, vol25, vratio
                else:
                    print(f"[WARN] {url} no rows", flush=True)
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        time.sleep(1 + i)
    print(f"[FAIL] {url}", flush=True)
    return None, "", "", ""

def fetch_credit_ratio(code):
    # HTML parse (fallback). Leave blank if missing.
    url = MARGIN.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                text = r.text
                m = re.search(r"信用倍率[^0-9]*(\d+(?:\.\d+)?)倍", text)
                if m:
                    return m.group(1)
        except Exception:
            pass
        time.sleep(1 + i)
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

def main():
    with open("tickers.txt", "r", encoding="utf-8") as f:
        codes = [line.strip() for line in f if line.strip()]

    offset = int(os.getenv("OFFSET", "0"))
    limit  = int(os.getenv("MAX_TICKERS", "0"))
    if limit > 0:
        codes = codes[offset:offset+limit]

    total = len(codes)
    print(f"Total tickers to process in this shard: {total}", flush=True)

    out = []
    for i, code in enumerate(codes, 1):
        print(f"[{i}/{total}] {code} start", flush=True)
        close, vol5, vol25, vratio = stooq_close_vols(code)

        eps = bps = netinc = equity = assets = dps = ""
        pl = get_csv(code, CSV_PL)
        bs = get_csv(code, CSV_BS)
        dv = get_csv(code, CSV_DIV)
        if pl:
            h = pl[0]
            eps_idx = col_index(h, EPS_KEYS)
            ni_idx  = col_index(h, NI_KEYS)
            eps = last_num(pl, eps_idx)
            netinc = last_num(pl, ni_idx)
        if bs:
            h = bs[0]
            bps_idx = col_index(h, BPS_KEYS)
            eq_idx  = col_index(h, EQ_KEYS)
            as_idx  = col_index(h, AS_KEYS)
            bps = last_num(bs, bps_idx)
            equity = last_num(bs, eq_idx)
            assets = last_num(bs, as_idx)
        if dv:
            h = dv[0]
            dps_idx = col_index(h, DPS_KEYS)
            dps = last_num(dv, dps_idx)

        # 1件目だけDEBUG詳細
if i == 1:
    print(f"[DEBUG] {code} eps={eps} bps={bps} ni={netinc} eq={equity} as={assets} dps={dps}", flush=True)


        per = safe_div(close, eps) if (close is not None) else ""
        pbr = safe_div(close, bps) if (close is not None) else ""
        roe = safe_div(netinc, equity)
        roe_pct = to_pct(roe) if roe != "" else ""
        eq_ratio = safe_div(equity, assets)
        eq_ratio_pct = to_pct(eq_ratio) if eq_ratio != "" else ""
        dy = safe_div(dps, close) if (dps and close not in (None,"")) else ""
        dy_pct = to_pct(dy) if dy != "" else ""

        op_yoy = fetch_opinc_yoy(code)  # ここも get_csv() を通るのでOK/FAILが出ます
        credit = fetch_credit_ratio(code)  # これはHTML→正規表現

        out.append([code, close if close is not None else "", per, pbr, roe_pct, eq_ratio_pct,
                    dy_pct, op_yoy, credit, vol5, vol25, vratio])
        time.sleep(0.4)

    with open("metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code","close","per","pbr","roe_pct","equity_ratio_pct",
                    "dividend_yield_pct","op_income_yoy_pct","credit_ratio",
                    "vol5","vol25","volratio_5_25"])
        w.writerows(out)
    print("metrics.csv written", flush=True)

if __name__ == "__main__":
    main()
