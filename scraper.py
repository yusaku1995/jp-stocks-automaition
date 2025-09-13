import re, csv, time, math
import requests
from lxml import html

HEADERS = {"User-Agent": "Mozilla/5.0"}
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
MARGIN = "https://irbank.net/{code}/margin"

# CSV paths on IRBANK
CSV_PL = "fy-profit-and-loss.csv"      # includes EPS, 当期純利益, etc.
CSV_BS = "fy-balance-sheet.csv"        # includes BPS, 自己資本, 総資産, etc.
CSV_DIV= "fy-stock-dividend.csv"       # includes 1株配当
CSV_QQ_YOY_OP = "qq-yoy-operating-income.csv"

def get_csv(code, path):
    url = IR_CSV.format(code=code, path=path)
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200 and len(r.text.strip().splitlines()) >= 2:
                return [row.split(',') for row in r.text.strip().splitlines()]
        except Exception:
            pass
        time.sleep(1 + i)
    return None

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
    pl = get_csv(code, CSV_PL)
    bs = get_csv(code, CSV_BS)
    dv = get_csv(code, CSV_DIV)
    eps = bps = netinc = equity = assets = dps = ""

    if pl:
        h = pl[0]
        eps_idx = col_index(h, ["EPS","1株当たり利益"])
        ni_idx  = col_index(h, ["当期純利益","純利益"])
        eps = last_num(pl, eps_idx)
        netinc = last_num(pl, ni_idx)

    if bs:
        h = bs[0]
        bps_idx = col_index(h, ["BPS","1株当たり純資産"])
        eq_idx  = col_index(h, ["自己資本","純資産"])
        as_idx  = col_index(h, ["総資産"])
        bps = last_num(bs, bps_idx)
        equity = last_num(bs, eq_idx)
        assets = last_num(bs, as_idx)

    if dv:
        h = dv[0]
        dps_idx = col_index(h, ["1株配当","配当金"])
        dps = last_num(dv, dps_idx)

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
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                lines = [x.split(',') for x in r.text.strip().splitlines()]
                if len(lines) >= 2:
                    rows = lines[1:]
                    vols = [int(x[5]) for x in rows if len(x) >= 6 and x[5].isdigit()]
                    close = None
                    for x in reversed(rows):
                        if len(x) >= 6:
                            try:
                                close = float(x[4])
                                break
                            except:
                                pass
                    vol5 = vol25 = vratio = ""
                    if len(vols) >= 25:
                        vol5  = int(sum(vols[-5:]) / 5)
                        vol25 = int(sum(vols[-25:]) / 25)
                        vratio = (vol5/vol25) if vol25 else ""
                    return close, vol5, vol25, vratio
        except Exception:
            pass
        time.sleep(1 + i)
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
    out = []
    for i, code in enumerate(codes, 1):
        print(f"[{i}/{len(codes)}] {code}")
        close, vol5, vol25, vratio = stooq_close_vols(code)
        eps, bps, netinc, equity, assets, dps = fetch_eps_bps_profit_equity_assets_dps(code)
        per = safe_div(close, eps) if (close is not None) else ""
        pbr = safe_div(close, bps) if (close is not None) else ""
        roe = safe_div(netinc, equity)
        roe_pct = to_pct(roe) if roe != "" else ""
        eq_ratio = safe_div(equity, assets)
        eq_ratio_pct = to_pct(eq_ratio) if eq_ratio != "" else ""
        dy = safe_div(dps, close) if (dps and close not in (None,"")) else ""
        dy_pct = to_pct(dy) if dy != "" else ""
        op_yoy = fetch_opinc_yoy(code)
        credit = fetch_credit_ratio(code)
        out.append([code, close if close is not None else "", per, pbr, roe_pct, eq_ratio_pct,
                    dy_pct, op_yoy, credit, vol5, vol25, vratio])
        time.sleep(1)  # politeness
    with open("metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code","close","per","pbr","roe_pct","equity_ratio_pct",
                    "dividend_yield_pct","op_income_yoy_pct","credit_ratio",
                    "vol5","vol25","volratio_5_25"])
        w.writerows(out)

if __name__ == "__main__":
    main()
