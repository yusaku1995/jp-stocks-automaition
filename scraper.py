# -*- coding: utf-8 -*-
import os
import io
import csv
import re
import time
import random
import requests

# ====== 設定 ======
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

RETRIES = 6   # リトライ回数

# IRBANK（CSV）/ Stooq（株価）/ IRBANK（信用）
IR_CSV = "https://f.irbank.net/files/{code}/{path}"
STOOQ  = "https://stooq.com/q/d/l/?s={code}.jp&i=d"
MARGIN = "https://irbank.net/{code}/margin"

# Kabutan（HTML）
KABU_OVERVIEW = "https://kabutan.jp/stock/?code={code}"
KABU_FINANCE  = "https://kabutan.jp/stock/finance?code={code}"

# CSV ファイル（IRBANK）
CSV_PL        = "fy-profit-and-loss.csv"       # 損益
CSV_BS        = "fy-balance-sheet.csv"         # 貸借
CSV_DIV       = "fy-stock-dividend.csv"        # 配当（年次）
CSV_QQ_YOY_OP = "qq-yoy-operating-income.csv"  # 四半期 営業益YoY
CSV_PS        = "fy-per-share.csv"             # 1株あたり（EPS/BPS/DPS）

# 行ラベルの候補（表記ゆれを想定して広めに）
EPS_KEYS = [
    "EPS","EPS(円)","EPS（円）","1株当たり利益",
    "1株当たり当期純利益","1株当たり当期純利益(円)","1株当たり当期純利益（円）",
    "1株当たり純利益"
]
BPS_KEYS = [
    "BPS","BPS(円)","BPS（円）",
    "1株当たり純資産","1株当たり純資産(円)","1株当たり純資産（円）","1株純資産"
]
DPS_KEYS = [
    "1株配当","1株配当金","配当金","配当(円)","配当（円）","1株当たり配当金"
]
EQ_KEYS  = ["自己資本","自己資本合計","株主資本","株主資本合計","純資産","純資産合計"]
AS_KEYS  = ["総資産","資産合計","資産総額"]
NI_KEYS  = ["当期純利益","親会社株主に帰属する当期純利益","純利益"]

# ====== ユーティリティ（正規化/パース） ======
def _norm(s: str) -> str:
    """行ラベルの正規化（括弧内削除、空白・記号除去）"""
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r'（.*?）', '', s)   # 全角括弧
    s = re.sub(r'\(.*?\)',  '', s)  # 半角括弧
    s = re.sub(r'[\s　,/％%円¥\-–—]', '', s)  # 空白・記号
    return s

def row_index_by_keys(rows, keys):
    """1列目(行見出し)を正規化して候補キーと部分一致で行番号を返す"""
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
            if nk and (nk in head or head in nk):
                return i
    return None

def last_num_in_row(rows, ridx):
    """指定行の右端から数値を拾う（カンマ/空白/記号除去、空/ダッシュはスキップ）"""
    if ridx is None:
        return ""
    r = rows[ridx]
    for x in reversed(r[1:]):  # 2列目以降が年度データ
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

def polite_sleep(sec):
    time.sleep(sec + random.uniform(0.1, 0.7))

# ====== 取得系 ======
def get_csv(code, path):
    """IRBANKのCSVを取得（CSV以外＝制限ページを弾く、丁寧なバックオフ）"""
    url = IR_CSV.format(code=code, path=path)
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=_headers(), timeout=20)
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
                    polite_sleep(2.5)  # 成功時でも一拍（アクセス礼儀）
                    return rows
                else:
                    print(f"[WARN] {url} -> CSV but too short", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(2 + 2*i)  # リトライごとに待機を増やす
    print(f"[FAIL] {url} retried {RETRIES}x", flush=True)
    return None

def fetch_eps_bps_profit_equity_assets_dps(code):
    """EPS/BPS/DPS/純利益/自己資本/総資産を取得。per-share CSVもフォールバックで使用。"""
    pl = get_csv(code, CSV_PL)    # 損益
    bs = get_csv(code, CSV_BS)    # 貸借
    dv = get_csv(code, CSV_DIV)   # 配当（年次）
    ps = get_csv(code, CSV_PS)    # 1株あたり（EPS/BPS/DPS）

    eps = bps = netinc = equity = assets = dps = ""

    # 損益（EPS / 純利益）
    if pl:
        eps_row = row_index_by_keys(pl, EPS_KEYS)
        ni_row  = row_index_by_keys(pl, NI_KEYS)
        eps     = last_num_in_row(pl, eps_row)
        netinc  = last_num_in_row(pl, ni_row)

    # EPSフォールバック（per-share）
    if (eps == "") and ps:
        eps_row_ps = row_index_by_keys(ps, EPS_KEYS)
        eps        = last_num_in_row(ps, eps_row_ps)

    # 貸借（BPS / 自己資本 / 総資産）
    if bs:
        bps_row = row_index_by_keys(bs, BPS_KEYS)
        eq_row  = row_index_by_keys(bs, EQ_KEYS)
        as_row  = row_index_by_keys(bs, AS_KEYS)
        bps     = last_num_in_row(bs, bps_row)
        equity  = last_num_in_row(bs, eq_row)
        assets  = last_num_in_row(bs, as_row)

    # BPSフォールバック（per-share）
    if (bps == "") and ps:
        bps_row_ps = row_index_by_keys(ps, BPS_KEYS)
        bps        = last_num_in_row(ps, bps_row_ps)

    # 配当（DPS）
    if dv:
        dps_row = row_index_by_keys(dv, DPS_KEYS)
        dps     = last_num_in_row(dv, dps_row)

    # DPSフォールバック（per-share）
    if (dps == "") and ps:
        dps_row_ps = row_index_by_keys(ps, DPS_KEYS)
        dps        = last_num_in_row(ps, dps_row_ps)

    return eps, bps, netinc, equity, assets, dps

def fetch_opinc_yoy(code):
    """四半期 営業利益成長率（YoY％）をCSVから取得。なければ空。"""
    qq = get_csv(code, CSV_QQ_YOY_OP)
    if not qq:
        return ""
    for row in reversed(qq[1:]):  # 2列目がYoY%
        if len(row) <= 1: 
            continue
        s = re.sub(r'[^0-9.\-]', '', (row[1] or ""))
        if s in ("", "-", ".", "-."):
            continue
        return s
    return ""

def stooq_close_vols(code):
    """終値・出来高（5日/25日平均）・比率をStooqから取得。"""
    url = STOOQ.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=15)
            if r.status_code == 200 and "\n" in r.text:
                lines = [x.split(',') for x in r.text.strip().splitlines()]
                if len(lines) >= 2:
                    rows = lines[1:]
                    # 終値（最後の有効値）
                    close = None
                    for x in reversed(rows):
                        if len(x) >= 5:
                            try:
                                close = float(x[4]); break
                            except:
                                pass
                    # 出来高配列
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
                    print(f"[WARN] {url} no rows", flush=True)
            else:
                print(f"[WARN] {url} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] {url} -> {e}", flush=True)
        polite_sleep(1 + i)
    print(f"[FAIL] {url}", flush=True)
    return None, "", "", ""

# ====== Kabutan フォールバック ======
# 概要ページ（PER/PBR/ROE/配当利回り/信用倍率などが載る）
RE_PER  = re.compile(r"PER[^0-9]*(\d+(?:\.\d+)?)\s*倍")
RE_PBR  = re.compile(r"PBR[^0-9]*(\d+(?:\.\d+)?)\s*倍")
RE_ROE  = re.compile(r"ROE[^0-9]*(\d+(?:\.\d+)?)\s*%")
RE_DIVY = re.compile(r"配当利回り[^0-9]*(\d+(?:\.\d+)?)\s*%")
RE_CRED = re.compile(r"信用倍率[^0-9]*(\d+(?:\.\d+)?)\s*倍")
# 財務ページ（自己資本比率）
RE_EQR  = re.compile(r"自己資本比率[^0-9]*(\d+(?:\.\d+)?)\s*%")

def fetch_kabutan_overview(code):
    url = KABU_OVERVIEW.format(code=code)
    for i in range(3):
        try:
            r = requests.get(url, headers=_headers(), timeout=25)
            if r.status_code == 200 and r.text:
                t = r.text
                out = {}
                m = RE_PER.search(t);  out["per"]  = m.group(1) if m else ""
                m = RE_PBR.search(t);  out["pbr"]  = m.group(1) if m else ""
                m = RE_ROE.search(t);  out["roe_pct"] = m.group(1) if m else ""
                m = RE_DIVY.search(t); out["divy_pct"] = m.group(1) if m else ""
                m = RE_CRED.search(t); out["credit_ratio"] = m.group(1) if m else ""
                if any(out.values()):
                    print(f"[OK] kabutan overview {code} -> {out}", flush=True)
                    polite_sleep(1.5)
                    return out
                else:
                    print(f"[WARN] kabutan overview {code} no matches", flush=True)
            else:
                print(f"[WARN] kabutan overview {code} HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[ERR] kabutan overview {code} -> {e}", flush=True)
        polite_sleep(2 + i)
    return {"per":"", "pbr":"", "roe_pct":"", "divy_pct":"", "credit_ratio":""}

def fetch_kabutan_finance(code):
    url = KABU_FIN
