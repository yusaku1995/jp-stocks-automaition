# JP Stocks Weekly Metrics Automation (No API keys)
This pack fetches metrics for JP tickers (e.g., 215A) weekly and publishes a CSV.
It uses **IRBANK official CSVs** + **Stooq daily OHLCV** and computes metrics.

## What it collects
- PER = Close / EPS  (EPS from IRBANK, Close from Stooq)
- PBR = Close / BPS  (BPS from IRBANK)
- ROE % = NetIncome / Equity * 100  (from IRBANK)
- Equity Ratio % = Equity / TotalAssets * 100  (from IRBANK)
- Dividend Yield % = DPS / Close * 100  (DPS from IRBANK)
- Op. Income YoY % (latest)  (IRBANK quarterly YoY CSV)
- Credit Ratio (倍率)  (IRBANK /margin HTML parsed; left blank if missing)
- Vol5 / Vol25 / VolRatio (Stooq daily Volume)

## Files
- `tickers.txt` – put one ticker per line (e.g. 215A, 6920). Example includes 215A.
- `scraper.py` – main script
- `requirements.txt` – Python deps
- `.github/workflows/scrape.yml` – schedule (weekdays 18:15 JST)

## Quick start
1) Create a **new GitHub repo**, upload all files in this folder.
2) Edit `tickers.txt` with your 126 tickers.
3) Enable GitHub Pages (Settings → Pages → Build from `gh-pages`).
4) Actions tab → enable workflows → the job will run on schedule or via **Run workflow**.
5) The output `metrics.csv` is published to gh-pages.
6) In Google Sheets, use:  
   `=IMPORTDATA("https://<yourname>.github.io/<repo>/metrics.csv")`

## Local run (optional)
```bash
pip install -r requirements.txt
python scraper.py
```

## Notes
- Be respectful: the script has sleep + retries.
- If any field is missing, it is left blank. CSV always includes headers.
- GitHub Actions cron is set to 09:15 UTC (18:15 JST), weekdays. Adjust as needed.
