"""
Sources:
  - Natural Gas   : MIBGAS (mibgas.es)      — Iberian gas market, EUR/MWh, daily
  - Coal API2     : yfinance 'MTF=F'        — API2 Rotterdam futures, USD/t, daily
  - Diesel        : yfinance 'HO=F'         — NYMEX Heating Oil proxy, USD/Gallon, daily
  - EUR/USD FX    : yfinance 'EURUSD=X'     — spot rate for USD -> EUR conversion
  - EU ETS CO2    : yfinance '^ICEEUA'      — ICE EUA Carbon Futures Index, EUR/tCO2, daily
                    (fallback: local Excel Coal_Diesel_ETS_Monthly_Costs.xlsx)
  - Uranium       : yfinance 'UX=F'         — UxC Uranium U3O8 Futures, USD/lb, daily
                    (fallback: local Excel Uranium_Annual_Prices2020-2024.xlsx)
"""

import os
import warnings
import requests
import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')

# Paths
REPO_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_OTHER_DIR = os.path.join(REPO_ROOT, "data", "other")
os.makedirs(DATA_OTHER_DIR, exist_ok=True)

# Config
YEAR_START = 2020
YEAR_END   = 2024
START_DATE = f"{YEAR_START}-01-01"
END_DATE   = f"{YEAR_END}-12-31"
YF_END     = f"{YEAR_END + 1}-01-01"  # yfinance end is exclusive

# Conversion factors
COAL_MWH_PER_TON      = 8.14    # 1 metric ton thermal coal  ~ 8.14 MWh
DIESEL_MWH_PER_GALLON = 0.0406  # 1 US gallon heating oil    ~ 0.0406 MWh
URANIUM_MWH_PER_LB    = 30.211  # U3O8 lb -> MWh fuel equivalent

print("=" * 60)
print("  fetch_cost_data.py - Automated Cost Data Downloader")
print("=" * 60)

# ── Step 1: Perfect daily calendar ────────────────────────────────────────────
print("\n[1] Building daily calendar...")
calendar = pd.DataFrame({
    'date': pd.date_range(start=START_DATE, end=END_DATE, freq='D')
})
calendar['date_str']   = calendar['date'].dt.strftime('%Y-%m-%d')
calendar['year_month'] = calendar['date'].dt.strftime('%Y-%m')
calendar['year']       = calendar['date'].dt.year
print(f"    {len(calendar)} days ({START_DATE} to {END_DATE})")

# ── Step 2: Natural Gas — MIBGAS ──────────────────────────────────────────────
print("\n[2] Downloading Natural Gas from MIBGAS...")
mibgas_dfs = []
for year in range(YEAR_START, YEAR_END + 1):
    url = (f"https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx"
           f"?path=AGNO_{year}/XLS")
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            print(f"    [!] {year}: HTTP {resp.status_code}")
            continue

        xls = pd.ExcelFile(pd.io.common.BytesIO(resp.content))

        # Find Day-Ahead sheet
        sheet = next(
            (s for s in xls.sheet_names if 'D+1' in s or 'DA' in s.upper()),
            xls.sheet_names[0]
        )

        # Detect header row
        df_raw = xls.parse(sheet, header=None)
        header_row = next(
            (i for i, row in df_raw.iterrows()
             if any(k in str(v).lower() for v in row.values
                    for k in ['trading', 'fecha', 'date'])),
            None
        )
        if header_row is None:
            print(f"    [!] {year}: header not found in sheet '{sheet}'")
            continue

        df_year = xls.parse(sheet, header=header_row)
        date_col  = next((c for c in df_year.columns if any(
            k in str(c).lower() for k in ['trading', 'fecha', 'date'])),
            df_year.columns[0])
        price_col = next((c for c in df_year.columns if any(
            k in str(c).lower() for k in ['reference', 'precio', 'price'])),
            df_year.columns[1])

        df_y = df_year[[date_col, price_col]].copy()
        df_y.columns = ['date_str', 'cost_gas_eur_mwh']
        df_y['date_str'] = pd.to_datetime(df_y['date_str'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_y['cost_gas_eur_mwh'] = pd.to_numeric(df_y['cost_gas_eur_mwh'], errors='coerce')
        df_y = df_y.dropna(subset=['date_str', 'cost_gas_eur_mwh'])
        mibgas_dfs.append(df_y)
        print(f"    OK {year}: {len(df_y)} trading days")

    except Exception as e:
        print(f"    [!] {year}: {e}")

if mibgas_dfs:
    df_gas = pd.concat(mibgas_dfs, ignore_index=True).drop_duplicates('date_str')
    calendar = pd.merge(calendar, df_gas, on='date_str', how='left')
    print(f"    -> {calendar['cost_gas_eur_mwh'].notna().sum()} days with gas price")
else:
    print("    [!!!] MIBGAS failed completely. cost_gas_eur_mwh = NaN")
    calendar['cost_gas_eur_mwh'] = np.nan

# ── Step 3: Coal, Diesel, FX — yfinance ───────────────────────────────────────
print("\n[3] Downloading Coal, Diesel, FX from Yahoo Finance...")
try:
    raw_yf = yf.download(
        ['MTF=F', 'HO=F', 'EURUSD=X'],
        start=START_DATE, end=YF_END, progress=False
    )
    if isinstance(raw_yf.columns, pd.MultiIndex):
        raw_yf = raw_yf['Close']
    raw_yf.index = pd.to_datetime(raw_yf.index).strftime('%Y-%m-%d')
    raw_yf = raw_yf.reset_index()
    raw_yf.columns.values[0] = 'date_str'
    calendar = pd.merge(calendar, raw_yf, on='date_str', how='left')
    print(f"    OK Coal  (MTF=F):   {calendar['MTF=F'].notna().sum()} days")
    print(f"    OK Diesel (HO=F):   {calendar['HO=F'].notna().sum()} days")
    print(f"    OK FX (EURUSD=X):   {calendar['EURUSD=X'].notna().sum()} days")
except Exception as e:
    print(f"    [!] yfinance error: {e}")
    for col in ['MTF=F', 'HO=F', 'EURUSD=X']:
        calendar[col] = np.nan

# ── Step 4: EU ETS CO2 — yfinance with local Excel fallback ───────────────────
print("\n[4] Downloading EU ETS CO2 price...")
ets_ok = False
try:
    raw_ets = yf.download('^ICEEUA', start=START_DATE, end=YF_END, progress=False)
    if isinstance(raw_ets.columns, pd.MultiIndex):
        raw_ets = raw_ets['Close']
    else:
        raw_ets = raw_ets[['Close']]
    raw_ets.index = pd.to_datetime(raw_ets.index).strftime('%Y-%m-%d')
    raw_ets = raw_ets.reset_index()
    raw_ets.columns = ['date_str', 'eu_ets_price_eur_tco2']
    raw_ets['eu_ets_price_eur_tco2'] = pd.to_numeric(
        raw_ets['eu_ets_price_eur_tco2'], errors='coerce')
    n_ok = raw_ets['eu_ets_price_eur_tco2'].notna().sum()
    if n_ok > 100:
        calendar = pd.merge(calendar, raw_ets, on='date_str', how='left')
        print(f"    OK ^ICEEUA: {n_ok} trading days (source: ICE via Yahoo Finance)")
        ets_ok = True
    else:
        print(f"    [!] ^ICEEUA: only {n_ok} rows — using local Excel fallback")
except Exception as e:
    print(f"    [!] ^ICEEUA error: {e} — using local Excel fallback")

if not ets_ok:
    for path in [
        os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.xlsx"),
        os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.csv"),
    ]:
        if not os.path.exists(path):
            continue
        try:
            df_cde = pd.read_excel(path) if path.endswith('.xlsx') else pd.read_csv(path)
            col_date = 'Date' if 'Date' in df_cde.columns else df_cde.columns[0]
            df_cde['year_month'] = pd.to_datetime(df_cde[col_date]).dt.strftime('%Y-%m')
            ets_col = next((c for c in df_cde.columns if 'ets' in c.lower()), None)
            if not ets_col:
                raise ValueError("ETS column not found")
            df_ets = df_cde[['year_month', ets_col]].rename(
                columns={ets_col: 'eu_ets_price_eur_tco2'})
            calendar = pd.merge(calendar, df_ets, on='year_month', how='left')
            print(f"    OK Fallback: EU ETS loaded from {os.path.basename(path)}")
            ets_ok = True
            break
        except Exception as e:
            print(f"    [!] Fallback {os.path.basename(path)}: {e}")

if not ets_ok:
    print("    [!!!] EU ETS: all sources failed — filling with NaN")
    calendar['eu_ets_price_eur_tco2'] = np.nan

# ── Step 5: Uranium — yfinance with local Excel fallback ──────────────────────
print("\n[5] Downloading Uranium price...")
uranium_from_yf = False
try:
    raw_ura = yf.download('UX=F', start=START_DATE, end=YF_END, progress=False)
    if isinstance(raw_ura.columns, pd.MultiIndex):
        raw_ura = raw_ura['Close']
    else:
        raw_ura = raw_ura[['Close']]
    raw_ura.index = pd.to_datetime(raw_ura.index).strftime('%Y-%m-%d')
    raw_ura = raw_ura.reset_index()
    raw_ura.columns = ['date_str', 'uranium_usd_lb']
    raw_ura['uranium_usd_lb'] = pd.to_numeric(raw_ura['uranium_usd_lb'], errors='coerce')
    n_ok = raw_ura['uranium_usd_lb'].notna().sum()
    if n_ok > 100:
        calendar = pd.merge(calendar, raw_ura, on='date_str', how='left')
        print(f"    OK UX=F: {n_ok} trading days (source: UxC via Yahoo Finance)")
        uranium_from_yf = True
    else:
        print(f"    [!] UX=F: only {n_ok} rows — using local Excel fallback")
except Exception as e:
    print(f"    [!] UX=F error: {e} — using local Excel fallback")

if not uranium_from_yf:
    for path in [
        os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.xlsx"),
        os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.csv"),
    ]:
        if not os.path.exists(path):
            continue
        try:
            df_ura = pd.read_excel(path) if path.endswith('.xlsx') else pd.read_csv(path)
            col_year = 'year' if 'year' in df_ura.columns else df_ura.columns[0]
            df_ura['year'] = pd.to_datetime(df_ura[col_year].astype(str)).dt.year
            df_ura['cost_uranium_eur_mwh'] = (
                pd.to_numeric(df_ura['cost_uranium_eur_mwh'], errors='coerce')
                / URANIUM_MWH_PER_LB
            )
            calendar = pd.merge(calendar, df_ura[['year', 'cost_uranium_eur_mwh']],
                                 on='year', how='left')
            print(f"    OK Fallback: Uranium loaded from {os.path.basename(path)} (annual -> daily)")
            break
        except Exception as e:
            print(f"    [!] Fallback {os.path.basename(path)}: {e}")

    if 'cost_uranium_eur_mwh' not in calendar.columns:
        print("    [!!!] Uranium: all sources failed — filling with NaN")
        calendar['cost_uranium_eur_mwh'] = np.nan

# ── Step 6: Unit conversions ──────────────────────────────────────────────────
print("\n[6] Applying unit conversions...")
fx = pd.to_numeric(calendar.get('EURUSD=X'), errors='coerce')

# Coal: USD/t -> EUR/MWh
if 'MTF=F' in calendar.columns:
    calendar['cost_coal_eur_mwh'] = (
        pd.to_numeric(calendar['MTF=F'], errors='coerce') / fx / COAL_MWH_PER_TON)
    print(f"    OK Coal:   {calendar['cost_coal_eur_mwh'].notna().sum()} days")
else:
    calendar['cost_coal_eur_mwh'] = np.nan

# Diesel: USD/gal -> EUR/MWh
if 'HO=F' in calendar.columns:
    calendar['cost_diesel_eur_mwh'] = (
        pd.to_numeric(calendar['HO=F'], errors='coerce') / fx / DIESEL_MWH_PER_GALLON)
    print(f"    OK Diesel: {calendar['cost_diesel_eur_mwh'].notna().sum()} days")
else:
    calendar['cost_diesel_eur_mwh'] = np.nan

# Uranium: USD/lb -> EUR/MWh (only if from yfinance; Excel fallback already in EUR/MWh)
if uranium_from_yf and 'uranium_usd_lb' in calendar.columns:
    calendar['cost_uranium_eur_mwh'] = (
        pd.to_numeric(calendar['uranium_usd_lb'], errors='coerce') / fx / URANIUM_MWH_PER_LB)
    print(f"    OK Uranium:{calendar['cost_uranium_eur_mwh'].notna().sum()} days")

# ── Step 7: Fill weekends and market holidays ─────────────────────────────────
print("\n[7] Filling weekends/holidays (ffill then bfill)...")
cost_cols = [
    'cost_gas_eur_mwh', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh',
    'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2'
]
calendar[cost_cols] = calendar[cost_cols].ffill().bfill()

# ── Output ────────────────────────────────────────────────────────────────────
final = calendar[['date_str', 'year_month', 'year'] + cost_cols].copy()
output_path = os.path.join(DATA_OTHER_DIR, "auto_cost_data3.csv")
final.to_csv(output_path, index=False)

print("\n" + "=" * 60)
print(f"  Saved: {output_path}")
print(f"  Rows: {len(final):,}  |  {final['date_str'].min()} to {final['date_str'].max()}")
print("\n  Missings after fill (target = 0 for all):")
for col in cost_cols:
    n = final[col].isna().sum()
    flag = "OK" if n == 0 else "[!]"
    print(f"    {flag}  {col}: {n} missing")
print("\n  Summary statistics:")
print(final[cost_cols].describe().round(2).to_string())
print("=" * 60)
print("  Done!")
