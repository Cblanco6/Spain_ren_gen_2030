"""
fetch_cost_data.py
==================
Standalone script to automatically download all fuel and carbon cost data
needed for the Spain electricity market model (2020-2024).

Sources:
  - Natural Gas  : MIBGAS (mibgas.es) — official Iberian gas market, EUR/MWh, daily
  - Coal API2    : yfinance 'MTF=F'   — API2 Rotterdam futures, USD/t, daily
  - Diesel/Gasoil: yfinance 'HO=F'    — NYMEX Heating Oil futures (Proxy), USD/Gallon, daily
  - EUR/USD FX   : yfinance 'EURUSD=X' — for USD → EUR conversion
  - EU ETS CO2   : Local Excel — data/other/Coal_Diesel_ETS_Monthly_Costs.xlsx
  - Uranium      : Local Excel — data/other/Uranium_Annual_Prices2020-2024.xlsx

Output:
  data/other/auto_cost_data.csv  — daily cost file ready to replace the manual Excels
"""

import os
import requests
import warnings
import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_OTHER_DIR = os.path.join(REPO_ROOT, "data", "other")
os.makedirs(DATA_OTHER_DIR, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
YEAR_START = 2020
YEAR_END   = 2024
START_DATE = f"{YEAR_START}-01-01"
END_DATE   = f"{YEAR_END}-12-31"

# Conversion factors
COAL_MWH_PER_TON      = 8.14    # 1 metric ton of thermal coal ≈ 8.14 MWh
DIESEL_MWH_PER_GALLON = 0.0406  # 1 US Gallon of Heating Oil/Diesel ≈ 0.0406 MWh

print("=" * 60)
print(" fetch_cost_data.py — Automated Cost Data Downloader")
print("=" * 60)

# ── Step 1: Build a perfect daily calendar ─────────────────────────────────────
print("\n[1/5] Building daily calendar...")
calendar = pd.DataFrame({
    'date': pd.date_range(start=START_DATE, end=END_DATE, freq='D')
})
calendar['date_str']   = calendar['date'].dt.strftime('%Y-%m-%d')
calendar['year_month'] = calendar['date'].dt.strftime('%Y-%m')
calendar['year']       = calendar['date'].dt.year

# ── Step 2: Natural Gas — MIBGAS ───────────────────────────────────────────────
print("\n[2/5] Downloading Natural Gas prices from MIBGAS...")
mibgas_dfs = []
for year in range(YEAR_START, YEAR_END + 1):
    url = f"https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            xls = pd.ExcelFile(pd.io.common.BytesIO(resp.content))
            sheet = None
            for s in xls.sheet_names:
                if 'D+1' in s or 'DA' in s.upper() or 'DAY' in s.upper():
                    sheet = s
                    break
            if sheet is None:
                sheet = xls.sheet_names[0]  

            df_raw = xls.parse(sheet, header=None)

            header_row = None
            for i, row in df_raw.iterrows():
                row_str = ' '.join([str(v) for v in row.values]).lower()
                if 'trading' in row_str or 'fecha' in row_str or 'date' in row_str:
                    header_row = i
                    break

            if header_row is not None:
                df_year = xls.parse(sheet, header=header_row)
                date_col  = next((c for c in df_year.columns if 'trading' in str(c).lower() or 'fecha' in str(c).lower() or 'date' in str(c).lower()), df_year.columns[0])
                price_col = next((c for c in df_year.columns if 'reference' in str(c).lower() or 'precio' in str(c).lower() or 'price' in str(c).lower()), df_year.columns[1])
                
                df_year = df_year[[date_col, price_col]].copy()
                df_year.columns = ['date_str', 'cost_gas_eur_mwh']
                df_year['date_str'] = pd.to_datetime(df_year['date_str'], errors='coerce').dt.strftime('%Y-%m-%d')
                df_year = df_year.dropna(subset=['date_str', 'cost_gas_eur_mwh'])
                df_year['cost_gas_eur_mwh'] = pd.to_numeric(df_year['cost_gas_eur_mwh'], errors='coerce')
                
                mibgas_dfs.append(df_year)
                print(f"   MIBGAS {year}: {len(df_year)} days downloaded")
            else:
                print(f"   [!] MIBGAS {year}: could not find header row in sheet '{sheet}'")
        else:
            print(f"   [!] MIBGAS {year}: HTTP {resp.status_code}")
    except Exception as e:
        print(f"   [!] MIBGAS {year}: {e}")

if mibgas_dfs:
    df_gas = pd.concat(mibgas_dfs, ignore_index=True).drop_duplicates(subset='date_str')
    calendar = pd.merge(calendar, df_gas, on='date_str', how='left')
    print(f"   Gas: {calendar['cost_gas_eur_mwh'].notna().sum()} trading days with data")
else:
    print("   [!!!] MIBGAS download failed entirely. cost_gas_eur_mwh will be NaN.")
    calendar['cost_gas_eur_mwh'] = np.nan

# ── Step 3: Coal, Diesel (Proxy), EUR/USD — yfinance ─────────────────────────
print("\n[3/5] Downloading Coal, Diesel (Proxy), and FX from Yahoo Finance...")
# Tickers:
#   MTF=F  → API2 Rotterdam Coal futures (USD/t)
#   HO=F   → NYMEX Heating Oil / Diesel Proxy (USD/Gallon)
#   EURUSD=X → EUR/USD spot rate

tickers = ['MTF=F', 'HO=F', 'EURUSD=X']
try:
    raw = yf.download(tickers, start=START_DATE, end=f"{YEAR_END+1}-01-01", progress=False)
    if isinstance(raw.columns, pd.MultiIndex):
        raw = raw['Close']
    raw.index = pd.to_datetime(raw.index).strftime('%Y-%m-%d')
    raw = raw.reset_index().rename(columns={'index': 'date_str', 'Date': 'date_str', 'Datetime': 'date_str'})
    raw.columns.values[0] = 'date_str'
    calendar = pd.merge(calendar, raw, on='date_str', how='left')
    print(f"   yfinance: downloaded {len(raw)} rows")
except Exception as e:
    print(f"   [!] yfinance error: {e}")
    for col in ['MTF=F', 'HO=F', 'EURUSD=X']:
        calendar[col] = np.nan

# ── Step 4: CO2 and Uranium — Local Excels ─────────────────────────────────────
print("\n[4/5] Reading CO2 and Uranium prices from local Excels...")

# 4.1 CO2: Reading from Coal_Diesel_ETS_Monthly_Costs
cde_path_xlsx = os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.xlsx")
cde_path_csv  = os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.csv")
try:
    if os.path.exists(cde_path_xlsx):
        df_cde = pd.read_excel(cde_path_xlsx)
    elif os.path.exists(cde_path_csv):
        df_cde = pd.read_csv(cde_path_csv)
    else:
        raise FileNotFoundError("Coal_Diesel_ETS file not found in data/other/")

    col_fecha = 'Date' if 'Date' in df_cde.columns else df_cde.columns[0]
    df_cde['year_month'] = pd.to_datetime(df_cde[col_fecha]).dt.strftime('%Y-%m')
    
    # Identify CO2 column flexibly
    ets_col = 'eu_ets_usd_ton' if 'eu_ets_usd_ton' in df_cde.columns else 'eu_ets_price_eur_tco2'
    df_ets = df_cde[['year_month', ets_col]].rename(columns={ets_col: 'eu_ets_price_eur_tco2'})
    
    calendar = pd.merge(calendar, df_ets, on='year_month', how='left')
    print(f"   CO2 (EU ETS): {calendar['eu_ets_price_eur_tco2'].notna().sum()} days mapped")
except Exception as e:
    print(f"   [!] CO2: {e} — filling with NaN")
    calendar['eu_ets_price_eur_tco2'] = np.nan

# 4.2 Uranium: Reading from Uranium_Annual_Prices
uranium_path_xlsx = os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.xlsx")
uranium_path_csv  = os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.csv")
try:
    if os.path.exists(uranium_path_xlsx):
        df_ura = pd.read_excel(uranium_path_xlsx)
    elif os.path.exists(uranium_path_csv):
        df_ura = pd.read_csv(uranium_path_csv)
    else:
        raise FileNotFoundError("Uranium file not found in data/other/")

    col_year = 'year' if 'year' in df_ura.columns else df_ura.columns[0]
    df_ura['year'] = pd.to_datetime(df_ura[col_year].astype(str)).dt.year
    df_ura['cost_uranium_eur_mwh'] = pd.to_numeric(df_ura['cost_uranium_eur_mwh'], errors='coerce') / 30.211
    df_ura = df_ura[['year', 'cost_uranium_eur_mwh']]
    
    calendar = pd.merge(calendar, df_ura, on='year', how='left')
    print(f"   Uranium: {calendar['cost_uranium_eur_mwh'].notna().sum()} days mapped")
except Exception as e:
    print(f"   [!] Uranium: {e} — filling with NaN")
    calendar['cost_uranium_eur_mwh'] = np.nan

# ── Step 5: Unit conversions ───────────────────────────────────────────────────
print("\n[5/5] Applying unit conversions...")

# Coal: USD/t → EUR/MWh
if 'MTF=F' in calendar.columns and 'EURUSD=X' in calendar.columns:
    calendar['cost_coal_eur_mwh'] = (
        pd.to_numeric(calendar['MTF=F'], errors='coerce')
        / pd.to_numeric(calendar['EURUSD=X'], errors='coerce')
        / COAL_MWH_PER_TON
    )
else:
    calendar['cost_coal_eur_mwh'] = np.nan

# Diesel: USD/Gallon → EUR/MWh
if 'HO=F' in calendar.columns and 'EURUSD=X' in calendar.columns:
    calendar['cost_diesel_eur_mwh'] = (
        pd.to_numeric(calendar['HO=F'], errors='coerce')
        / pd.to_numeric(calendar['EURUSD=X'], errors='coerce')
        / DIESEL_MWH_PER_GALLON
    )
else:
    calendar['cost_diesel_eur_mwh'] = np.nan

# ── Step 6: Fill weekends and holidays (forward fill then backward fill) ────────
print("\n[6/6] Filling weekends and market holidays...")
cost_cols = ['cost_gas_eur_mwh', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh',
             'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2']
calendar[cost_cols] = calendar[cost_cols].ffill().bfill()

# ── Output ─────────────────────────────────────────────────────────────────────
final = calendar[['date_str', 'year_month', 'year'] + cost_cols].copy()
output_path = os.path.join(DATA_OTHER_DIR, "auto_cost_data2.csv")
final.to_csv(output_path, index=False)

print("\n" + "=" * 60)
print(f" Output saved: {output_path}")
print(f" Rows: {len(final):,} | Date range: {final['date_str'].min()}  {final['date_str'].max()}")
print("\n Missings per column (should be 0 after fill):")
print(final[cost_cols].isna().sum().to_string())

print("\n Summary statistics:")
print(final[cost_cols].describe().round(2).to_string())
print("=" * 60)
print(" Done!")