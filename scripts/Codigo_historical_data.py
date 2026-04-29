import requests
import pandas as pd
import time
import calendar
import numpy as np
import os
import warnings
import yfinance as yf
from entsoe import EntsoePandasClient

warnings.filterwarnings('ignore')

# Resolve paths relative to the repo root, regardless of where the script is run from
REPO_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR       = os.path.join(REPO_ROOT, "data")
DATA_OTHER_DIR = os.path.join(REPO_ROOT, "data", "other")

# =========================================================
# 1. CONFIGURATION AND DICTIONARIES
# =========================================================

TOKEN = "e5abc167c3936f4d527b6e44b07a748f8796092b5fc767a358a2d84784dc7050"
HEADERS = {'x-api-key': TOKEN, 'Accept': 'application/json'}
BASE_URL = "https://api.esios.ree.es/indicators/"
APIKey_entsoe = "0b312fe1-9d1a-4de1-a307-767ff7968c3c"

YEAR_START = 2020
YEAR_END = 2024

# HOURLY DICTIONARIES
IDS_DEMANDA         = {1201: "demand_less1kV_mwh", 1202: "demand_1kV_14kV_mwh", 1203: "demand_14kV_36kV_mwh",  1204: "demand_36kV_72.5kV_mwh",
                        1205: "demand_72.5kV_145kV_mwh", 1206: "demand_145kV_220kV_mwh", 1207: "demand_more220kV_mwh", 2037: "total_national_demand_mwh"}
IDS_INTERCONEXIONES = {10207: "net_raw_France_mwh", 10208: "net_raw_Portugal_mwh", 10209: "net_raw_Morocco_mwh"}
IDS_PRECIO          = {600: "spot_price_eur_mwh"}
IDS_CAPACIDAD       = {1478: "coal_cap_mw", 1483: "combined_cycle_cap_mw", 1480: "gas_turbine_cap_mw", 1481: "vapor_turbine_cap_mw",
                        1489: "cogeneration_cap_mw", 1479: "diesel_cap_mw", 1490: "nonrenewable_waste_cap_mw", 1477: "nuclear_cap_mw",
                        1475: "conventional_hydro_cap_mw", 1476: "pumped_hydro_cap_mw", 1486: "solar_pv_cap_mw", 1487: "solar_thermal_cap_mw",
                        1485: "wind_cap_mw", 1488: "other_renewable_cap_mw", 1491: "renewable_waste_cap_mw"}
IDS_GENERACION      = {2038: "wind_gen_mwh", 2039: "nuclear_gen_mwh", 2040: "coal_gen_mwh",
                        2041: "combined_cycle_gen_mwh", 2042: "conventional_hydro_gen_mwh", 2044: "solar_pv_gen_mwh",
                        2045: "solar_thermal_gen_mwh", 2047: "diesel_gen_mwh", 2048: "gas_turbine_gen_mwh",
                        2049: "vapor_turbine_gen_mwh", 2046: "other_renewable_gen_mwh",
                        10039: "cogeneration_gen_mwh", 10040: "nonrenewable_waste_gen_mwh", 10062: "renewable_waste_gen_mwh"}
IDS_AVAILABLE       = { 474: "nuclear_self_reported_cap_mw"}

# =========================================================
# 2. DOWNLOAD ENGINES
# =========================================================

# ESIOS
def fetch_hourly(indicator_id, start_year, end_year):
    """
    Downloads hourly data for a specific indicator from the ESIOS API.
    Processed quarterly to avoid API saturation. Returns a DataFrame with hourly averaged values.
    """
    all_data = []
    for y in range(start_year, end_year + 1):
        for q in [(1,3), (4,6), (7,9), (10,12)]:
            last_day = calendar.monthrange(y, q[1])[1]
            sd, ed = f"{y}-{q[0]:02d}-01", f"{y}-{q[1]:02d}-{last_day}"
            url = f"{BASE_URL}{indicator_id}?start_date={sd}T00:00&end_date={ed}T23:59&time_trunc=hour&time_agg=avg"
            if indicator_id in [10039, 10040, 10062, 472, 474]:
                url += "&geo_agg=sum"
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                if r.status_code == 200:
                    all_data.extend(r.json().get('indicator', {}).get('values', []))
                time.sleep(0.1)
            except:
                continue
    if not all_data:
        return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df['datetime'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df = df.drop_duplicates(subset=['datetime']).set_index('datetime')[['value']].resample('h').mean().reset_index()
    return df

def fetch_monthly_capacity(indicator_id, start_year, end_year):
    """
    Downloads monthly capacity data from the ESIOS API, processed annually.
    Returns a DataFrame grouped by 'YYYY-MM' with summed capacity values.
    """
    all_data = []
    for y in range(start_year, end_year + 1):
        url = f"{BASE_URL}{indicator_id}?start_date={y}-01-01T00:00&end_date={y}-12-31T23:59&time_trunc=month"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                all_data.extend(r.json().get('indicator', {}).get('values', []))
            time.sleep(0.1)
        except:
            continue
    if not all_data:
        return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df['datetime'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df['year_month'] = df['datetime'].dt.strftime('%Y-%m')
    return df.groupby('year_month')['value'].sum().reset_index()

# ENTSO-E (Generation Only)
def fetch_entsoe_hydro(start_year, end_year):
    """
    Downloads hydroelectric generation data from the ENTSO-E API for Spain, month by month.
    Extracts Run-of-river, Reservoir, and Pumped Storage generation and consumption.

    FIX (v5): The index column is renamed defensively regardless of its original name,
    to avoid silent NaN merges when entsoe-py returns 'utc_timestamp' instead of 'index'.
    """
    client = EntsoePandasClient(api_key=APIKey_entsoe)
    all_dfs = []
    for year in range(start_year, end_year + 1):
        print(f" > ENTSO-E Year {year} (Downloading generation month by month)...")
        for month in range(1, 13):
            start = pd.Timestamp(f'{year}-{month:02d}-01', tz='UTC')
            if month == 12:
                end = pd.Timestamp(f'{year+1}-01-01', tz='UTC')
            else:
                end = pd.Timestamp(f'{year}-{month+1:02d}-01', tz='UTC')

            attempts = 5
            for i in range(attempts):
                try:
                    df = client.query_generation('ES', start=start, end=end)
                    if df is not None and not df.empty:
                        df = df.resample('h').mean()
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = ['_'.join(col).strip('_') for col in df.columns]
                        all_dfs.append(df)
                    break
                except Exception as e:
                    if "503" in str(e) or "Timeout" in str(e):
                        if i < attempts - 1:
                            print(f"     [!] Server congested in {year}-{month:02d}. Retrying in 10s... (Attempt {i+2}/{attempts})")
                            time.sleep(10)
                        else:
                            print(f"   [!] Persistent ENTSO-E error in {year}-{month:02d}: {e}")
                    else:
                        print(f"   [!] ENTSO-E Error in {year}-{month:02d}: {e}")
                        break
            time.sleep(0.5)

    if not all_dfs:
        return pd.DataFrame()

    entsoe_df = pd.concat(all_dfs)
    entsoe_df = entsoe_df[~entsoe_df.index.duplicated(keep='first')]

    # set index name explicitly before reset,
    # so the resulting column is always 'datetime' regardless of entsoe-py version.
    entsoe_df.index.name = 'datetime'
    entsoe_df = entsoe_df.reset_index()

    col_ror = [c for c in entsoe_df.columns if 'Run-of-river' in c]
    if col_ror:
        entsoe_df['run_of_river_hydro_gen_mwh'] = entsoe_df[col_ror[0]]
    col_res = [c for c in entsoe_df.columns if 'Reservoir' in c]
    if col_res:
        entsoe_df['conventional_hydro_gen_mwh'] = entsoe_df[col_res[0]]
    col_pump_gen  = [c for c in entsoe_df.columns if 'Pumped Storage' in c and 'Aggregated' in c]
    col_pump_cons = [c for c in entsoe_df.columns if 'Pumped Storage' in c and 'Consumption' in c]
    entsoe_df['pumped_hydro_gen_mwh']         = entsoe_df[col_pump_gen[0]].fillna(0)  if col_pump_gen  else 0
    entsoe_df['pumped_hydro_consumption_mwh'] = entsoe_df[col_pump_cons[0]].fillna(0) if col_pump_cons else 0

    cols_to_keep = ['datetime', 'run_of_river_hydro_gen_mwh', 'conventional_hydro_gen_mwh',
                    'pumped_hydro_gen_mwh', 'pumped_hydro_consumption_mwh']
    return entsoe_df[[c for c in cols_to_keep if c in entsoe_df.columns]]


# =========================================================
# --- EXECUTION ---
# =========================================================

print(f"Downloading HOURLY DATA ({YEAR_START}-{YEAR_END})...")
# Creates a perfect chronological calendar base
full_df = pd.DataFrame({'datetime': pd.date_range(
    start=f"{YEAR_START}-01-01", end=f"{YEAR_END}-12-31 23:00", freq='h', tz='UTC')})

# Iterates over all indicators and merges them into the calendar
for id_ext, name in {**IDS_DEMANDA, **IDS_INTERCONEXIONES, **IDS_PRECIO, **IDS_GENERACION, **IDS_AVAILABLE}.items():
    temp_df = fetch_hourly(id_ext, YEAR_START, YEAR_END)
    if not temp_df.empty:
        full_df = pd.merge(full_df, temp_df.rename(columns={'value': name}), on='datetime', how='left')

print(f"Downloading MONTHLY CAPACITIES ({YEAR_START}-{YEAR_END})...")
full_df['year_month'] = full_df['datetime'].dt.strftime('%Y-%m')
for id_ext, name in IDS_CAPACIDAD.items():
    temp_cap = fetch_monthly_capacity(id_ext, YEAR_START, YEAR_END)
    if not temp_cap.empty:
        temp_cap = temp_cap.rename(columns={'value': name})
        full_df = pd.merge(full_df, temp_cap, on='year_month', how='left')

print("\nVerifying integrity of downloaded data...")
all_expected_cols = list({**IDS_DEMANDA, **IDS_INTERCONEXIONES, **IDS_PRECIO,
                           **IDS_GENERACION, **IDS_AVAILABLE, **IDS_CAPACIDAD}.values())
for col in all_expected_cols:
    if col not in full_df.columns:
        print(f" [!] ESIOS Warning: '{col}' failed to download. Filling with 0s.")
        full_df[col] = 0.0

df_entsoe = fetch_entsoe_hydro(YEAR_START, YEAR_END)
if not df_entsoe.empty:
    full_df = pd.merge(
        full_df.drop(columns=['conventional_hydro_gen_mwh', 'run_of_river_hydro_gen_mwh',
                               'pumped_hydro_gen_mwh', 'pumped_hydro_consumption_mwh'], errors='ignore'),
        df_entsoe, on='datetime', how='left')


# =========================================================
# 3. ROBUST IMPUTATION (MISSING DATA HANDLING)
# =========================================================
print("\n--- STARTING ROBUST MISSING DATA IMPUTATION ---")

full_df = full_df.sort_values(by='datetime').reset_index(drop=True)

initial_missings = full_df.isna().sum().sum()
print(f"Detected {initial_missings} empty cells (NaNs). Starting robust fill...")

time_cols  = ['datetime', 'year_month']
# Spot price excluded from 24h-shift imputation.
# Daily copy is valid for physical quantities (demand, generation) which follow
# strong diurnal seasonality. Prices do not share this property — linear
# interpolation is more appropriate and avoids importing yesterday's price spike.
price_cols = ['spot_price_eur_mwh']
data_cols  = [col for col in full_df.columns if col not in time_cols]
non_price_data_cols = [col for col in data_cols if col not in price_cols]

# Step 1: 1-hour gaps → Forward fill from previous hour (all columns)
full_df[data_cols] = full_df[data_cols].ffill(limit=1)

# Step 2: Larger blocks → Copy from 24 hours ago (physical quantities only, NOT prices)
for _ in range(3):
    full_df[non_price_data_cols] = full_df[non_price_data_cols].fillna(
        full_df[non_price_data_cols].shift(24))

# Step 3: Remaining gaps → Linear interpolation + backward fill (all columns)
full_df[data_cols] = full_df[data_cols].interpolate(method='linear')
full_df[data_cols] = full_df[data_cols].bfill()

final_missings = full_df.isna().sum().sum()
print(f"Missings filled: {initial_missings - final_missings}")
print(f"Remaining Missings: {final_missings} (Should be 0)")
print("--- IMPUTATION COMPLETE ---\n")


# =========================================================
# 4. GENERAL PROCESSING & MANUAL CAPACITIES
# =========================================================

# Fixed hydro capacities (manually set from REE official figures, MW)
# Run of river increased to 2000 MW to cover generation peaks (~1994 MW).
# Conventional hydro decreased proportionally (15771.4 - (2000 - 1154.8)) to maintain total hydro capacity.
full_df['run_of_river_hydro_cap_mw']   = 2000.0
full_df['pumped_hydro_turb_cap_mw']    = 3417.5
full_df['pumped_hydro_pump_cap_mw']    = 2278.0
full_df['conventional_hydro_cap_mw']   = 14926.2

# Battery capacity by year (MW, source: REE System Reports 2020-2024).
# Grid-scale battery storage in Spain remained at ~25 MW throughout 2020-2024
# per official REE installed capacity reports. This will be parameterized
# separately in the Julia model for the 2030 scenario projections.
BATTERY_CAP_MW_BY_YEAR = {2020: 10, 2021: 10, 2022: 15, 2023: 25, 2024: 25}
full_df['batteries_cap_mw'] = full_df['datetime'].dt.year.map(BATTERY_CAP_MW_BY_YEAR).fillna(25)
full_df['batteries_gen_mwh'] = 0.0

print("Processing Demand...")
# Correct magnitude if total demand is reported in wrong scale
if full_df['total_national_demand_mwh'].mean() > 50000:
    full_df['total_national_demand_mwh'] = full_df['total_national_demand_mwh'] / 10

# Calculate the share of each voltage level and split into residential, commercial, industrial
levels = [f'demand_{l}_mwh' for l in ['less1kV', '1kV_14kV', '14kV_36kV', '36kV_72.5kV', '72.5kV_145kV', '145kV_220kV', 'more220kV']]
sum_dis = full_df[levels].sum(axis=1).replace(0, np.nan)  # Avoid division by zero
full_df['mdh'] = full_df['datetime'].dt.strftime('%m-%d-%H')
for col in levels:
    share = (full_df[col] / sum_dis).groupby(full_df['mdh']).transform('mean').ffill().bfill()
    full_df[col] = share * full_df['total_national_demand_mwh']

full_df['residential_demand_mwh'] = full_df['demand_less1kV_mwh']
full_df['commercial_demand_mwh']  = full_df['demand_1kV_14kV_mwh'] + full_df['demand_14kV_36kV_mwh']
full_df['industrial_demand_mwh']  = (full_df['demand_36kV_72.5kV_mwh'] + full_df['demand_72.5kV_145kV_mwh']
                                     + full_df['demand_145kV_220kV_mwh'] + full_df['demand_more220kV_mwh'])

print("Processing Interconnections, Capacities and Generation...")
# Split raw net flows into isolated Import and Export columns
for c in ['France', 'Portugal', 'Morocco']:
    raw = f"net_raw_{c}_mwh"
    if raw in full_df.columns:
        full_df[f"net_flows_{c}_mwh"] = full_df[raw]
        full_df[f"imports_{c}_mwh"]   = full_df[raw].clip(lower=0)
        full_df[f"exports_{c}_mwh"]   = full_df[raw].clip(upper=0).abs()

# Added .replace(0, np.nan) to avoid division-by-zero producing inf/NaN
# in solar and wind capacity factors (consistent with nuclear treatment).
full_df['nuclear_cap_factor']       = (full_df['nuclear_self_reported_cap_mw']
                                        / full_df['nuclear_cap_mw'].replace(0, np.nan)).clip(0, 1).fillna(0)
full_df['solar_pv_cap_factor']      = (full_df['solar_pv_gen_mwh']
                                        / full_df['solar_pv_cap_mw'].replace(0, np.nan)).clip(0, 1).fillna(0)
full_df['solar_thermal_cap_factor'] = (full_df['solar_thermal_gen_mwh']
                                        / full_df['solar_thermal_cap_mw'].replace(0, np.nan)).clip(0, 1).fillna(0)
full_df['wind_cap_factor']          = (full_df['wind_gen_mwh']
                                        / full_df['wind_cap_mw'].replace(0, np.nan)).clip(0, 1).fillna(0)


# Ensure no negative generation or capacity factor values
all_gen_factors = [c for c in full_df.columns if '_gen_' in c or '_cap_factor' in c]
for c in all_gen_factors:
    full_df[c] = full_df[c].clip(lower=0)

# Format explicit time columns
full_df['time_long'] = full_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
for attr in ['year', 'month', 'day', 'hour']:
    full_df[attr] = getattr(full_df['datetime'].dt, attr)


# =========================================================
# 5. PROCESSING: FUEL & CO2 COSTS  (automated fetch + fallbacks)
# =========================================================
# Sources:
#   Gas     : MIBGAS (mibgas.es) — Iberian day-ahead, EUR/MWh, daily
#   Coal    : yfinance 'MTF=F'  — API2 Rotterdam futures, USD/t -> EUR/MWh
#   Diesel  : yfinance 'HO=F'  — NYMEX Heating Oil, USD/gal -> EUR/MWh
#   EUR/USD : yfinance 'EURUSD=X' — for USD -> EUR conversion
#   EU ETS  : yfinance '^ICEEUA' (fallback: Coal_Diesel_ETS_Monthly_Costs.xlsx)
#   Uranium : yfinance 'UX=F'   (fallback: Uranium_Annual_Prices2020-2024.xlsx)

print("Integrating Fuel and CO2 Costs (automated fetch)...")

# Conversion factors
COAL_MWH_PER_TON      = 8.14    # 1 metric ton thermal coal  ~ 8.14 MWh
DIESEL_MWH_PER_GALLON = 0.0406  # 1 US gallon heating oil    ~ 0.0406 MWh
URANIUM_MWH_PER_LB    = 30.211  # U3O8 lb -> MWh fuel equivalent

yf_start = f"{YEAR_START}-01-01"
yf_end   = f"{YEAR_END + 1}-01-01"  # yfinance end date is exclusive

# Build a daily cost calendar (one row per calendar day)
cost_cal = pd.DataFrame({'date': pd.date_range(
    start=f"{YEAR_START}-01-01", end=f"{YEAR_END}-12-31", freq='D')})
cost_cal['date_str']   = cost_cal['date'].dt.strftime('%Y-%m-%d')
cost_cal['year_month'] = cost_cal['date'].dt.strftime('%Y-%m')
cost_cal['year']       = cost_cal['date'].dt.year

# ── Natural Gas: MIBGAS API (primary) → local Excel (fallback) ───────────────
print("  [Gas] Downloading from MIBGAS...")
mibgas_dfs = []
for _yr in range(YEAR_START, YEAR_END + 1):
    _url = (f"https://www.mibgas.es/en/file-access/MIBGAS_Data_{_yr}.xlsx"
            f"?path=AGNO_{_yr}/XLS")
    try:
        _resp = requests.get(_url, timeout=30)
        if _resp.status_code != 200:
            print(f"    [!] {_yr}: HTTP {_resp.status_code}"); continue
        _xls  = pd.ExcelFile(pd.io.common.BytesIO(_resp.content))
        _sheet = next((s for s in _xls.sheet_names if 'D+1' in s or 'DA' in s.upper()),
                      _xls.sheet_names[0])
        _raw  = _xls.parse(_sheet, header=None)
        _hrow = next((i for i, row in _raw.iterrows()
                      if any(k in str(v).lower() for v in row.values
                             for k in ['trading', 'fecha', 'date'])), None)
        if _hrow is None:
            print(f"    [!] {_yr}: header not found"); continue
        _df = _xls.parse(_sheet, header=_hrow)
        _dc = next((c for c in _df.columns if any(
                    k in str(c).lower() for k in ['trading', 'fecha', 'date'])), _df.columns[0])
        _pc = next((c for c in _df.columns if any(
                    k in str(c).lower() for k in ['reference', 'precio', 'price'])), _df.columns[1])
        _dy = _df[[_dc, _pc]].copy()
        _dy.columns = ['date_str', 'cost_gas_eur_mwh']
        _dy['date_str'] = pd.to_datetime(_dy['date_str'], errors='coerce').dt.strftime('%Y-%m-%d')
        _dy['cost_gas_eur_mwh'] = pd.to_numeric(_dy['cost_gas_eur_mwh'], errors='coerce')
        _dy = _dy.dropna(subset=['date_str', 'cost_gas_eur_mwh'])
        mibgas_dfs.append(_dy)
        print(f"    OK {_yr}: {len(_dy)} trading days")
    except Exception as _e:
        print(f"    [!] {_yr}: {_e}")

if mibgas_dfs:
    _gas_df = pd.concat(mibgas_dfs, ignore_index=True).drop_duplicates('date_str')
    cost_cal = pd.merge(cost_cal, _gas_df, on='date_str', how='left')
    print(f"    -> {cost_cal['cost_gas_eur_mwh'].notna().sum()} days with gas price")
else:
    print("    [!!!] MIBGAS failed. Trying local Excel fallback...")
    _gas_ok = False
    for _p in [os.path.join(DATA_OTHER_DIR, "NaturalGas_Daily_Prices2020-2025.xlsx"),
               os.path.join(DATA_OTHER_DIR, "NaturalGas_Daily_Prices2020-2025.csv")]:
        if not os.path.exists(_p): continue
        try:
            _gf = pd.read_excel(_p) if _p.endswith('.xlsx') else pd.read_csv(_p)
            _dc = 'Trading day' if 'Trading day' in _gf.columns else _gf.columns[0]
            _pc = 'Reference Price [EUR/MWh]' if 'Reference Price [EUR/MWh]' in _gf.columns else _gf.columns[1]
            _gf['date_str'] = pd.to_datetime(_gf[_dc]).dt.strftime('%Y-%m-%d')
            _gf = _gf.rename(columns={_pc: 'cost_gas_eur_mwh'})[['date_str', 'cost_gas_eur_mwh']]
            cost_cal = pd.merge(cost_cal, _gf, on='date_str', how='left')
            print(f"    OK Fallback: {os.path.basename(_p)}")
            _gas_ok = True; break
        except Exception as _e:
            print(f"    [!] {os.path.basename(_p)}: {_e}")
    if not _gas_ok:
        cost_cal['cost_gas_eur_mwh'] = np.nan

# ── Coal, Diesel, EUR/USD FX: yfinance ───────────────────────────────────────
print("  [Coal/Diesel] Downloading from Yahoo Finance...")
try:
    _yf = yf.download(['MTF=F', 'HO=F', 'EURUSD=X'],
                      start=yf_start, end=yf_end, progress=False)
    if isinstance(_yf.columns, pd.MultiIndex):
        _yf = _yf['Close']
    _yf.index = pd.to_datetime(_yf.index).strftime('%Y-%m-%d')
    _yf = _yf.reset_index()
    _yf.columns.values[0] = 'date_str'
    cost_cal = pd.merge(cost_cal, _yf, on='date_str', how='left')
    _fx = pd.to_numeric(cost_cal['EURUSD=X'], errors='coerce')
    cost_cal['cost_coal_eur_mwh'] = (
        pd.to_numeric(cost_cal['MTF=F'], errors='coerce') / _fx / COAL_MWH_PER_TON)
    cost_cal['cost_diesel_eur_mwh'] = (
        pd.to_numeric(cost_cal['HO=F'], errors='coerce') / _fx / DIESEL_MWH_PER_GALLON)
    print(f"    OK Coal:   {cost_cal['cost_coal_eur_mwh'].notna().sum()} days")
    print(f"    OK Diesel: {cost_cal['cost_diesel_eur_mwh'].notna().sum()} days")
except Exception as _e:
    print(f"    [!] yfinance error: {_e}")
    cost_cal['cost_coal_eur_mwh']   = np.nan
    cost_cal['cost_diesel_eur_mwh'] = np.nan

# ── EU ETS CO2: yfinance (primary) → local Excel fallback ────────────────────
print("  [ETS] Downloading EU ETS CO2 price...")
_ets_ok = False
try:
    _ets = yf.download('^ICEEUA', start=yf_start, end=yf_end, progress=False)
    if isinstance(_ets.columns, pd.MultiIndex):
        _ets = _ets['Close']
    else:
        _ets = _ets[['Close']]
    _ets.index = pd.to_datetime(_ets.index).strftime('%Y-%m-%d')
    _ets = _ets.reset_index()
    _ets.columns = ['date_str', 'eu_ets_price_eur_tco2']
    _ets['eu_ets_price_eur_tco2'] = pd.to_numeric(_ets['eu_ets_price_eur_tco2'], errors='coerce')
    if _ets['eu_ets_price_eur_tco2'].notna().sum() > 100:
        cost_cal = pd.merge(cost_cal, _ets, on='date_str', how='left')
        print(f"    OK ^ICEEUA: {_ets['eu_ets_price_eur_tco2'].notna().sum()} days")
        _ets_ok = True
    else:
        print(f"    [!] ^ICEEUA: insufficient data — using local Excel fallback")
except Exception as _e:
    print(f"    [!] ^ICEEUA: {_e} — using local Excel fallback")

if not _ets_ok:
    for _p in [os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.xlsx"),
               os.path.join(DATA_OTHER_DIR, "Coal_Diesel_ETS_Monthly_Costs.csv")]:
        if not os.path.exists(_p): continue
        try:
            _cde = pd.read_excel(_p) if _p.endswith('.xlsx') else pd.read_csv(_p)
            _dc  = 'Date' if 'Date' in _cde.columns else _cde.columns[0]
            _cde['year_month'] = pd.to_datetime(_cde[_dc]).dt.strftime('%Y-%m')
            _ec  = next((c for c in _cde.columns if 'ets' in c.lower()), None)
            if not _ec: raise ValueError("ETS column not found")
            _ets_m = _cde[['year_month', _ec]].rename(columns={_ec: 'eu_ets_price_eur_tco2'})
            cost_cal = pd.merge(cost_cal, _ets_m, on='year_month', how='left')
            print(f"    OK Fallback: {os.path.basename(_p)}")
            _ets_ok = True; break
        except Exception as _e:
            print(f"    [!] {os.path.basename(_p)}: {_e}")
    if not _ets_ok:
        print("    [!!!] EU ETS: all sources failed — filling with 0")
        cost_cal['eu_ets_price_eur_tco2'] = 0.0

# ── Uranium: yfinance (primary) → local Excel fallback ───────────────────────
print("  [Uranium] Downloading Uranium price...")
_ura_yf = False
try:
    _ura = yf.download('UX=F', start=yf_start, end=yf_end, progress=False)
    if isinstance(_ura.columns, pd.MultiIndex):
        _ura = _ura['Close']
    else:
        _ura = _ura[['Close']]
    _ura.index = pd.to_datetime(_ura.index).strftime('%Y-%m-%d')
    _ura = _ura.reset_index()
    _ura.columns = ['date_str', 'uranium_usd_lb']
    _ura['uranium_usd_lb'] = pd.to_numeric(_ura['uranium_usd_lb'], errors='coerce')
    if _ura['uranium_usd_lb'].notna().sum() > 100:
        cost_cal = pd.merge(cost_cal, _ura, on='date_str', how='left')
        _fx = pd.to_numeric(cost_cal.get('EURUSD=X'), errors='coerce')
        cost_cal['cost_uranium_eur_mwh'] = (
            pd.to_numeric(cost_cal['uranium_usd_lb'], errors='coerce') / _fx / URANIUM_MWH_PER_LB)
        print(f"    OK UX=F: {_ura['uranium_usd_lb'].notna().sum()} days")
        _ura_yf = True
    else:
        print(f"    [!] UX=F: insufficient data — using local Excel fallback")
except Exception as _e:
    print(f"    [!] UX=F: {_e} — using local Excel fallback")

if not _ura_yf:
    for _p in [os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.xlsx"),
               os.path.join(DATA_OTHER_DIR, "Uranium_Annual_Prices2020-2024.csv")]:
        if not os.path.exists(_p): continue
        try:
            _uf = pd.read_excel(_p) if _p.endswith('.xlsx') else pd.read_csv(_p)
            _yc = 'year' if 'year' in _uf.columns else _uf.columns[0]
            _uf['year'] = pd.to_datetime(_uf[_yc].astype(str)).dt.year
            _uf['cost_uranium_eur_mwh'] = (
                pd.to_numeric(_uf['cost_uranium_eur_mwh'], errors='coerce') / URANIUM_MWH_PER_LB)
            cost_cal = pd.merge(cost_cal, _uf[['year', 'cost_uranium_eur_mwh']], on='year', how='left')
            print(f"    OK Fallback: {os.path.basename(_p)} (annual -> daily)")
            break
        except Exception as _e:
            print(f"    [!] {os.path.basename(_p)}: {_e}")
    if 'cost_uranium_eur_mwh' not in cost_cal.columns:
        print("    [!!!] Uranium: all sources failed — filling with 0")
        cost_cal['cost_uranium_eur_mwh'] = 0.0

# ── Fill weekends / market holidays, then merge into hourly dataframe ─────────
_cost_cols = ['cost_gas_eur_mwh', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh',
              'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2']
cost_cal = cost_cal.sort_values('date')
cost_cal[_cost_cols] = cost_cal[_cost_cols].ffill().bfill()

full_df['_date_only'] = full_df['datetime'].dt.strftime('%Y-%m-%d')
full_df = pd.merge(
    full_df,
    cost_cal[['date_str'] + _cost_cols],
    left_on='_date_only', right_on='date_str', how='left'
).drop(columns=['date_str', '_date_only'])
full_df[_cost_cols] = full_df[_cost_cols].ffill().bfill()


# =========================================================
# 6. STRICT EXPORT AND GW CONVERSION
# =========================================================

print("Converting Demands, Generation and Capacities to GW/GWh...")

# 1. VOLUMES: divide by 1000 (MW → GW, MWh → GWh)
quantities = [c for c in full_df.columns if c.endswith('_mw') or c.endswith('_mwh')]
quantities = [c for c in quantities if not c.startswith('spot_') and not c.startswith('cost_') and not c.startswith('eu_ets')]
full_df[quantities] = full_df[quantities] / 1000.0
rename_dict_quant = {c: c.replace('_mwh', '_gwh').replace('_mw', '_gw') for c in quantities}
full_df = full_df.rename(columns=rename_dict_quant)

# 2. PRICES: multiply by 1000 (€/MWh → €/GWh). Spot price is kept in EUR/MWh.
prices = [c for c in full_df.columns if c.startswith('cost_')]
full_df[prices] = full_df[prices] * 1000.0
rename_dict_prices = {c: c.replace('_mwh', '_gwh') for c in prices}
full_df = full_df.rename(columns=rename_dict_prices)

official_order = [
    'time_long', 'year', 'month', 'day', 'hour', 'spot_price_eur_mwh',
    'residential_demand_gwh', 'commercial_demand_gwh', 'industrial_demand_gwh',

    # Capacities
    'coal_cap_gw', 'combined_cycle_cap_gw', 'gas_turbine_cap_gw', 'vapor_turbine_cap_gw',
    'cogeneration_cap_gw', 'diesel_cap_gw', 'nonrenewable_waste_cap_gw', 'nuclear_cap_gw',
    'conventional_hydro_cap_gw', 'run_of_river_hydro_cap_gw', 'pumped_hydro_turb_cap_gw', 'pumped_hydro_pump_cap_gw',
    'solar_pv_cap_gw', 'solar_thermal_cap_gw', 'wind_cap_gw', 'other_renewable_cap_gw',
    'renewable_waste_cap_gw', 'batteries_cap_gw',

    # Generation
    'coal_gen_gwh', 'combined_cycle_gen_gwh', 'gas_turbine_gen_gwh', 'vapor_turbine_gen_gwh',
    'cogeneration_gen_gwh', 'diesel_gen_gwh', 'nonrenewable_waste_gen_gwh', 'nuclear_gen_gwh',
    'conventional_hydro_gen_gwh', 'run_of_river_hydro_gen_gwh', 'pumped_hydro_gen_gwh',
    'pumped_hydro_consumption_gwh', 'solar_pv_gen_gwh', 'solar_thermal_gen_gwh', 'wind_gen_gwh',
    'other_renewable_gen_gwh', 'renewable_waste_gen_gwh', 'batteries_gen_gwh',

    # Capacity Factors
    'nuclear_cap_factor', 'solar_pv_cap_factor', 'solar_thermal_cap_factor', 'wind_cap_factor',

    # Interconnections
    'imports_France_gwh', 'exports_France_gwh', 'net_flows_France_gwh',
    'imports_Portugal_gwh', 'exports_Portugal_gwh', 'net_flows_Portugal_gwh',
    'imports_Morocco_gwh', 'exports_Morocco_gwh', 'net_flows_Morocco_gwh',

    # External Costs
    'cost_coal_eur_gwh', 'cost_gas_eur_gwh', 'cost_diesel_eur_gwh', 'cost_uranium_eur_gwh', 'eu_ets_price_eur_tco2'
]

present_columns = [c for c in official_order if c in full_df.columns]

os.makedirs(DATA_DIR, exist_ok=True)
output_path = os.path.join(DATA_DIR, "historical_data.csv")
full_df[present_columns].to_csv(output_path, index=False)

print(f"\n Dataset saved: {output_path}")
print(f" Rows: {len(full_df):,} | Columns: {len(present_columns)}")
print("Script completed successfully!")