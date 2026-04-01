import requests
import pandas as pd
import time
import calendar
import numpy as np
import os
import traceback
from entsoe import EntsoePandasClient

# =========================================================
# 1. CONFIGURACIÓN Y DICCIONARIOS
# =========================================================

TOKEN = "e5abc167c3936f4d527b6e44b07a748f8796092b5fc767a358a2d84784dc7050"
HEADERS = {'x-api-key': TOKEN, 'Accept': 'application/json'}
BASE_URL = "https://api.esios.ree.es/indicators/"
APIKey_entsoe = "0b312fe1-9d1a-4de1-a307-767ff7968c3c"

YEAR_START = 2020
YEAR_END = 2024

# DICCIONARIOS HORARIOS
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
                        2049: "vapor_turbine_gen_mwh", 2046: "other_renewable_gen_mwh", 2050: "auxiliary_generation_gen_mwh",
                        10039: "cogeneration_gen_mwh", 10040: "nonrenewable_waste_gen_mwh", 10062: "renewable_waste_gen_mwh"}
IDS_AVAILABLE       = {472: "hydro_self_reported_cap_mw", 474: "nuclear_self_reported_cap_mw"}

# =========================================================
# 2. MOTORES DE DESCARGA
# =========================================================

# ESIOS
def fetch_hourly(indicator_id, start_year, end_year):
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
            except: continue
    if not all_data: return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df['datetime'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df = df.drop_duplicates(subset=['datetime']).set_index('datetime')[['value']].resample('h').mean().reset_index()
    return df
# Esta funcion descarga datos horarios para un indicador específico de la API de ESIOS.
# Procesa los datos y devuelve un DataFrame con los valores promediados por hora. Lo hace de forma trimestral (para evitar que se saturen las APIS) 
# y maneja casos especiales de agregación geográfica, para tratar el team de datos de la peninsula vs baleares y canarias. 
# Se asegura tambien de que si la periodicidad de lso datos es diferente a 1hr, esta se ajuste, y trata los ajustes diferente segun la naturaleza de los datos.
# El resultado es un DataFrame con columnas 'datetime' y 'value', donde 'datetime' es el timestamp horario y 'value' es el valor del indicador para esa hora.


def fetch_monthly_capacity(indicator_id, start_year, end_year):
    all_data = []
    for y in range(start_year, end_year + 1):
        url = f"{BASE_URL}{indicator_id}?start_date={y}-01-01T00:00&end_date={y}-12-31T23:59&time_trunc=month"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                all_data.extend(r.json().get('indicator', {}).get('values', []))
            time.sleep(0.1)
        except: continue
    if not all_data: return pd.DataFrame()
    df = pd.DataFrame(all_data)
    df['datetime'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df['year_month'] = df['datetime'].dt.strftime('%Y-%m')
    return df.groupby('year_month')['value'].sum().reset_index()
# Esta función descarga datos mensuales de capacidad para un indicador específico de la API de ESIOS.
# Procesa los datos y devuelve un DataFrame con los valores sumados por mes. Lo hace de forma anual (para evitar que se saturen las APIS)
# y maneja casos especiales de agregación geográfica, para tratar el team de datos de la peninsula vs baleares y canarias.
# Se asegura tambien de que si la periodicidad de lso datos es diferente a 1hr, esta se ajuste, y trata los ajustes diferente segun la naturaleza de los datos.
# El resultado es un DataFrame con columnas 'year_month' y 'value', donde 'year_month' es el mes en formato 'YYYY-MM' y 'value' es la suma de los valores del indicador para ese mes.


# ENTSO-E (Solo Generación)
def fetch_entsoe_hydro(start_year, end_year):
    client = EntsoePandasClient(api_key=APIKey_entsoe)
    all_dfs = []
    for year in range(start_year, end_year + 1):
        print(f" > ENTSO-E Año {year} (Descargando generación mes a mes)...")
        for month in range(1, 13):
            start = pd.Timestamp(f'{year}-{month:02d}-01', tz='UTC')
            if month == 12:
                end = pd.Timestamp(f'{year+1}-01-01', tz='UTC')
            else:
                end = pd.Timestamp(f'{year}-{month+1:02d}-01', tz='UTC')
            
            intentos = 5
            for i in range(intentos):
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
                        if i < intentos - 1:
                            print(f"     [!] Servidor congestionado en {year}-{month:02d}. Reintentando en 10s... (Intento {i+2}/{intentos})")
                            time.sleep(10) 
                        else:
                            print(f"   [!] Error persistente ENTSO-E en {year}-{month:02d}: {e}")
                    else:
                        print(f"   [!] Error ENTSO-E en {year}-{month:02d}: {e}")
                        break
            
            time.sleep(0.5)
            
    if not all_dfs: return pd.DataFrame()
    entsoe_df = pd.concat(all_dfs)
    entsoe_df = entsoe_df[~entsoe_df.index.duplicated(keep='first')]
    entsoe_df = entsoe_df.reset_index().rename(columns={'index': 'datetime'})
    col_ror = [c for c in entsoe_df.columns if 'Run-of-river' in c]
    if col_ror: entsoe_df['run_of_river_hydro_gen_mwh'] = entsoe_df[col_ror[0]]
    col_res = [c for c in entsoe_df.columns if 'Reservoir' in c]
    if col_res: entsoe_df['conventional_hydro_gen_mwh'] = entsoe_df[col_res[0]]
    col_pump_gen = [c for c in entsoe_df.columns if 'Pumped Storage' in c and 'Aggregated' in c]
    col_pump_cons = [c for c in entsoe_df.columns if 'Pumped Storage' in c and 'Consumption' in c]
    entsoe_df['pumped_hydro_gen_mwh'] = entsoe_df[col_pump_gen[0]].fillna(0) if col_pump_gen else 0
    entsoe_df['pumped_hydro_consumption_mwh'] = entsoe_df[col_pump_cons[0]].fillna(0) if col_pump_cons else 0
    cols_to_keep = ['datetime', 'run_of_river_hydro_gen_mwh', 'conventional_hydro_gen_mwh', 'pumped_hydro_gen_mwh', 'pumped_hydro_consumption_mwh']
    return entsoe_df[[c for c in cols_to_keep if c in entsoe_df.columns]]
# Explicacion de la funcion fetch_entsoe_hydro: descarga datos de generación hidroeléctrica desde la API de ENTSO-E para España.
# Descarga los datos mes a mes para evitar problemas de timeout o congestión del servidor. Maneja casos de error específicos para reintentar en caso de congestión.
# Procesa los datos para extraer la generación de tipo "Run-of-river", "Reservoir" y "Pumped Storage" (tanto generación como consumo) y devuelve un DataFrame con estas columnas.
# El resultado es un DataFrame con columnas 'datetime', 'run_of_river_hydro_gen_mwh', 'conventional_hydro_gen_mwh', 'pumped_hydro_gen_mwh' y 'pumped_hydro_consumption_mwh'. 
# Donde cada fila representa la generación o consumo de hidroeléctrica para esa hora específica.


# --- EJECUCIÓN ---

print(f"Descargando DATOS HORARIOS ({YEAR_START}-{YEAR_END})...")
full_df = pd.DataFrame({'datetime': pd.date_range(start=f"{YEAR_START}-01-01", end=f"{YEAR_END}-12-31 23:00", freq='h', tz='UTC')})
for id_ext, name in {**IDS_DEMANDA, **IDS_INTERCONEXIONES, **IDS_PRECIO, **IDS_GENERACION, **IDS_AVAILABLE}.items():
    temp_df = fetch_hourly(id_ext, YEAR_START, YEAR_END)
    if not temp_df.empty:
        full_df = pd.merge(full_df, temp_df.rename(columns={'value': name}), on='datetime', how='left')
# Este bloque es el núcleo de la descarga de datos. 
# Primero, se crea un DataFrame 'full_df' con una columna 'datetime' que contiene todas las horas desde el inicio hasta el final del período especificado.
# Luego, se itera sobre todos los indicadores definidos en los diccionarios (demanda, interconexiones, precio, generación y capacidad disponible) 
# y llama a la función 'fetch_hourly' para cada uno. Si la función devuelve un DataFrame no vacío, se fusiona con 'full_df' usando un merge por la columna 'datetime'.


print(f"Descargando CAPACIDADES MENSUALES ({YEAR_START}-{YEAR_END})...")
full_df['year_month'] = full_df['datetime'].dt.strftime('%Y-%m') 
for id_ext, name in IDS_CAPACIDAD.items():
    temp_cap = fetch_monthly_capacity(id_ext, YEAR_START, YEAR_END)
    if not temp_cap.empty:
        temp_cap = temp_cap.rename(columns={'value': name})
        full_df = pd.merge(full_df, temp_cap, on='year_month', how='left')
# Este bloque se encarga de descargar los datos de capacidad que tienen una periodicidad mensual.
# Primero, se crea una nueva columna 'year_month' en 'full_df' que contiene el año y mes de cada fila en formato 'YYYY-MM'.
# Luego, se itera sobre los indicadores de capacidad definidos en el diccionario IDS_CAPACIDAD y se llama a la función 'fetch_monthly_capacity' para cada uno.
# Si la función devuelve un DataFrame no vacío, se renombra la columna 'value' al nombre del indicador y se fusiona con 'full_df' usando un merge por la columna 'year_month'.


print("\nVerificando integridad de datos descargados...")
todas_columnas_esperadas = list({**IDS_DEMANDA, **IDS_INTERCONEXIONES, **IDS_PRECIO, **IDS_GENERACION, **IDS_AVAILABLE, **IDS_CAPACIDAD}.values())
for col in todas_columnas_esperadas:
    if col not in full_df.columns:
        print(f" [!] Advertencia de ESIOS: '{col}' falló al descargar. Rellenando con 0s para evitar cuelgues.")
        full_df[col] = 0.0
# Este bloque se encarga de verificar que todas las columnas esperadas estén presentes en el DataFrame 'full_df'.
# Si alguna columna no está presente, se imprime una advertencia y se rellena con 0s para evitar cuelgues.

df_entsoe = fetch_entsoe_hydro(YEAR_START, YEAR_END)
if not df_entsoe.empty:
    full_df = pd.merge(full_df.drop(columns=['conventional_hydro_gen_mwh', 'run_of_river_hydro_gen_mwh', 'pumped_hydro_gen_mwh', 'pumped_hydro_consumption_mwh'], errors='ignore'), df_entsoe, on='datetime', how='left')
# Este bloque se encarga de descargar los datos de generación hidroeléctrica desde la API de ENTSO-E 
# utilizando la función 'fetch_entsoe_hydro'. Si la función devuelve un DataFrame no vacío, se fusiona con 'full_df' usando un merge por la columna 'datetime'.


# =========================================================
# 3. PROCESAMIENTO GENERAL Y CAPACIDADES MANUALES
# =========================================================


full_df['temp_year'] = full_df['datetime'].dt.year
full_df['run_of_river_hydro_cap_mw'] = 1154.8
full_df['pumped_hydro_turbine_cap_mw'] = 3417.5
full_df['pumped_hydro_pump_cap_mw'] = 2.278
full_df['conventional_hydro_cap_mw'] = 15771.4
full_df = full_df.drop(columns=['temp_year'])
# Este bloque asigna valores fijos de capacidad para diferentes tipos de generación hidroeléctrica.
# Se asignan capacidades para "run_of_river_hydro_cap_mw", "pumped_hydro_turbine_cap_mw", "pumped_hydro_pump_cap_mw" y "conventional_hydro_cap_mw" 
# Basándose en datos conocidos o supuestos.

print("\nProcesando Demanda...")
if full_df['total_national_demand_mwh'].mean() > 50000:
    full_df['total_national_demand_mwh'] = full_df['total_national_demand_mwh'] / 10
levels = [f'demand_{l}_mwh' for l in ['less1kV', '1kV_14kV', '14kV_36kV', '36kV_72.5kV', '72.5kV_145kV', '145kV_220kV', 'more220kV']]
full_df[levels] = full_df[levels].interpolate(method='linear', limit=48)
sum_dis = full_df[levels].sum(axis=1)
full_df['mdh'] = full_df['datetime'].dt.strftime('%m-%d-%H')
for col in levels:
    share = (full_df[col] / sum_dis).groupby(full_df['mdh']).transform('mean').ffill().bfill()
    full_df[col] = share * full_df['total_national_demand_mwh']
full_df['residential_demand_mwh'] = full_df['demand_less1kV_mwh']
full_df['commercial_demand_mwh'] = full_df['demand_1kV_14kV_mwh'] + full_df['demand_14kV_36kV_mwh']
full_df['industrial_demand_mwh'] = full_df['demand_36kV_72.5kV_mwh'] + full_df['demand_72.5kV_145kV_mwh'] + full_df['demand_145kV_220kV_mwh'] + full_df['demand_more220kV_mwh']
# Este bloque se encarga de procesar los datos de demanda para corregir posibles errores y rellenar valores faltantes.
# Primero, se verifica si la demanda total parece estar en MWh en lugar de GWh (lo que se deduce si el promedio es mayor a 50,000 MWh). Si es así, se divide por 10 para corregirlo.
# Luego, se identifican las columnas de demanda por nivel de tensión y se interpolan para rellenar valores faltantes.
# Después, se calcula la suma de la demanda por niveles y se utiliza para calcular la participación promedio de cada nivel en la demanda total para cada combinación de mes, día y hora (mdh).
# Finalmente, se asignan los valores corregidos a cada nivel de demanda y se crean columnas adicionales para demanda residencial, comercial e industrial sumando los niveles correspondientes. 


print("Procesando Interconexiones, Capacidad y Generación...")
for c in ['France', 'Portugal', 'Morocco']:
    raw = f"net_raw_{c}_mwh"
    if raw in full_df.columns:
        full_df[f"net_flows_{c}_mwh"] = full_df[raw]
        full_df[f"imports_{c}_mwh"] = full_df[raw].clip(lower=0)
        full_df[f"exports_{c}_mwh"] = full_df[raw].clip(upper=0).abs()
# Este bloque se encarga de procesar los datos de interconexiones para cada país (Francia, Portugal y Marruecos).
# Para cada país, se verifica si la columna de flujo neto existe. Si existe, se crea una nueva columna 'net_flows_{c}_mwh' que es igual al flujo neto original. 
# Luego se crean dos columnas adicionales: 
# 'imports_{c}_mwh' que contiene solo los valores positivos (importaciones) 
# 'exports_{c}_mwh' que contiene solo los valores negativos (exportaciones) convertidos a positivos.
# Esto permite analizar por separado las importaciones y exportaciones de electricidad con cada país. 
# Si la columna de flujo neto no existe, no se crean estas columnas adicionales para ese país. 


# Capacidades y Fijas restantes
full_df[list(IDS_CAPACIDAD.values())] = full_df[list(IDS_CAPACIDAD.values())].ffill().bfill(limit=24*31)
full_df['batteries_cap_mw'] = 25          
# Este bloque se encarga de procesar los datos de capacidad para rellenar valores faltantes y asignar una capacidad fija para baterías.
# Primero, se toma la lista de columnas de capacidad definidas en el diccionario IDS_CAPIDAD 
# Se aplica un método de forward fill (ffill) seguido de backward fill (bfill) con un límite de 31 días (24 horas * 31 días) para rellenar valores faltantes.
# Esto significa que si hay un valor faltante, se rellenará con el último valor conocido hacia adelante
# si aún quedan valores faltantes después de eso, se rellenarán con el siguiente valor conocido hacia atrás, pero solo hasta un máximo de 31 días.
# Luego, se asigna una capacidad fija de 25 MW para baterías, ya que no se dispone de datos específicos para esta categoría.


gen_cols = list(IDS_GENERACION.values())
if set(gen_cols).issubset(full_df.columns):
    full_df[gen_cols] = full_df[gen_cols].interpolate(method='linear', limit=2)
full_df['batteries_gen_mwh'] = 0
# Este bloque se encarga de procesar los datos de generación para rellenar valores faltantes y asignar una generación fija para baterías.


hydro_cols = ['run_of_river_hydro_gen_mwh', 'conventional_hydro_gen_mwh', 'pumped_hydro_gen_mwh', 'pumped_hydro_consumption_mwh']
for hc in hydro_cols:
    if hc not in full_df.columns:
        full_df[hc] = 0.0
    else:
        full_df[hc] = full_df[hc].fillna(0.0)
# Este bloque se encarga de procesar los datos de generación hidroeléctrica para asegurarse de que no haya valores faltantes.
# Se define una lista de columnas relacionadas con la generación hidroeléctrica (run-of-river, convencional, bombeo y consumo de bombeo).
# Para cada una de estas columnas, se verifica si existe en el DataFrame 'full_df'. Si no existe, se crea la columna y se rellena con 0.0. Si existe, se rellenan los valores faltantes con 0.0.


full_df['hydro_self_reported_cap_mw'] = full_df['hydro_self_reported_cap_mw'].interpolate(method='linear', limit=48).ffill().bfill()
full_df['nuclear_self_reported_cap_mw'] = full_df['nuclear_self_reported_cap_mw'].interpolate(method='linear', limit=48).ffill().bfill()
# Este bloque se encarga de procesar las columnas de capacidad auto-reportada para hidroeléctrica y nuclear.
# Para cada una de estas columnas, se aplica un método de interpolación lineal con un límite de 48 horas para rellenar valores faltantes. 
# Luego se aplica un forward fill (ffill) seguido de backward fill (bfill) para asegurarse de que no queden valores faltantes, utilizando los valores conocidos más cercanos hacia adelante y hacia atrás.

full_df['nuclear_cap_factor'] = (full_df['nuclear_self_reported_cap_mw'] / full_df['nuclear_cap_mw'].replace(0, np.nan)).clip(0, 1).fillna(0)
# Este bloque se encarga de calcular el factor de capacidad para la generación nuclear.
# El factor de capacidad se calcula dividiendo la capacidad auto-reportada por la capacidad total de nuclear. Para evitar divisiones por cero, se reemplazan los valores de capacidad total de nuclear que son 0 por NaN.
# Luego, se utiliza el método 'clip' para limitar los valores del factor de capacidad entre 0 y 1, ya que un factor de capacidad no puede ser negativo ni mayor a 1. 
# Finalmente, se rellenan los valores faltantes con 0, lo que significa que si no hay datos disponibles para la capacidad auto-reportada o la capacidad total, se asumirá que el factor de capacidad es 0.

total_hydro_cap = (
    full_df['conventional_hydro_cap_mw'] + 
    full_df['run_of_river_hydro_cap_mw'] + 
    full_df['pumped_hydro_turbine_cap_mw'] + 
    full_df['pumped_hydro_pump_cap_mw']
).replace(0, np.nan)
# Este bloque se encarga de calcular la capacidad total de generación hidroeléctrica sumando las capacidades de los diferentes tipos de generación hidroeléctrica (convencional, run-of-river, bombeo como turbina y bombeo como bomba).
# Para evitar divisiones por cero en el cálculo del factor de capacidad hidroeléctrica, se reemplazan los valores de capacidad total de hidroeléctrica que son 0 por NaN. 
# Esto significa que si la suma de las capacidades hidroeléctricas es 0, se considerará como un valor faltante en lugar de 0, lo que permitirá manejarlo adecuadamente en el cálculo del factor de capacidad.


full_df['solar_pv_cap_factor'] = (full_df['solar_pv_gen_mwh'] / full_df['solar_pv_cap_mw']).clip(0, 1)
full_df['solar_thermal_cap_factor'] = (full_df['solar_thermal_gen_mwh'] / full_df['solar_thermal_cap_mw']).clip(0, 1)
full_df['wind_cap_factor'] = (full_df['wind_gen_mwh'] / full_df['wind_cap_mw']).clip(0, 1)
# Este bloque se encarga de calcular los factores de capacidad para la generación hidroeléctrica, solar fotovoltaica, solar térmica y eólica.
# Para cada tipo de generación, el factor de capacidad se calcula dividiendo la generación real (en MWh) por la capacidad instalada (en MW).
# Para los factores de capacidad solar y eólica, se utilizan las columnas correspondientes de generación y capacidad.
# Luego, se utiliza el método 'clip' para limitar los valores de los factores de capacidad entre 0 y 1, ya que un factor de capacidad no puede ser negativo ni mayor a 1.


todas_gen_factors = [c for c in full_df.columns if '_gen_' in c or '_cap_factor' in c]
for c in todas_gen_factors:
    full_df[c] = full_df[c].clip(lower=0)
# Este bloque se encarga de asegurarse de que todos los valores en las columnas relacionadas con la generación y los factores de capacidad sean no negativos.
# Se crea una lista de todas las columnas que contienen '_gen_' o '_cap_factor' en su nombre, lo que indica que están relacionadas con la generación o los factores de capacidad.
# Luego, para cada una de estas columnas, se utiliza el método 'clip' para establecer un límite inferior de 0, lo que significa que cualquier valor negativo se reemplazará por 0. 


full_df['time_long'] = full_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
for attr in ['year', 'month', 'day', 'hour']:
    full_df[attr] = getattr(full_df['datetime'].dt, attr)
# Este bloque se encarga de crear una nueva columna 'time_long' que contiene la fecha y hora en formato de texto completo, y también extrae atributos individuales de la fecha y hora (año, mes, día y hora) en columnas separadas.
# La columna 'time_long' se formatea como 'YYYY-MM-DD HH:MM:SS' utilizando el método 'strftime'.
# Luego, se utiliza un bucle para extraer el año, mes, día y hora de la columna 'datetime' y asignarlos a nuevas columnas 'year', 'month', 'day' y 'hour' respectivamente, utilizando el método 'dt' para acceder a los atributos de fecha y hora.


# =========================================================
# 4. PROCESAMIENTO: COSTOS DE COMBUSTIBLE Y CO2 
# =========================================================

print("Integrando Costos de Combustible y CO2...")
try:
    def load_file_flexible(base_name):
        if os.path.exists(f"{base_name}.xlsx"): return pd.read_excel(f"{base_name}.xlsx")
        elif os.path.exists(f"{base_name}.csv"): return pd.read_csv(f"{base_name}.csv")
        else: raise FileNotFoundError(f"No se encontró {base_name}")
    df_gas = load_file_flexible("NaturalGas_Daily_Prices2020-2025")
    df_cde = load_file_flexible("Coal_Diesel_ETS_Monthly_Costs")
    df_ura = load_file_flexible("Uranium_Annual_Prices2020-2024")
    col_fecha_gas = 'Trading day' if 'Trading day' in df_gas.columns else df_gas.columns[0]
    col_precio_gas = 'Reference Price [EUR/MWh]' if 'Reference Price [EUR/MWh]' in df_gas.columns else df_gas.columns[1]
    df_gas['date'] = pd.to_datetime(df_gas[col_fecha_gas]).dt.strftime('%Y-%m-%d')
    df_gas = df_gas.rename(columns={col_precio_gas: 'cost_gas_eur_mwh'})[['date', 'cost_gas_eur_mwh']]
    col_fecha_cde = 'Date' if 'Date' in df_cde.columns else df_cde.columns[0]
    df_cde['year_month'] = pd.to_datetime(df_cde[col_fecha_cde]).dt.strftime('%Y-%m')
    df_cde = df_cde.rename(columns={'coal_eur_mwh': 'cost_coal_eur_mwh', 'diesel_pretax_eur_mwh': 'cost_diesel_eur_mwh', 'eu_ets_usd_ton': 'eu_ets_price_eur_tco2'})[['year_month', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh', 'eu_ets_price_eur_tco2']]
    col_fecha_ura = 'year' if 'year' in df_ura.columns else df_ura.columns[0]
    df_ura['year'] = pd.to_datetime(df_ura[col_fecha_ura].astype(str)).dt.year
    df_ura['cost_uranium_eur_mwh'] = df_ura['cost_uranium_eur_mwh'] / 30.211
    df_ura = df_ura[['year', 'cost_uranium_eur_mwh']]
    full_df['date_only'] = full_df['datetime'].dt.strftime('%Y-%m-%d')
    full_df = pd.merge(full_df, df_gas, left_on='date_only', right_on='date', how='left').drop(columns=['date', 'date_only'])
    full_df = pd.merge(full_df, df_cde, on='year_month', how='left')
    full_df = pd.merge(full_df, df_ura, on='year', how='left')
    cost_cols = ['cost_gas_eur_mwh', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh', 'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2']
    full_df[cost_cols] = full_df[cost_cols].ffill().bfill()
except Exception as e:
    for c in ['cost_gas_eur_mwh', 'cost_coal_eur_mwh', 'cost_diesel_eur_mwh', 'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2']: full_df[c] = 0
if 'spot_price_eur_mwh' in full_df.columns:
    full_df['spot_price_eur_mwh'] = full_df['spot_price_eur_mwh'].interpolate(method='linear', limit=2)
# Este bloque se encarga de integrar los costos de combustible y CO2 en el DataFrame 'full_df'.
# Primero, se define una función 'load_file_flexible' que intenta cargar un archivo con el nombre base dado, buscando tanto en formato Excel como CSV. Si no se encuentra el archivo, se lanza un error.
# Luego, se cargan los datos de precios de gas natural, carbón, diésel y uranio desde los archivos correspondientes utilizando la función definida.
# Se procesan las fechas y se renombran las columnas para que tengan nombres consistentes con el formato del DataFrame 'full_df'.
# Después, se crea una columna 'date_only' en 'full_df' para facilitar la fusión con los datos de precios diarios. Se realizan fusiones (merge) para integrar los costos de gas, carbón, diésel, uranio y el precio del EU ETS en 'full_df'.
# Finalmente, se rellenan los valores faltantes en las columnas de costos utilizando forward fill y backward fill, y se interpolan los valores faltantes en la columna de precio spot si está presente. 
# Si ocurre algún error durante este proceso, se asignan valores de 0 a las columnas de costos para evitar cuelgues.


# =========================================================
# 5. EXPORTACIÓN ESTRICTA Y CAMBIO A GW
# =========================================================

print("Convirtiendo Demandas, Generación y Capacidades a GW/GWh...")
cantidades = [c for c in full_df.columns if c.endswith('_mw') or c.endswith('_mwh')]
cantidades = [c for c in cantidades if not c.startswith('spot_') and not c.startswith('cost_') and not c.startswith('eu_ets')]
full_df[cantidades] = full_df[cantidades] / 1000.0
rename_dict = {c: c.replace('_mwh', '_gwh').replace('_mw', '_gw') for c in cantidades}
full_df = full_df.rename(columns=rename_dict)

orden_oficial = [
    'time_long', 'year', 'month', 'day', 'hour', 'spot_price_eur_mwh', 
    'residential_demand_gwh', 'commercial_demand_gwh', 'industrial_demand_gwh', 
    
    # Capacidades
    'coal_cap_gw', 'combined_cycle_cap_gw', 'gas_turbine_cap_gw', 'vapor_turbine_cap_gw', 
    'cogeneration_cap_gw', 'diesel_cap_gw', 'nonrenewable_waste_cap_gw', 'nuclear_cap_gw', 
    'conventional_hydro_cap_gw', 'run_of_river_hydro_cap_gw', 'pumped_hydro_turbine_cap_gw', 'pumped_hydro_pump_cap_gw',
    'solar_pv_cap_gw', 'solar_thermal_cap_gw', 'wind_cap_gw', 'other_renewable_cap_gw', 
    'renewable_waste_cap_gw', 'batteries_cap_gw', 
    
    # Generaciones
    'coal_gen_gwh', 'combined_cycle_gen_gwh', 'gas_turbine_gen_gwh', 'vapor_turbine_gen_gwh', 
    'cogeneration_gen_gwh', 'diesel_gen_gwh', 'nonrenewable_waste_gen_gwh', 'nuclear_gen_gwh', 
    'conventional_hydro_gen_gwh', 'run_of_river_hydro_gen_gwh', 'pumped_hydro_gen_gwh', 
    'pumped_hydro_consumption_gwh', 'solar_pv_gen_gwh', 'solar_thermal_gen_gwh', 'wind_gen_gwh', 
    'other_renewable_gen_gwh', 'renewable_waste_gen_gwh', 'auxiliary_generation_gen_gwh', 'batteries_gen_gwh',
    
    # Cap Factors
    'nuclear_cap_factor', 'hydro_cap_factor', 'solar_pv_cap_factor', 'solar_thermal_cap_factor', 'wind_cap_factor',
    
    # Interconexiones
    'imports_France_gwh', 'exports_France_gwh', 'net_flows_France_gwh', 
    'imports_Portugal_gwh', 'exports_Portugal_gwh', 'net_flows_Portugal_gwh', 
    'imports_Morocco_gwh', 'exports_Morocco_gwh', 'net_flows_Morocco_gwh',
    
    # Costos Externos
    'cost_coal_eur_mwh', 'cost_gas_eur_mwh', 'cost_diesel_eur_mwh', 'cost_uranium_eur_mwh', 'eu_ets_price_eur_tco2']

columnas_presentes = [c for c in orden_oficial if c in full_df.columns]
full_df[columnas_presentes].to_csv("data_version5.csv",index=False)

print("¡Script completado exitosamente!")