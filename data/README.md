# Data
Construcción del Dataset del Mercado Eléctrico Español (2020-2024)
Este repositorio contiene el código automatizado en Python para descargar, limpiar, procesar y unificar los datos del sistema eléctrico español. El objetivo es crear un dataset maestro horario, listo para ser utilizado en modelos de optimización matemática (ej. Julia/Pyomo) o análisis económicos.

1- Fuente de datos: 

El script se alimenta de dos fuentes principales:
A) API de ESIOS (Red Eléctrica de España): Descargamos dinámicamente más de 35 indicadores referentes a Demanda, Generación, Capacidad Instalada, Interconexiones internacionales y Precio Spot.
B) Archivos Estáticos (Costos Marginales): Datos de mercados financieros que no están en ESIOS, agrupados en archivos Excel/CSV:
  - Precio diario del Gas Natural (Mibgas/TTF).
  - Precios mensuales del Carbón, Diésel y derechos de emisión de CO2 (EU ETS).
  - Precio anual del Uranio.


2. Pocesamiento de los datos
   
Paso 1: Extracción Horaria y Agrupación temporal
Los datos de generación e interconexiones en ESIOS a veces vienen en bloques de 10 o 15 minutos. 
El código descarga estos bloques y los promedia automáticamente a 1 hora (resample('h').mean()) para estandarizar toda la serie temporal. 
Para algunos indicadores fragmentados geográficamente (como la Cogeneración o Residuos), el código aplica una suma geográfica (geo_agg=sum) para obtener el total nacional exacto.

Paso 2: Tratamiento de Capacidades Instaladas
A diferencia de la generación, la capacidad instalada no cambia cada hora. 
El código descarga estos datos agrupándolos mensualmente (time_trunc=month) y suma todas las centrales individuales del país para obtener un único valor sólido de Potencia Instalada (MW) por tecnología, que luego se asigna a todas las horas de ese mes.


Paso 3: Reconciliación de la Demanda
ESIOS proporciona la demanda por niveles de tensión. Los agrupamos en sectores económicos:
  - Residencial: < 1 kV
  - Comercial: 1 kV - 36 kV
  - Industrial: > 36 kV
Para asegurar que la suma de estos sectores cuadre perfectamente con la "Demanda Nacional Total" oficial, calculamos el "peso" (share) de cada sector y lo multiplicamos por la Demanda Nacional real.
Así evitamos descuadres matemáticos.

Paso 4: Cálculos de Factores de Capacidad (Capacity Factors)
Calculamos la disponibilidad real de cada tecnología:
Eólica y Solar: Se calcula dividiendo la Generación Real Horaria entre la Capacidad Instalada de ese mes.
Nuclear e Hidroeléctrica: Usamos los indicadores de "Capacidad Disponible Declarada" de ESIOS y los dividimos por su capacidad instalada, permitiendo capturar con precisión las paradas por recarga o mantenimiento operativo.

Paso 5: Integración de Costos
Los archivos externos de costos se cruzan (Left Join) con nuestro esqueleto horario:
El gas se cruza por día exacto.
El CO2, Carbón y Diésel se cruzan por mes.
El Uranio: El precio crudo anual se divide matemáticamente por su factor de eficiencia y quemado térmico (30.211) para obtener el coste real en EUR/MWh eléctrico.


3. Manejo de los Missing Values (NAs)
   
La API puede tener huecos o fallos de conexión. Dependiendo de la naturaleza de la variable, los dividimos en 3 estrategias:

Estrategia 1: Forward Fill (ffill()) - Para Capacidades y Costos
A quién aplica: Capacidad Nuclear, Capacidad Solar, Coste del Gas, Precio CO2.
La lógica: Si sabemos que en enero hay 7.117 MW nucleares, y el siguiente dato es en febrero, rellenamos todas las horas de enero "hacia adelante" con 7.117. (Una central no desaparece de la noche a la mañana). Igual con los precios: si el gas no cotiza el domingo, usamos el precio de cierre del viernes.

Estrategia 2: Imputación por "Perfil Histórico" - Para Demandas
A quién aplica: residential_demand, commercial_demand, industrial_demand.
La lógica: Si falta un bloque entero de datos, no podemos trazar una línea recta porque el consumo tiene picos y valles. Lo que hacemos es calcular el share (porcentaje) promedio histórico para esa hora exacta del año (ej: "¿Qué % de la demanda suele ser residencial los lunes a las 10:00?") y lo multiplicamos por la demanda total nacional (que nunca falta).

Estrategia 3: Interpolación Lineal - Para Generación y Precios Spot
A quién aplica: Generación Eólica, Solar, Precio Spot.
La lógica: Si ESIOS falla y falta la hora 15:00, trazamos una línea recta matemática entre el valor de las 14:00 y las 16:00. Pero ojo: hemos puesto un límite de seguridad (limit=2). Si faltan más de 2 horas seguidas, la interpolación se detiene para no inventarnos energía que no existió.


4. Formato Final del Dataset
   
Como último paso, para facilitar el trabajo a los optimizadores matemáticos (solvers) y evitar problemas de escala con números gigantes, el script convierte automáticamente todos los valores de energía y potencia (MW/MWh) a Gigavatios (GW/GWh) dividiéndolos por 1.000.
Los precios (EUR/MWh) y los Factores de Capacidad (0 a 1) mantienen su escala original.



