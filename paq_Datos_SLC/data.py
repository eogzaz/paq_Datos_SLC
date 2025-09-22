import requests
import pandas as pd
import polars as pl
import numpy as np
import xml.etree.ElementTree as ET
import html, re
from datetime import datetime, timedelta
from .info import *

# Clase que consulta y procesa los datos necesarios para la SLC
class DATA:
    
    def __init__(self, output_format="XML"):
        """
        Constructor de la clase DATA.

        Inicializa las URLs base de las dos APIs oficiales utilizadas:

        - MPC (Minor Planet Center): para la descarga de observaciones astronómicas.
        - NASA JPL Horizons API: para la obtención de efemérides de cuerpos menores.

        No requiere parámetros de entrada.

        Ejemplo:
        --------
        >>> from datautils import DATA
        >>> mpc = DATA()
        """
        # URL base de las API
        self.url_mpc = "https://data.minorplanetcenter.net/api/get-obs"
        self.url_horizons = "https://ssd.jpl.nasa.gov/api/horizons_file.api"
        #formato de salida
        self.output_format = output_format

    #Método para limpiar cadenas XML con caracteres no válidos o mal escapados
    def _sanitize_xml(self, xml_string: str) -> str:
        # Elimina el BOM (Byte Order Mark) si aparece
        s = xml_string.lstrip("\ufeff")
        # Convierte entidades HTML en caracteres normales (ej: &amp; → &)
        s = html.unescape(s)
        # Elimina caracteres de control inválidos en XML
        s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
        # Corrige entidades mal formadas (ej: "&" suelto → "&amp;")
        s = re.sub(r'&(?!#\d+;|#x[0-9A-Fa-f]+;|[A-Za-z][A-Za-z0-9]*;)', '&amp;', s)
        return s

    #Convertir fecha de observacion a objetos datatime (parsear fechas de las observaciones)
    def _parse_obs_time(self, date):
        if date is None or pd.isna(date):  # Maneja valores nulos o NaN
            return pd.NaT
        date = date.strip()
        try:
            # Intenta parsear la fecha directamente con pandas
            return pd.to_datetime(date, errors="raise", utc=True)
        except Exception:
            pass
        try:
            # Manejo especial para fechas con parte fraccional del día (ej: 2025-08-31.5)
            if "." in date and "-" in date:
                base, frac = date.split(".")           # Separa fecha base y fracción
                frac_day = float("0." + frac)         # Convierte la fracción a decimal
                base_date = datetime.strptime(base, "%Y-%m-%d")  # Convierte fecha base
                return pd.to_datetime(base_date + timedelta(days=frac_day), utc=True)     # Suma la fracción de día
        except Exception:
            pass
        return pd.NaT  # Devuelve fecha vacía si no pudo parsear

    # Método público para obtener observaciones de un objeto específico
    def observations_MPC_raw(self, selected_object):
        """
        Obtiene observaciones astronómicas de un objeto desde la API oficial del MPC.

        Este método consulta la API del Minor Planet Center, descarga las observaciones 
        disponibles en formato XML, las procesa y devuelve un DataFrame de pandas con 
        la información estructurada.

        Parámetros
        ----------
        selected_object : str
            Identificador del objeto a consultar (ej. "Ceres", "433", "Pallas").

        Retorna
        -------
        pandas.DataFrame
            DataFrame con las observaciones, incluyendo la columna `obsTime` convertida 
            a formato datetime cuando está disponible.

        Excepciones
        -----------
        RuntimeError
            Si la API no devuelve datos válidos o ocurre un error en la petición.

        Ejemplo
        --------
        >>> df = DATA().observations_MPC_raw("Ceres")
        >>> df.head()
             obsTime     ra     dec   mag ...
        0 2024-01-01  ...   ...    ...
        1 2024-01-02  ...   ...    ...
        
        """        
        # Payload con parámetros de búsqueda (designación del objeto + formato)
        payload = {"desigs": [selected_object], "output_format": [self.output_format]}
        
        # Llamado HTTP a la API del MPC (usa GET con json=payload, aunque usualmente se usaría params o POST)
        response = requests.get(self.url_mpc, json=payload)
        if not response.ok:
            # Si la respuesta falla, lanza error con código y contenido
            raise RuntimeError(f"Error {response.status_code}: {response.content.decode()}")
    
        # Convierte la respuesta JSON a diccionario de Python
        dataset = response.json()
        # Extrae el XML del primer resultado
        xml_string = dataset[0].get("XML", "")
        if not xml_string:
            raise RuntimeError(f"No se encontró contenido XML para '{selected_object}'")
    
        try:
            # Intenta parsear el XML directamente
            root = ET.fromstring(xml_string)
        except ET.ParseError:
            # Si falla, limpia el XML y vuelve a intentar
            xml_string = self._sanitize_xml(xml_string)
            root = ET.fromstring(xml_string)
    
        # Lista donde se almacenarán las observaciones extraídas
        observations = []
        # Recorre cada entrada "optical" dentro del XML (observaciones ópticas)
        for obs in root.findall(".//optical"):
            data = {child.tag: child.text for child in obs}  # Convierte cada nodo hijo en diccionario
            observations.append(data)
    
        # Convierte la lista de observaciones en un DataFrame de pandas
        df = pd.DataFrame(observations)
    
        # Si existe la columna de tiempos de observación, la parsea con la función personalizada
        if "obsTime" in df.columns:
            df["obsTime"] = df["obsTime"].apply(self._parse_obs_time)
    
        return df  # Devuelve el DataFrame con las observaciones            

    #Correción a banda V
    def V_band_correction(self, df):
        """
        Aplica corrección de magnitudes según banda para convertir a banda V.
        Trabaja completamente en Polars (sin loops fila a fila).
        
        Parámetros
        ----------
        df : pl.DataFrame
            DataFrame de Polars con columnas:
            - 'mag'  : magnitud observada
            - 'band' : identificador de banda (str)
        
        Retorna
        -------
        pl.DataFrame
            DataFrame con nueva columna 'Magn_obs' corregida.
        """
    
        Correcciones = {
            'V':0,'R':0.4,'G':0.28,'C':0.4,'r':0.14,'g':-0.35,'c':-0.05,'o':0.33,'w':-0.13,'i':0.32,'v':0,'Vj':0,
            'Rc':0.4,'Sg':-0.35,'Sr':0.14,'Si':0.32,'Pg':-0.35,'Pr':0.14,'Pi':0.32,'Pw':-0.13,'Ao':0.33,'Ac':-0.05,
            ''  :np.nan,'U':np.nan,'u':np.nan,'B':np.nan,'I':np.nan,'J':np.nan,'H':np.nan,'K':np.nan,
            'W':np.nan,'Y':np.nan,'z':np.nan,'y':np.nan,'Lu':np.nan,'Lg':np.nan,'Lr':np.nan,'Lz':np.nan,'Ly':np.nan,
            'VR':np.nan,'Ic':np.nan,'Bj':np.nan,'Uj':np.nan,'Sz':np.nan,'Pz':np.nan,'Py':np.nan,
            'Gb':np.nan,'Gr':np.nan,'N':np.nan,'T':np.nan
        }
    
        # Construcción del DataFrame de correcciones forzando tipo Float64
        tabla_corr = pl.DataFrame(
            {
                "band": list(Correcciones.keys()),
                "corr": list(Correcciones.values())
            },
            schema={"band": pl.Utf8, "corr": pl.Float64},  # <-- forzamos dtype
            strict=False  # <-- permitimos mezcla de tipos y nulos
        )
    
        # Join con el df original
        df = df.join(tabla_corr, on="band", how="left")
    
        # Magnitud corregida
        df = df.with_columns(
            (pl.col("mag").cast(pl.Float64) + pl.col("corr")).alias("Magn_obs")
        )
    
        # Eliminar filas con Magn_obs nula
        df = df.drop_nulls()
        df = df.filter(~df["Magn_obs"].is_nan())
    
        return df       
        
    #Limpieza de datos observacionales
    def observations_MPC_clean(self, selected_object,start_date, end_date):
        
        #Datos de observacion crudos
        df_a = pl.from_pandas(self.observations_MPC_raw(selected_object))

        #Solo se selecciona fecha, magnitud y banda de observacion
        #Se eliminan los registros que no contienen magnitud
        df_b = df_a[['obsTime', 'mag', 'band']].drop_nulls(subset=["mag"]).with_columns([pl.col("mag").cast(pl.Float64),
                                                                                         pl.col("band").cast(pl.Utf8)])

        #Se restringe al rango de fechas especifico
        df_c = df_b.filter((pl.col('obsTime') >= pl.lit(start_date).str.strptime(df_b.schema['obsTime'], strict=False)) &
                           (pl.col('obsTime') <= pl.lit(end_date).str.strptime(df_b.schema['obsTime'], strict=False)))

        #Se corrige la magnitud a banda V
        df = self.V_band_correction(df_c)

        return df
        
        
    #Efemerides
    def get_ephemerides(self, selected_object, start_date, end_date,object_type):
        """
        Obtiene efemérides astronómicas de un objeto desde la API Horizons de la NASA JPL.

        Parámetros
        ----------
        select_object : str
            Identificador del objeto (ejemplo: "Ceres").
        start_date : str
            Fecha inicial en formato 'YYYY-MM-DD'.
        end_date : str
            Fecha final en formato 'YYYY-MM-DD'.

        Retorna
        -------
        pandas.DataFrame
            DataFrame con columnas:
            - 'Date' : Fecha calendario
            - 'Delta' : Distancia Tierra–objeto (ua)
            - 'r' : Distancia objeto–Sol (ua)
            - 'Fase' : Ángulo de fase (grados)

        Ejemplos
        --------
        >>> mpc = DATA()
        >>> df_efe = mpc.efemerides_API("Ceres", "2025-01-01", "2025-01-10")
        >>> df_efe.head()
        """
        # Comandos estilo archivo .api
        if object_type =='Cometa':
            # Comandos estilo archivo .api
            horizons_input = f"""
            !$$SOF
            COMMAND='DES = {selected_object};CAP;'
            OBJ_DATA='YES'
            MAKE_EPHEM='YES'
            TABLE_TYPE='OBSERVER'
            CENTER='500@399'
            START_TIME='{start_date}'
            STOP_TIME='{end_date}'
            STEP_SIZE='1 d'
            QUANTITIES='1,19,20,43'
            !$$EOF
            """
        elif object_type=='Asteroide' or object_type=='Objeto Interestelar' :
            # Comandos estilo archivo .api
            horizons_input = f"""
            !$$SOF
            COMMAND='{selected_object};'
            OBJ_DATA='YES'
            MAKE_EPHEM='YES'
            TABLE_TYPE='OBSERVER'
            CENTER='500@399'
            START_TIME='{start_date}'
            STOP_TIME='{end_date}'
            STEP_SIZE='1 d'
            QUANTITIES='1,19,20,43'
            !$$EOF
            """
        else:
            return None

        # Enviar como parámetro 'input'
        response = requests.post(self.url_horizons, data={'input': horizons_input})
        dataset = response.json()

        # Paso 3: Extraer el contenido plano del resultado
        raw_result = dataset["result"]
        start_idx = raw_result.find('$$SOE')+6
        end_idx = raw_result.find('$$EOE')
        lines = raw_result[start_idx:end_idx].splitlines()

        date, delta, r, alpha = [], [], [], []
        for line in lines:
          date  = np.append(date, line[1:12].strip())                #Fechas 
          delta = np.append(delta, float(line[76:93].strip()))       #Distancia Tierra-objeto
          r     = np.append(r, float(line[48:63].strip()))           #Distancia Sol-objeto
          alpha = np.append(alpha, float(line[108:115].strip()))     #Angulo de fase

        date_datatime = pd.to_datetime(date, format='%Y-%b-%d',utc=True)
        df = pd.DataFrame({'Date':date_datatime,'Delta':delta,'r':r,'Fase':alpha})

        return pl.from_pandas(df)  # Devuelve el DataFrame con las efemerides seleccionadas ( date, delta, r, alpha)
    
    def observations_COBS(self,selected_comet,start_date,end_date):
        """
        Descarga observaciones de un cometa desde la API de COBS y devuelve un DataFrame.
        
        Parámetros
        ----------
        selected_comet : str
            Nombre o designación del cometa (ejemplo: "C/2023 A3").
        max_pages : int
            Máximo número de páginas a descargar (por defecto 5).
        
        Retorna
        -------
        pd.DataFrame
            DataFrame con columnas: obsTime, Magn_obs
        """
        all_obs = []
        page = 1
        while True:
            url = (
                    f"https://cobs.si/api/obs_list.api"
                    f"?des={selected_comet}"
                    f"&format=json"
                    f"&from_date={start_date} 00:00"
                    f"&page={page}"
                    f"&exclude_faint=False"
                    f"&exclude_not_accurate=False"
                )
            r = requests.get(url, timeout=15)
            data = r.json()
            
            if not data.get("objects"):
                break  # no hay más resultados
        
            all_obs.extend(data["objects"])
            page += 1
    
            
        if not all_obs:
            return pd.DataFrame(columns=["obsTime", "Magn_obs"])
        
        df_pandas = pd.DataFrame(all_obs)
            
        
        # Limpieza
        df_pandas["obsTime"] = pd.to_datetime(df_pandas["obs_date"], errors="coerce", utc=True)
        df_pandas["Magn_obs"] = pd.to_numeric(df_pandas["magnitude"], errors="coerce")
    
        df_polars = pl.from_pandas(df_pandas[["obsTime", "Magn_obs"]].dropna())

        #Se restringe al rango de fechas especifico
        df = df_polars.filter((pl.col('obsTime') >= pl.lit(start_date).str.strptime(df_polars.schema['obsTime'], strict=False)) &
                           (pl.col('obsTime') <= pl.lit(end_date).str.strptime(df_polars.schema['obsTime'], strict=False)))
        
        return df


    
    def days_to_perihelion(self, df, selected_object):
        T_peri = Information(selected_object).date_perihelion()
        P = float(Information(selected_object).orbital_period())*365.25
        df = df.with_columns([((((pl.col("obsTime") - T_peri).dt.total_seconds())/86400 + P/2) % P - P/2).alias("t-Tq")])
        return df

    def reduced_magnitude(self, df):
        df = df.with_columns((pl.col("Magn_obs").cast(pl.Float64) - 5 * np.log10(pl.col("r") * pl.col("Delta"))).alias("Magn_redu"))
        return df

    def organization_df(self, df):
    
        df = df.with_columns([
                # Año, mes, día fraccionado
                pl.col("obsTime").dt.year().alias("Anio"),
                pl.col("obsTime").dt.month().alias("Mes"),
                (
                    pl.col("obsTime").dt.day()
                    + pl.col("obsTime").dt.hour() / 24.0
                    + pl.col("obsTime").dt.minute() / (24.0 * 60.0)
                    + pl.col("obsTime").dt.second() / (24.0 * 60.0 * 60.0)
                    + pl.col("obsTime").dt.microsecond() / (24.0 * 60.0 * 60.0 * 1_000_000.0)
                ).round(2).alias("Dia"),
    
                # Redondeos de las otras columnas
                pl.col("t-Tq").round(2),
                pl.col("Delta").round(2),
                pl.col("r").round(2),
                pl.col("Fase").round(2),
                pl.col("Magn_obs").cast(pl.Float64).round(2),
                pl.col("Magn_redu").round(2)
            ]).drop(["obsTime","mag","band","corr","Date"])  # Eliminamos la columna original
        
        df = df.select(["Anio", "Mes", "Dia", "t-Tq", "Delta", "r", "Fase", "Magn_obs", "Magn_redu"])
        return df    

    def datos_SLC(self, selected_object,start_date, end_date, object_type):
        df_obs = self.observations_MPC_clean(selected_object, start_date, end_date)
        df_eph = self.get_ephemerides(selected_object,start_date, end_date, object_type)

        # 1. Convertir las columnas datetime a solo fecha
        df_obs = df_obs.with_columns(
            pl.col("obsTime").dt.date().alias("Date")
        )
        
        df_eph = df_eph.with_columns(
            pl.col("Date").dt.date().alias("Date")
        )
        
        # 2. Hacer el join usando la nueva columna "Date"
        df_join = df_obs.join(df_eph, on="Date", how="inner")

        df = self.organization_df(self.reduced_magnitude(self.days_to_perihelion(df_join,selected_object))) 
        return df   
