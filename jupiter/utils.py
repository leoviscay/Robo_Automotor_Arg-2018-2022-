import pandas as pd
from textblob import TextBlob

def types_df(df):
    '''
    Analiza de los tipos de datos en un DataFrame y devuelve un resumen que incluye información sobre
    los tipos de datos en cada columna, el porcentaje de valores no nulos y nulos, así como la
    cantidad de valores nulos por columna.

    Parameters:
        df (pandas.DataFrame): El DataFrame que se va a analizar.

    Returns:
        pandas.DataFrame: Un DataFrame que contiene el resumen de cada columna, incluyendo:
        - 'nombre_campo': Nombre de cada columna.
        - 'tipo_datos': Tipos de datos únicos presentes en cada columna.
        - 'no_nulos_%': Porcentaje de valores no nulos en cada columna.
        - 'nulos_%': Porcentaje de valores nulos en cada columna.
        - 'nulos': Cantidad de valores nulos en cada columna.
    '''

    mi_dict = {"nombre_campo": [], "tipo_datos": [], "no_nulos_%": [], "nulos_%": [], "nulos": []}

    for columna in df.columns:
        porcentaje_no_nulos = (df[columna].count() / len(df)) * 100
        mi_dict["nombre_campo"].append(columna)
        mi_dict["tipo_datos"].append(df[columna].apply(type).unique())
        mi_dict["no_nulos_%"].append(round(porcentaje_no_nulos, 2))
        mi_dict["nulos_%"].append(round(100-porcentaje_no_nulos, 2))
        mi_dict["nulos"].append(df[columna].isnull().sum())

    df_info = pd.DataFrame(mi_dict)
        
    return df_info


def asignar_tipo_tramite_id(df):
    """
    Asigna un tipo_tramite_id a cada fila del DataFrame según los valores de la columna tipo_tramite.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna tipo_tramite.

    Returns:
    - DataFrame
        El DataFrame con la nueva columna tipo_tramite_id.
    """
    # Mapeo de valores de tipo_tramite a tipo_tramite_id
    mapeo_tramite_tipo = {
        'DENUNCIA DE ROBO O HURTO': 1,
        'COMUNICACIÓN DE RECUPERO': 2,
        'DENUNCIA DE ROBO O HURTO / RETENCION INDEBIDA': 3
    }

    # Crear la nueva columna tipo_tramite_id
    df['tramite_id'] = df['tramite_tipo'].map(mapeo_tramite_tipo)

    return df

import pandas as pd

def extraer_anio(df, columna_fecha='tramite_fecha', nueva_columna_anio='Anio_tramite'):
    """
    Extrae el año de una columna de fechas y lo asigna a una nueva columna.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna de fechas.
    - columna_fecha: str, optional
        El nombre de la columna que contiene fechas (por defecto 'tramite_fecha').
    - nueva_columna_anio: str, optional
        El nombre de la nueva columna para almacenar los años (por defecto 'Anio_tramite').

    Returns:
    - DataFrame
        El DataFrame con la nueva columna de años.
    """
    df[nueva_columna_anio] = pd.to_datetime(df[columna_fecha]).dt.year
    return df

# Ejemplo de uso
# df_data = pd.read_csv('ruta/del/archivo/data.csv')
# df_data = extraer_anio(df_data, 'tramite_fecha')




def extraer_mes(df, columna_fecha='tramite_fecha', nueva_columna_mes='Mes_tramite'):
    """
    Extrae el mes de una columna de fechas y lo asigna a una nueva columna.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna de fechas.
    - columna_fecha: str, optional
        El nombre de la columna que contiene fechas (por defecto 'tramite_fecha').
    - nueva_columna_mes: str, optional
        El nombre de la nueva columna para almacenar los meses (por defecto 'Mes_tramite').

    Returns:
    - DataFrame
        El DataFrame con la nueva columna de meses.
    """
    df[nueva_columna_mes] = pd.to_datetime(df[columna_fecha]).dt.month
    return df

# Ejemplo de uso
# df_data = pd.read_csv('ruta/del/archivo/data.csv')
# df_data = extraer_mes(df_data, 'tramite_fecha')

def mapear_origen(df, columna_origen='automotor_origen'):
    """
    Mapea los valores de la columna 'automotor_origen' a 'N', 'I', 'P'.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna 'automotor_origen'.
    - columna_origen: str, optional
        El nombre de la columna 'automotor_origen' (por defecto 'automotor_origen').

    Returns:
    - DataFrame
        El DataFrame con la columna 'automotor_origen' mapeada.
    """
    mapeo_origen = {
        'Nacional': 'N',
        'Importado': 'I',
        'Protocolo 21': 'P'
    }

    df[columna_origen] = df[columna_origen].map(mapeo_origen)
    return df

def mapear_tipo_persona(df, columna_origen='titular_tipo_persona'):
    """
    Mapea los valores de la columna 'titular_tipo_persona' a 'F', 'J'.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna 'titular_tipo_persona'.
    - columna_origen: str, optional
        El nombre de la columna 'titular_tipo_persona' (por defecto 'titular_tipo_persona').

    Returns:
    - DataFrame
        El DataFrame con la columna 'titular_tipo_persona' mapeada.
    """
    mapeo_origen = {
        'Física': 'F',
        'Jurídica': 'J',
        
    }

    df[columna_origen] = df[columna_origen].map(mapeo_origen)
    return df

def mapear_genero(df, columna_origen='titular_genero'):
    """
    Mapea los valores de la columna 'titular_genero' a 'F', 'M', 'O'.

    Parameters:
    - df: DataFrame
        El DataFrame que contiene la columna 'titular_genero'.
    - columna_origen: str, optional
        El nombre de la columna 'titular_genero' (por defecto 'titular_genero').

    Returns:
    - DataFrame
        El DataFrame con la columna 'titular_genero' mapeada.
    """
    mapeo_origen = {
        'Masculino': 'M',
        'Femenino': 'F',
        'No identificado': 'O', 
        'No aplica': 'O',
        
    }

    df[columna_origen] = df[columna_origen].map(mapeo_origen)
    return df