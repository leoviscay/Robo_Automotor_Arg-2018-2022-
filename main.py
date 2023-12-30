from fastapi import FastAPI, Path, HTTPException, Query
from fastapi.responses import HTMLResponse
import asyncio
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import pyarrow.parquet as pq
import os



# Se instancia la aplicación
app = FastAPI(title="Sistema de Consultas de Robos de Automóviles")

######################################### CARGA DE DATOS ############################################

# Ruta del archivo Parquet 
parquet_file_path = 'data/data_parquet.parquet'

try:
    data_parquet = pd.read_parquet(parquet_file_path)
    

except FileNotFoundError:
    # Si el archivo no se encuentra, maneja la excepción
    raise HTTPException(status_code=500, detail="Error al cargar el archivo de datos Parquet")


######################################### FUNCIONES #############################################

# Define la función para obtener los 3 vehículos más robados en un año específico
@app.get('/top_vehiculos_robados_por_anio/{anio}')
def obtener_top_vehiculos_robados_por_anio(anio: int):
    try:
        if anio < 2018 or anio > 2022:
            raise HTTPException(status_code=400, detail="El año debe estar entre 2018 y 2022")

        # Resto de la lógica de la función
        data_anio = data_parquet[data_parquet['Anio_tramite'] == anio]
        vehiculos_robados = data_anio['automotor_modelo_descripcion'].value_counts()
        top_3_vehiculos_robados = vehiculos_robados.head(3).index.tolist()

        return top_3_vehiculos_robados
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servidor: {str(e)}")

@app.get('/registro_seccional_mas_tramites_po_anio/{anio}')
def registro_seccional_mas_tramites_por_anio(anio: int):
    try:
        if anio < 2018 or anio > 2022:
            raise HTTPException(status_code=400, detail="El año debe estar entre 2018 y 2022")
        
        # Filtra datos por el año dado
        data_anio = data_parquet[data_parquet['Anio_tramite'] == anio]

        # Verifica si hay datos para el año dado
        if data_anio.empty:
            raise HTTPException(status_code=404, detail="No hay datos para el año especificado")

        # Encuentra la descripción del registro seccional con más trámites
        registro_mas_tramites = data_anio['registro_seccional_descripcion'].value_counts().idxmax()

        return registro_mas_tramites
    except HTTPException:
        raise  # Re-levanta excepciones HTTP para mantener su comportamiento original
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servidor: {str(e)}")


@app.get('/autos_robados_por_genero_y_anio/{anio}')
def autos_robados_por_genero_y_anio(anio: int):
    try:
        if anio < 2018 or anio > 2022:
            raise HTTPException(status_code=400, detail="El año debe estar entre 2018 y 2022")
        
        # Filtra datos por el año dado
        data_anio = data_parquet[data_parquet['Anio_tramite'] == anio]

        # Verifica si hay datos para el año dado
        if data_anio.empty:
            raise HTTPException(status_code=404, detail="No hay datos para el año especificado")

        # Conteo de autos robados por género
        conteo_genero = data_anio['titular_genero'].value_counts()

        # Convierte el conteo a un diccionario de Python
        conteo_genero_dict = conteo_genero.to_dict()

        # Construye el resultado
        resultado = {
            'Femenino': conteo_genero_dict.get('F', 0),
            'Masculino': conteo_genero_dict.get('M', 0),
            'Otro': conteo_genero_dict.get('Otro', 0)
        }

        return resultado
    except HTTPException:
        raise  # Re-levanta excepciones HTTP para mantener su comportamiento original
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servidor: {str(e)}")


    
@app.get('/recomendacion_menos_robados/{automotor_marca_descripcion}')
def recomendacion_menos_robados(automotor_marca_descripcion: str):

    """
    Genera una recomendación de los 3 vehículos menos robados por marca en todos los años.

    Args:
    - marca (str): Marca del automóvil para obtener recomendaciones.

    Returns:
    list[RecomendacionResponse]: Lista de objetos Pydantic con información sobre los vehículos menos robados.
          Cada objeto tiene las siguientes propiedades:
          - automotor_marca_descripcion: Descripción de la marca del automóvil.
          - automotor_modelo_descripcion: Descripción del modelo del automóvil.
          - frecuencia_robos: Número de robos registrados para el modelo y marca.
    """
    try:
    
        # Filtrar datos por la marca proporcionada
        data_marca = data_parquet[data_parquet['automotor_marca_descripcion'] == automotor_marca_descripcion]

        # Agrupar datos por marca y modelo y contar la frecuencia de robos
        frecuencia_robos = data_marca.groupby(['automotor_marca_descripcion', 'automotor_modelo_descripcion']).size().reset_index(name='frecuencia_robos')

        # Ordenar por frecuencia de robos de menor a mayor
        frecuencia_robos = frecuencia_robos.sort_values(by='frecuencia_robos')

        # Seleccionar los 3 modelos menos robados
        recomendacion = frecuencia_robos.head(3)

        # Construir el resultado
        resultado = recomendacion[['automotor_marca_descripcion', 'automotor_modelo_descripcion', 'frecuencia_robos']].to_dict(orient='records')

        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servidor: {str(e)}")



def presentacion():
    return '''
        <html>
            <head>
                <title>Consultas VH Robados 2018 hasta 2022</title>
                <style>
                    body {
                        color: white; 
                        background-color: black; 
                        font-family: Arial, sans-serif;
                        padding: 20px;
                    }
                    h1 {
                        color: #333;
                        text-align: center;
                    }
                    p {
                        color: #666;
                        text-align: center;
                        font-size: 18px;
                        margin-top: 20px;
                    }
                    footer {
                        text-align: center;
                    }
                </style>
            </head>
            <body>
                <h1>API de consulta de robo de vehiculos en Argentina</h1>
                <p>Bienvenido a la API de consultas de Robos de Vehiculos en Argentina, desde 2018 hasta 2022 inclusive.</p>
                
                <p>INSTRUCCIONES:</p>
                <p>Escriba <span style="background-color: lightgray;">/docs</span> a continuación de la URL actual de esta página para interactuar con la API</p>

                <footer> Autor: Leonel Viscay </footer>

            </body>
        </html>
    '''



############################################ RUTAS ############################################

    

# Página de inicio
@app.get(path="/", response_class=HTMLResponse, tags=["Home"])
def home():
    return presentacion()

# Consultas Generales

@app.get("/top_vehiculos_robados_por_anio/{anio}", tags=["Consultas Generales"])
def top_vehiculos_robados(anio: int = Path(..., description="Año en formato número, XXXX")):
    return obtener_top_vehiculos_robados_por_anio(anio)

@app.get("/registro_mas_tramites_por_anio/{anio}", tags=["Consultas Generales"])
def registro_seccional_mas_tramites_por_anio(anio: int = Path(..., description="Año en formato número, XXXX")):
    return registro_seccional_mas_tramites_por_anio(anio)

@app.get("/autos_robados_por_genero_y_anio/{anio}", tags=["Consultas Generales"])
def obtener_autos_robados_por_genero_y_anio(anio: int = Path(..., description="Año en formato número, XXXX")):
    return autos_robados_por_genero_y_anio(anio)

@app.get("/recomendacion_menos_robados/{automotor_marca_descripcion}", tags=["Consultas Generales"])
def recomendacion_menos_robados_(automotor_marca_descripcion: str = Path(..., description="Marca del automóvil para obtener recomendaciones,marca en MAYUSCULA")):
    return recomendacion_menos_robados(automotor_marca_descripcion)
