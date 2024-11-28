from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import JSONResponse

# Configurar FastAPI
app = FastAPI(title="Giros Salud API", description="API para analizar giros a EPS e IPS", version="1.0")

# Conectar con SQLite
DATABASE_URL = "sqlite:///giros_salud.sqlite"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


# Modelo de respuesta para los datos (opcional, por ahora simplificado)
class GiroResponse(BaseModel):
    nombre_eps: str
    total_giro: float


# Endpoint inicial de prueba
@app.get("/")
def root():
    return {"message": "¡Bienvenido a la API de Giros de Salud!"}

@app.exception_handler(Exception)
async def exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"message": f"Error interno: {str(exc)}"},
    )


@app.get("/api/distribucion-tipo-contratacion")
async def distribucion_tipo_contratacion():
    query = """
        SELECT tipo_contratacion, SUM(total_giro) AS total_giro
        FROM (
            SELECT 'Capitación' AS tipo_contratacion, total_giro FROM giro_directo_capitacion
            UNION ALL
            SELECT 'Evento' AS tipo_contratacion, total_giro FROM giro_directo_evento
        ) AS combined
        GROUP BY tipo_contratacion;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()
    return [{"tipo_contratacion": row[0], "total_giro": row[1]} for row in result]


@app.get("/api/tendencia-mensual")
async def tendencia_mensual():
    query = """
        SELECT 
            COALESCE(mes, 'Desconocido') AS mes, 
            tipo_contratacion, 
            COALESCE(SUM(total_giro), 0) AS total_giro
        FROM (
            SELECT mes, total_giro, 'Capitación' AS tipo_contratacion 
            FROM giro_directo_capitacion WHERE ao = '2024'
            UNION ALL
            SELECT mes, total_giro, 'Evento' AS tipo_contratacion 
            FROM giro_directo_evento WHERE ao = '2024'
        )
        GROUP BY mes, tipo_contratacion
        ORDER BY
            CASE
                WHEN mes = 'enero' THEN 1
                WHEN mes = 'febrero' THEN 2
                WHEN mes = 'marzo' THEN 3
                WHEN mes = 'abril' THEN 4
                WHEN mes = 'mayo' THEN 5
                WHEN mes = 'junio' THEN 6
                WHEN mes = 'julio' THEN 7
                WHEN mes = 'agosto' THEN 8
                WHEN mes = 'septiembre' THEN 9
                WHEN mes = 'octubre' THEN 10
                WHEN mes = 'noviembre' THEN 11
                WHEN mes = 'diciembre' THEN 12
                ELSE 13 -- Valores desconocidos se colocan al final
            END;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query)).fetchall()
        
        # Convertir las tuplas en diccionarios
        data = [{"mes": row[0], "tipo_contratacion": row[1], "total_giro": row[2]} for row in result]
        return data
    except Exception as e:
        return {"message": f"Error interno: {str(e)}"}
    

# Endpoint para obtener la distribución por EPS
@app.get("/api/distribucion-por-eps")
async def distribucion_por_eps():
    query = """
        SELECT nombre_eps, SUM(total_giro) AS total_giro
        FROM giro_directo_capitacion
        GROUP BY nombre_eps
        ORDER BY total_giro DESC;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()
    
    # Convertir el resultado de tuplas a diccionarios
    result_dict = [{"nombre_eps": row[0], "total_giro": row[1]} for row in result]

    return result_dict

@app.get("/api/giros-por-departamento-y-municipio")
async def giros_por_departamento_y_municipio():
    query = """
        SELECT departamento, municipio, SUM(total_giro) AS total_giro
        FROM giro_directo_capitacion
        GROUP BY departamento, municipio
        ORDER BY total_giro DESC;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()

    # Convirtiendo las tuplas a un formato más adecuado
    return [{"departamento": row[0], "municipio": row[1], "total_giro": row[2]} for row in result]


@app.get("/api/recursos-totales-por-ips")
async def recursos_totales_por_ips():
    query = """
        SELECT nombre_ips, SUM(total_giro) AS total_giro
        FROM giro_directo_capitacion
        GROUP BY nombre_ips
        ORDER BY total_giro DESC
        LIMIT 15;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()

    return [{"nombre_ips": row[0], "total_giro": row[1]} for row in result]


@app.get("/api/distribucion-por-eps-y-ips")
async def distribucion_por_eps_y_ips():
    query = """
        SELECT nombre_eps, nombre_ips, SUM(total_giro) AS total_giro
        FROM giro_directo_capitacion
        GROUP BY nombre_eps, nombre_ips
        ORDER BY nombre_eps, total_giro DESC
        LIMIT 10;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query)).fetchall()

    return [{"nombre_eps": row[0], "nombre_ips": row[1], "total_giro": row[2]} for row in result]






    




