import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from s3_uploader import S3Uploader
import json


def get_mysql_connection():
    """Establece conexión con MySQL"""
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT", 3306))
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    mysql_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(mysql_url)
    return engine


def table_exists(engine, table_name):
    """Verifica si una tabla existe"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def extract_productos(engine):
    """Extrae datos de la tabla productos"""
    if not table_exists(engine, 'productos'):
        raise ValueError("La tabla 'productos' no existe en MySQL")

    query = text("SELECT * FROM productos ORDER BY id")
    df = pd.read_sql(query, engine)
    return df


def extract_ofertas(engine):
    """Extrae datos de la tabla ofertas"""
    if not table_exists(engine, 'ofertas'):
        raise ValueError("La tabla 'ofertas' no existe en MySQL")

    query = text("SELECT * FROM ofertas ORDER BY id")
    df = pd.read_sql(query, engine)
    return df


def extract_ofertas_detalle(engine):
    """Extrae datos de la tabla ofertas_detalle"""
    if not table_exists(engine, 'ofertas_detalle'):
        raise ValueError("La tabla 'ofertas_detalle' no existe en MySQL")

    query = text("SELECT * FROM ofertas_detalle ORDER BY id")
    df = pd.read_sql(query, engine)
    return df


def main():
    """Función principal"""
    try:
        # Conectar a MySQL
        engine = get_mysql_connection()

        # Inicializar uploader S3
        s3_uploader = S3Uploader()

        resultados = {}

        # Extraer y subir productos
        try:
            df_productos = extract_productos(engine)
            url_productos = s3_uploader.upload_dataframe(df_productos, 'productos', 'productos')
            resultados['productos'] = {
                'url': url_productos,
                'registros': len(df_productos),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['productos'] = {
                'error': str(e)
            }

        # Extraer y subir ofertas
        try:
            df_ofertas = extract_ofertas(engine)
            url_ofertas = s3_uploader.upload_dataframe(df_ofertas, 'ofertas', 'ofertas')
            resultados['ofertas'] = {
                'url': url_ofertas,
                'registros': len(df_ofertas),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['ofertas'] = {
                'error': str(e)
            }

        # Extraer y subir ofertas_detalle
        try:
            df_ofertas_detalle = extract_ofertas_detalle(engine)
            url_ofertas_detalle = s3_uploader.upload_dataframe(df_ofertas_detalle, 'ofertas_detalle', 'ofertas_detalle')
            resultados['ofertas_detalle'] = {
                'url': url_ofertas_detalle,
                'registros': len(df_ofertas_detalle),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['ofertas_detalle'] = {
                'error': str(e)
            }

        # Cerrar conexión
        engine.dispose()

        # Imprimir resultado en JSON para que el orquestador lo capture
        print(json.dumps(resultados))
        sys.exit(0)

    except Exception as e:
        error_result = {
            'error': f"Error general en script MySQL: {str(e)}"
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()