import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from s3_uploader import S3Uploader
import json


def get_postgresql_connection():
    """Establece conexión con PostgreSQL"""
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT", 5432))
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DATABASE")

    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(postgres_url)
    return engine


def table_exists(engine, table_name):
    """Verifica si una tabla existe"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def extract_productos(engine):
    """Extrae datos de la tabla productos"""
    if not table_exists(engine, 'productos'):
        raise ValueError("La tabla 'productos' no existe en PostgreSQL")

    query = text("SELECT * FROM productos")
    df = pd.read_sql(query, engine)
    return df


def extract_ofertas(engine):
    """Extrae datos de las tablas ofertas y ofertas_detalle con JOIN"""
    if not table_exists(engine, 'ofertas'):
        raise ValueError("La tabla 'ofertas' no existe en PostgreSQL")

    if not table_exists(engine, 'ofertas_detalle'):
        raise ValueError("La tabla 'ofertas_detalle' no existe en PostgreSQL")

    query = text("""
        SELECT 
            o.id as oferta_id,
            o.fecha_vencimiento,
            o.fecha_creacion,
            o.fecha_actualizacion,
            od.id as detalle_id,
            od.producto_id,
            od.descuento
        FROM ofertas o
        LEFT JOIN ofertas_detalle od ON o.id = od.oferta_id
        ORDER BY o.id, od.id
    """)

    df = pd.read_sql(query, engine)
    return df


def main():
    """Función principal"""
    try:
        # Conectar a PostgreSQL
        engine = get_postgresql_connection()

        # Inicializar uploader S3
        s3_uploader = S3Uploader()

        resultados = {}

        # Extraer y subir productos
        try:
            df_productos = extract_productos(engine)
            url_productos = s3_uploader.upload_dataframe(df_productos, 'postgresql', 'productos')
            resultados['productos'] = {
                'url': url_productos,
                'registros': len(df_productos)
            }
        except Exception as e:
            resultados['productos'] = {
                'error': str(e)
            }

        # Extraer y subir ofertas
        try:
            df_ofertas = extract_ofertas(engine)
            url_ofertas = s3_uploader.upload_dataframe(df_ofertas, 'postgresql', 'ofertas_completo')
            resultados['ofertas'] = {
                'url': url_ofertas,
                'registros': len(df_ofertas)
            }
        except Exception as e:
            resultados['ofertas'] = {
                'error': str(e)
            }

        # Cerrar conexión
        engine.dispose()

        # Imprimir resultado en JSON para que el orquestador lo capture
        print(json.dumps(resultados))
        sys.exit(0)

    except Exception as e:
        error_result = {
            'error': f"Error general en script PostgreSQL: {str(e)}"
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()