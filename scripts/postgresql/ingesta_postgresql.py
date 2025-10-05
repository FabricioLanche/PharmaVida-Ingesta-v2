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


def extract_usuarios(engine):
    """Extrae datos de la tabla users (sin password)"""
    if not table_exists(engine, 'users'):
        raise ValueError("La tabla 'users' no existe en PostgreSQL")

    query = text("""
        SELECT id, dni, apellido, distrito, email, nombre, role
        FROM users 
        ORDER BY id
    """)
    df = pd.read_sql(query, engine)
    return df


def extract_compras(engine):
    """Extrae datos de la tabla compras"""
    if not table_exists(engine, 'compras'):
        raise ValueError("La tabla 'compras' no existe en PostgreSQL")

    query = text("SELECT * FROM compras ORDER BY id")
    df = pd.read_sql(query, engine)
    return df


def extract_compra_productos(engine):
    """Extrae datos de la tabla compra_productos"""
    if not table_exists(engine, 'compra_productos'):
        raise ValueError("La tabla 'compra_productos' no existe en PostgreSQL")

    query = text("SELECT * FROM compra_productos ORDER BY compra_id")
    df = pd.read_sql(query, engine)
    return df


def extract_compra_cantidades(engine):
    """Extrae datos de la tabla compra_cantidades"""
    if not table_exists(engine, 'compra_cantidades'):
        raise ValueError("La tabla 'compra_cantidades' no existe en PostgreSQL")

    query = text("SELECT * FROM compra_cantidades ORDER BY compra_id")
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

        # Extraer y subir usuarios
        try:
            df_usuarios = extract_usuarios(engine)
            url_usuarios = s3_uploader.upload_dataframe(df_usuarios, 'usuarios', 'usuarios')
            resultados['usuarios'] = {
                'url': url_usuarios,
                'registros': len(df_usuarios),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['usuarios'] = {
                'error': str(e)
            }

        # Extraer y subir compras
        try:
            df_compras = extract_compras(engine)
            url_compras = s3_uploader.upload_dataframe(df_compras, 'compras', 'compras')
            resultados['compras'] = {
                'url': url_compras,
                'registros': len(df_compras),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['compras'] = {
                'error': str(e)
            }

        # Extraer y subir compra_productos
        try:
            df_compra_productos = extract_compra_productos(engine)
            url_compra_productos = s3_uploader.upload_dataframe(df_compra_productos, 'compra_productos',
                                                                'compra_productos')
            resultados['compra_productos'] = {
                'url': url_compra_productos,
                'registros': len(df_compra_productos),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['compra_productos'] = {
                'error': str(e)
            }

        # Extraer y subir compra_cantidades
        try:
            df_compra_cantidades = extract_compra_cantidades(engine)
            url_compra_cantidades = s3_uploader.upload_dataframe(df_compra_cantidades, 'compra_cantidades',
                                                                 'compra_cantidades')
            resultados['compra_cantidades'] = {
                'url': url_compra_cantidades,
                'registros': len(df_compra_cantidades),
                'formato': 'CSV'
            }
        except Exception as e:
            resultados['compra_cantidades'] = {
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