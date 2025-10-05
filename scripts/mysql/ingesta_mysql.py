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


def extract_users(engine):
    """Extrae datos de la tabla users"""
    if not table_exists(engine, 'users'):
        raise ValueError("La tabla 'users' no existe en MySQL")

    query = text("SELECT * FROM users")
    df = pd.read_sql(query, engine)

    # Remover la columna password por seguridad
    if 'password' in df.columns:
        df = df.drop(columns=['password'])

    return df


def extract_compras(engine):
    """Extrae datos de la tabla compras"""
    if not table_exists(engine, 'compras'):
        raise ValueError("La tabla 'compras' no existe en MySQL")

    # Verificar si existen las tablas relacionadas
    has_productos = table_exists(engine, 'compra_productos')
    has_cantidades = table_exists(engine, 'compra_cantidades')

    if has_productos and has_cantidades:
        query = text("""
            SELECT 
                c.id,
                c.fecha_compra,
                c.usuario_id,
                GROUP_CONCAT(DISTINCT cp.producto_id ORDER BY cp.producto_id) as productos,
                GROUP_CONCAT(DISTINCT cc.cantidad ORDER BY cc.cantidad) as cantidades
            FROM compras c
            LEFT JOIN compra_productos cp ON c.id = cp.compra_id
            LEFT JOIN compra_cantidades cc ON c.id = cc.compra_id
            GROUP BY c.id, c.fecha_compra, c.usuario_id
            ORDER BY c.id
        """)
    else:
        query = text("SELECT * FROM compras")

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

        # Extraer y subir users
        try:
            df_users = extract_users(engine)
            url_users = s3_uploader.upload_dataframe(df_users, 'mysql', 'users')
            resultados['users'] = {
                'url': url_users,
                'registros': len(df_users)
            }
        except Exception as e:
            resultados['users'] = {
                'error': str(e)
            }

        # Extraer y subir compras
        try:
            df_compras = extract_compras(engine)
            url_compras = s3_uploader.upload_dataframe(df_compras, 'mysql', 'compras')
            resultados['compras'] = {
                'url': url_compras,
                'registros': len(df_compras)
            }
        except Exception as e:
            resultados['compras'] = {
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