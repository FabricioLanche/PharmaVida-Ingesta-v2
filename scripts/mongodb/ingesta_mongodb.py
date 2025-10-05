import os
import sys
import pandas as pd
from pymongo import MongoClient
from s3_uploader import S3Uploader
import json


def get_mongo_connection():
    """Establece conexión con MongoDB"""
    host = os.getenv("MONGO_HOST")
    port = int(os.getenv("MONGO_PORT", 27017))
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    database = os.getenv("MONGO_DATABASE")

    mongo_url = f"mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin"
    client = MongoClient(mongo_url)
    return client[database]


def collection_exists(db, collection_name):
    """Verifica si una colección existe"""
    return collection_name in db.list_collection_names()


def extract_medicos(db):
    """Extrae datos de la colección medicos"""
    if not collection_exists(db, 'medicos'):
        raise ValueError("La colección 'medicos' no existe en MongoDB")

    medicos = list(db.medicos.find())

    if not medicos:
        return pd.DataFrame(
            columns=['_id', 'cmp', 'nombre', 'especialidad', 'colegiaturaValida', 'createdAt', 'updatedAt'])

    df = pd.DataFrame(medicos)
    df['_id'] = df['_id'].astype(str)

    return df


def extract_recetas(db):
    """Extrae datos de la colección recetas"""
    if not collection_exists(db, 'recetas'):
        raise ValueError("La colección 'recetas' no existe en MongoDB")

    recetas = list(db.recetas.find())

    if not recetas:
        return pd.DataFrame(columns=['_id', 'pacienteDNI', 'medicoCMP', 'fechaEmision',
                                     'productos', 'archivoPDF', 'estadoValidacion', 'createdAt', 'updatedAt'])

    df = pd.DataFrame(recetas)
    df['_id'] = df['_id'].astype(str)
    df['productos'] = df['productos'].apply(lambda x: str(x) if isinstance(x, list) else x)

    return df


def main():
    """Función principal"""
    try:
        # Conectar a MongoDB
        db = get_mongo_connection()

        # Inicializar uploader S3
        s3_uploader = S3Uploader()

        resultados = {}

        # Extraer y subir medicos
        try:
            df_medicos = extract_medicos(db)
            url_medicos = s3_uploader.upload_dataframe(df_medicos, 'mongodb', 'medicos')
            resultados['medicos'] = {
                'url': url_medicos,
                'registros': len(df_medicos)
            }
        except Exception as e:
            resultados['medicos'] = {
                'error': str(e)
            }

        # Extraer y subir recetas
        try:
            df_recetas = extract_recetas(db)
            url_recetas = s3_uploader.upload_dataframe(df_recetas, 'mongodb', 'recetas')
            resultados['recetas'] = {
                'url': url_recetas,
                'registros': len(df_recetas)
            }
        except Exception as e:
            resultados['recetas'] = {
                'error': str(e)
            }

        # Imprimir resultado en JSON para que el orquestador lo capture
        print(json.dumps(resultados))
        sys.exit(0)

    except Exception as e:
        error_result = {
            'error': f"Error general en script MongoDB: {str(e)}"
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()