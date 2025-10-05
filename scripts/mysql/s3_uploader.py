import boto3
import os
from datetime import datetime
import io


class S3Uploader:
    def __init__(self):
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.s3_client = boto3.client('s3', region_name=self.region)

    def upload_csv(self, csv_content: str, database_name: str, table_name: str) -> str:
        """
        Sube un CSV al bucket S3 organizado por carpetas de base de datos

        Args:
            csv_content: Contenido del CSV como string
            database_name: Nombre de la base de datos (mongodb, postgresql, mysql)
            table_name: Nombre de la tabla o colección

        Returns:
            URL del archivo subido
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{database_name}/{table_name}_{timestamp}.csv"

        csv_buffer = io.BytesIO(csv_content.encode('utf-8'))

        self.s3_client.upload_fileobj(
            csv_buffer,
            self.bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'text/csv'}
        )

        s3_url = f"s3://{self.bucket_name}/{s3_key}"
        return s3_url

    def upload_dataframe(self, df, database_name: str, table_name: str) -> str:
        """
        Sube un DataFrame de pandas como CSV al bucket S3

        Args:
            df: DataFrame de pandas
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla o colección

        Returns:
            URL del archivo subido
        """
        csv_content = df.to_csv(index=False)
        return self.upload_csv(csv_content, database_name, table_name)