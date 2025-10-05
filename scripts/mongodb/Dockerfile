# MongoDB Ingesta Script
FROM python:3.11-slim

WORKDIR /app

# Copiar archivos necesarios
COPY requirements.txt .
COPY ingesta_mongodb.py .
COPY s3_uploader.py .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Comando para ejecutar el script
CMD ["python", "ingesta_mongodb.py"]