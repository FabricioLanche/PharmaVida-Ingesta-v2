# PostgreSQL Ingesta Script
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema para PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos necesarios
COPY requirements.txt .
COPY ingesta_postgresql.py .
COPY s3_uploader.py .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Comando para ejecutar el script
CMD ["python", "ingesta_postgresql.py"]