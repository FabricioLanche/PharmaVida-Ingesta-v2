# MySQL Ingesta Script
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema para MySQL
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos necesarios
COPY requirements.txt .
COPY ingesta_mysql.py .
COPY s3_uploader.py .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Comando para ejecutar el script
CMD ["python", "ingesta_mysql.py"]