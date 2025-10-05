#!/bin/bash

# Script para construir todas las imágenes de los scripts

set -e  # Salir si hay algún error

echo "🔨 Construyendo imágenes de scripts de ingesta..."
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -d "scripts" ]; then
    echo "❌ Error: No se encuentra el directorio 'scripts'"
    echo "Ejecuta este script desde la raíz del proyecto backend-ingesta"
    exit 1
fi

# Construir imagen MongoDB
echo "📦 Construyendo imagen MongoDB..."
docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb
if [ $? -eq 0 ]; then
    echo "✅ Imagen MongoDB construida exitosamente"
else
    echo "❌ Error construyendo imagen MongoDB"
    exit 1
fi
echo ""

# Construir imagen MySQL
echo "📦 Construyendo imagen MySQL..."
docker build -t pharmavida-ingesta-mysql:latest ./scripts/mysql
if [ $? -eq 0 ]; then
    echo "✅ Imagen MySQL construida exitosamente"
else
    echo "❌ Error construyendo imagen MySQL"
    exit 1
fi
echo ""

# Construir imagen PostgreSQL
echo "📦 Construyendo imagen PostgreSQL..."
docker build -t pharmavida-ingesta-postgresql:latest ./scripts/postgresql
if [ $? -eq 0 ]; then
    echo "✅ Imagen PostgreSQL construida exitosamente"
else
    echo "❌ Error construyendo imagen PostgreSQL"
    exit 1
fi
echo ""

echo "✅ Todas las imágenes han sido construidas exitosamente!"
echo ""
echo "📋 Imágenes disponibles:"
docker images | grep pharmavida-ingesta
echo ""
echo "🚀 Ahora puedes levantar el API Gateway con:"
echo "   docker-compose up -d"