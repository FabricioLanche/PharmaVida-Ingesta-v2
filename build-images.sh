#!/bin/bash

# Script para construir todas las imágenes de los scripts

echo "🔨 Construyendo imágenes de scripts de ingesta..."

# Construir imagen MongoDB
echo "📦 Construyendo imagen MongoDB..."
docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb

# Construir imagen MySQL
echo "📦 Construyendo imagen MySQL..."
docker build -t pharmavida-ingesta-mysql:latest ./scripts/mysql

# Construir imagen PostgreSQL
echo "📦 Construyendo imagen PostgreSQL..."
docker build -t pharmavida-ingesta-postgresql:latest ./scripts/postgresql

echo "✅ Todas las imágenes han sido construidas exitosamente!"
echo ""
echo "Imágenes disponibles:"
docker images | grep pharmavida-ingesta