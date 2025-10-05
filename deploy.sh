#!/bin/bash

# Script de despliegue completo para Backend de Ingesta

set -e  # Salir si hay algún error

echo "🚀 Iniciando despliegue del Backend de Ingesta..."
echo ""

# 1. Verificar que estamos en el directorio correcto
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Error: No se encuentra docker-compose.yml"
    echo "Ejecuta este script desde la raíz del proyecto backend-ingesta"
    exit 1
fi

# 2. Verificar archivo .env
if [ ! -f ".env" ]; then
    echo "⚠️  Advertencia: No se encuentra archivo .env"
    echo "📝 Copiando .env.example a .env..."
    cp .env.example .env
    echo "⚠️  IMPORTANTE: Edita el archivo .env con tus configuraciones antes de continuar"
    echo "   nano .env"
    exit 1
fi

echo "✅ Archivo .env encontrado"
echo ""

# 3. Verificar credenciales AWS
if [ ! -f ~/.aws/credentials ]; then
    echo "❌ Error: No se encuentran credenciales AWS en ~/.aws/credentials"
    echo "📝 Crea el archivo con:"
    echo "   mkdir -p ~/.aws"
    echo "   nano ~/.aws/credentials"
    echo ""
    echo "Contenido del archivo:"
    echo "[default]"
    echo "aws_access_key_id = YOUR_ACCESS_KEY"
    echo "aws_secret_access_key = YOUR_SECRET_KEY"
    exit 1
fi

echo "✅ Credenciales AWS encontradas"
echo ""

# 4. Detener contenedores previos
echo "🛑 Deteniendo contenedores previos..."
docker compose down 2>/dev/null || true
echo ""

# 5. Construir imágenes de scripts
echo "🔨 Construyendo imágenes de scripts..."
if [ -f "build-images.sh" ]; then
    chmod +x build-images.sh
    ./build-images.sh
else
    echo "⚠️  No se encuentra build-images.sh, construyendo manualmente..."
    docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb
    docker build -t pharmavida-ingesta-mysql:latest ./scripts/mysql
    docker build -t pharmavida-ingesta-postgresql:latest ./scripts/postgresql
fi
echo ""

# 6. Construir y levantar API Gateway
echo "🏗️  Construyendo API Gateway..."
docker compose build --no-cache
echo ""

echo "🚀 Levantando API Gateway..."
docker compose up -d
echo ""

# 7. Esperar que el servicio esté listo
echo "⏳ Esperando que el servicio esté listo..."
sleep 5

# 8. Verificar estado
echo "🔍 Verificando estado del servicio..."
if docker compose ps | grep -q "Up"; then
    echo "✅ API Gateway está corriendo"
    echo ""

    # Test de health check
    echo "🏥 Probando health check..."
    if curl -s http://localhost:8000/health | grep -q "healthy"; then
        echo "✅ Health check exitoso"
    else
        echo "⚠️  Health check falló, verificando logs..."
        docker compose logs --tail=20
    fi
else
    echo "❌ Error: El contenedor no está corriendo"
    echo "📋 Logs:"
    docker compose logs -f
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ¡Despliegue completado exitosamente!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Información del servicio:"
echo "   - API Gateway: http://localhost:8000"
echo "   - Documentación: http://localhost:8000/docs"
echo "   - Health Check: http://localhost:8000/health"
echo ""
echo "📦 Imágenes construidas:"
docker images | grep pharmavida-ingesta
echo ""
echo "🐳 Contenedores activos:"
docker compose ps
echo ""
echo "🧪 Prueba los endpoints:"
echo "   curl -X POST http://localhost:8000/api/ingesta/mongodb"
echo "   curl -X POST http://localhost:8000/api/ingesta/mysql"
echo "   curl -X POST http://localhost:8000/api/ingesta/postgresql"
echo ""
echo "📊 Ver logs:"
echo "   docker compose logs -f"
echo ""
echo "🛑 Detener servicio:"
echo "   docker compose down"
echo ""