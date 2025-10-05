# Backend de Ingesta - PharmaVida (Arquitectura Refactorizada)

Backend con arquitectura de microservicios que utiliza contenedores efímeros para ejecutar scripts de ingesta de datos desde múltiples bases de datos (MongoDB, PostgreSQL y MySQL) hacia AWS S3.

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                    Cliente Externo (MV)                      │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP POST
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              API Gateway (FastAPI)                           │
│  - Recibe requests HTTP                                      │
│  - Ejecuta docker run con scripts específicos               │
│  - Retorna resultados JSON                                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
    ┌─────────┐       ┌─────────┐      ┌─────────┐
    │ Script  │       │ Script  │      │ Script  │
    │ MongoDB │       │  MySQL  │      │Postgres │
    │(efímero)│       │(efímero)│      │(efímero)│
    └─────────┘       └─────────┘      └─────────┘
```

### Ventajas de esta arquitectura:
- ✅ **Aislamiento completo**: Cada script se ejecuta en su propio contenedor
- ✅ **Eficiencia de recursos**: Contenedores de scripts solo existen durante la ejecución
- ✅ **API Gateway ligero**: ~50MB, solo orquesta, no procesa datos
- ✅ **Escalable**: Fácil agregar nuevos scripts sin modificar el gateway
- ✅ **Mantenible**: Cada script es independiente con sus propias dependencias

## 📁 Estructura del Proyecto

```
backend-ingesta/
├── api-gateway/                      # API Gateway (siempre activo)
│   ├── app/
│   │   ├── main.py                   # Aplicación FastAPI
│   │   ├── core/
│   │   │   └── config.py             # Configuración
│   │   ├── api/routes/
│   │   │   └── ingesta.py            # Endpoints
│   │   └── orchestrator/
│   │       └── docker_runner.py      # Orquestador de contenedores
│   ├── requirements.txt
│   └── Dockerfile
├── scripts/                          # Scripts de ingesta (efímeros)
│   ├── mongodb/
│   │   ├── ingesta_mongodb.py
│   │   ├── s3_uploader.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── mysql/
│   │   ├── ingesta_mysql.py
│   │   ├── s3_uploader.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── postgresql/
│       ├── ingesta_postgresql.py
│       ├── s3_uploader.py
│       ├── requirements.txt
│       └── Dockerfile
├── docker-compose.yml                # Solo levanta el API Gateway
├── build-images.sh                   # Script para construir imágenes
├── .env.example
├── .dockerignore
├── .gitignore
└── README.md
```

## 🚀 Instalación y Configuración

### 1. Configurar Variables de Entorno

```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

**Configuración de hosts**:
- Si las BD's están en el mismo host con docker-compose separado: usar nombres de contenedor (ej: `pharmavida_mongodb`)
- Si las BD's están en host remoto: usar IP o hostname
- Si las BD's están localmente sin Docker: usar `host.docker.internal` (Mac/Windows) o IP del host (Linux)

### 2. Configurar Credenciales AWS

```bash
# Asegúrate de tener configurado ~/.aws/credentials
cat ~/.aws/credentials

[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
aws_session_token = YOUR_SESSION_TOKEN  # Opcional
```

### 3. Construir Imágenes de los Scripts

```bash
# Dar permisos de ejecución al script
chmod +x build-images.sh

# Construir todas las imágenes
./build-images.sh
```

O construir manualmente:
```bash
docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb
docker build -t pharmavida-ingesta-mysql:latest ./scripts/mysql
docker build -t pharmavida-ingesta-postgresql:latest ./scripts/postgresql
```

### 4. Levantar el API Gateway

```bash
docker-compose up -d
```

### 5. Verificar que el servicio está corriendo

```bash
docker-compose ps
curl http://localhost:8000/health
```

## 📡 Endpoints Disponibles

### 1. Ingesta MongoDB
```bash
POST /api/ingesta/mongodb
```
Ejecuta script en contenedor efímero que extrae:
- **medicos**: CMP, nombre, especialidad, colegiatura válida
- **recetas**: DNI paciente, CMP médico, productos, PDF, validación

### 2. Ingesta MySQL
```bash
POST /api/ingesta/mysql
```
Ejecuta script en contenedor efímero que extrae:
- **users**: DNI, nombre, apellido, email, rol (sin password)
- **compras**: Fecha, usuario, productos, cantidades

### 3. Ingesta PostgreSQL
```bash
POST /api/ingesta/postgresql
```
Ejecuta script en contenedor efímero que extrae:
- **productos**: Nombre, tipo, precio, stock, requiere receta
- **ofertas**: Ofertas con JOIN de detalles (descuentos, productos)

### 4. Health Check
```bash
GET /api/ingesta/health
GET /health
```

## 💻 Ejemplos de Uso

### Usando curl

```bash
# Ejecutar ingesta MongoDB
curl -X POST http://localhost:8000/api/ingesta/mongodb

# Respuesta esperada:
# {
#   "status": "success",
#   "database": "mongodb",
#   "result": {
#     "medicos": {
#       "url": "s3://pharmavida-ingesta/mongodb/medicos_20250102_153045.csv",
#       "registros": 150
#     },
#     "recetas": {
#       "url": "s3://pharmavida-ingesta/mongodb/recetas_20250102_153045.csv",
#       "registros": 450
#     }
#   }
# }
```

### Usando Python

```python
import requests

response = requests.post("http://localhost:8000/api/ingesta/postgresql")
data = response.json()

if data["status"] == "success":
    print(f"Productos exportados: {data['result']['productos']['registros']}")
    print(f"URL: {data['result']['productos']['url']}")
```

### Usando Postman

1. Abrir Postman
2. Crear nueva request POST
3. URL: `http://localhost:8000/api/ingesta/mysql`
4. Click en "Send"

## 📊 Estructura de Archivos en S3

```
s3://pharmavida-ingesta/
├── mongodb/
│   ├── medicos_20250102_153045.csv
│   └── recetas_20250102_153045.csv
├── mysql/
│   ├── users_20250102_153045.csv
│   └── compras_20250102_153045.csv
└── postgresql/
    ├── productos_20250102_153045.csv
    └── ofertas_completo_20250102_153045.csv
```

## 🔧 Comandos Útiles

### Ver logs del API Gateway
```bash
docker-compose logs -f api-gateway
```

### Ver imágenes construidas
```bash
docker images | grep pharmavida-ingesta
```

### Reconstruir una imagen específica
```bash
docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb
```

### Detener el servicio
```bash
docker-compose down
```

### Ejecutar un script manualmente (debug)
```bash
docker run --rm \
  -e MONGO_HOST=pharmavida_mongodb \
  -e MONGO_PORT=27017 \
  -e MONGO_USER=admin \
  -e MONGO_PASSWORD=pharmavida123 \
  -e MONGO_DATABASE=pharmavida_db \
  -e AWS_BUCKET_NAME=pharmavida-ingesta \
  -e AWS_REGION=us-east-1 \
  --network pharmavida_network \
  pharmavida-ingesta-mongodb:latest
```

## 🔒 Seguridad

- ✅ Credenciales AWS montadas como volumen read-only
- ✅ Variables sensibles en .env (no versionado)
- ✅ Passwords de usuarios excluidos automáticamente
- ✅ Scripts ejecutan con mínimos privilegios
- ✅ Contenedores efímeros se auto-destruyen tras ejecución

## 🐛 Troubleshooting

### Error: "Imagen no encontrada"
```bash
# Construir las imágenes primero
./build-images.sh
```

### Error: "No se puede conectar a Docker daemon"
```bash
# Verificar que Docker esté corriendo
docker ps

# Asegurar que el socket está montado correctamente en docker-compose.yml
```

### Error: "No se puede conectar a la base de datos"
```bash
# Verificar la red Docker
docker network ls

# Si las BD's están en otra red, actualizar DOCKER_NETWORK en .env
# Ejemplo: pharmavida-bd-s_PharmaVida
```

### Error: "Tabla no existe"
```bash
# Verificar que las tablas existen en las bases de datos
# El script valida la existencia antes de extraer
```

### Ver logs de un contenedor efímero (antes de que se destruya)
```bash
# En docker_runner.py, cambiar remove=True a remove=False temporalmente
# Luego ver logs:
docker logs <container_id>
```

## 📚 Documentación Interactiva

FastAPI genera documentación automática:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 🔄 Flujo de Ejecución

1. Cliente hace POST a `/api/ingesta/mongodb`
2. API Gateway recibe el request
3. Gateway ejecuta `docker run` con imagen `pharmavida-ingesta-mongodb`
4. Script se conecta a MongoDB, extrae datos
5. Script sube CSVs a S3
6. Script termina y retorna JSON
7. Contenedor se auto-destruye
8. Gateway retorna resultado al cliente

## 📈 Ventajas vs Arquitectura Anterior

| Aspecto | Arquitectura Anterior | Arquitectura Refactorizada |
|---------|----------------------|---------------------------|
| Contenedores activos | 1 backend grande | 1 gateway ligero |
| Memoria en reposo | ~200MB | ~50MB |
| Aislamiento de scripts | ❌ Mismo proceso | ✅ Contenedores separados |
| Escalabilidad | ⚠️ Limitada | ✅ Excelente |
| Mantenibilidad | ⚠️ Código acoplado | ✅ Scripts independientes |
| Dependencias | ⚠️ Compartidas | ✅ Aisladas por script |

## 🌐 Conexión con Bases de Datos

### Opción 1: BD's en el mismo host (docker-compose separado)
```bash
# En .env
MONGO_HOST=pharmavida_mongodb
DOCKER_NETWORK=pharmavida-bd-s_PharmaVida  # nombre-carpeta_nombre-red
```

### Opción 2: BD's en host remoto
```bash
# En .env
MONGO_HOST=192.168.1.100
MONGO_PORT=27017
DOCKER_NETWORK=bridge
```

### Opción 3: BD's locales (sin Docker)
```bash
# En .env (Mac/Windows)
MONGO_HOST=host.docker.internal
MONGO_PORT=27017

# Linux
MONGO_HOST=172.17.0.1  # IP del host desde contenedor
```

## 🚀 Despliegue en Producción

### Consideraciones:
1. **Credenciales AWS**: Usar IAM roles en lugar de archivos de credenciales
2. **Logs**: Configurar logging centralizado (ELK, CloudWatch)
3. **Monitoreo**: Agregar health checks y métricas
4. **Red**: Usar redes específicas para aislamiento
5. **Secrets**: Usar Docker secrets o AWS Secrets Manager

## 📝 Próximos Pasos

- [ ] Agregar reintentos automáticos
- [ ] Implementar cola de tareas (opcional)
- [ ] Agregar métricas de Prometheus
- [ ] Implementar rate limiting
- [ ] Agregar autenticación JWT