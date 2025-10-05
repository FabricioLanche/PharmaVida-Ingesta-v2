from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import ingesta

app = FastAPI(
    title="PharmaVida Ingesta API Gateway",
    description="API Gateway para orquestar scripts de ingesta en contenedores independientes",
    version="2.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(ingesta.router)


@app.get("/")
async def root():
    return {
        "message": "PharmaVida Ingesta API Gateway",
        "version": "2.0.0",
        "architecture": "Microservices with ephemeral containers",
        "endpoints": {
            "mongodb": "POST /api/ingesta/mongodb",
            "mysql": "POST /api/ingesta/mysql",
            "postgresql": "POST /api/ingesta/postgresql",
            "health": "GET /api/ingesta/health"
        }
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "api-gateway"}