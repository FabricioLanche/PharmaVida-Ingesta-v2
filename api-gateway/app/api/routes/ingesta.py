from fastapi import APIRouter, HTTPException
from typing import Dict
from ...orchestrator.docker_runner import DockerOrchestrator


router = APIRouter(prefix="/api/ingesta", tags=["ingesta"])


@router.post("/mongodb", response_model=Dict)
async def ingesta_mongodb():
    """
    Ejecuta el script de ingesta de MongoDB en un contenedor independiente.
    Extrae datos de medicos y recetas, y los sube a S3.
    """
    orchestrator = DockerOrchestrator()
    result = await orchestrator.run_mongodb_script()

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result)

    return result


@router.post("/mysql", response_model=Dict)
async def ingesta_mysql():
    """
    Ejecuta el script de ingesta de MySQL en un contenedor independiente.
    Extrae datos de users y compras, y los sube a S3.
    """
    orchestrator = DockerOrchestrator()
    result = await orchestrator.run_mysql_script()

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result)

    return result


@router.post("/postgresql", response_model=Dict)
async def ingesta_postgresql():
    """
    Ejecuta el script de ingesta de PostgreSQL en un contenedor independiente.
    Extrae datos de productos y ofertas, y los sube a S3.
    """
    orchestrator = DockerOrchestrator()
    result = await orchestrator.run_postgresql_script()

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result)

    return result


@router.get("/health")
async def health_check():
    """Endpoint de health check del API Gateway"""
    return {
        "status": "healthy",
        "service": "api-gateway",
        "version": "2.0.0"
    }