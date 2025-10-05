import docker
from docker.errors import ContainerError, ImageNotFound, APIError
from typing import Dict, Any
from app.core.config import settings
import json


class DockerOrchestrator:
    def __init__(self):
        try:
            # Conectar explícitamente al socket de Docker
            self.client = docker.DockerClient(base_url='unix://var/run/docker.sock')
            # Verificar conexión
            self.client.ping()
        except Exception as e:
            raise RuntimeError(f"No se pudo conectar al Docker daemon: {str(e)}")

    def _get_common_env(self) -> Dict[str, str]:
        """Retorna variables de entorno comunes para todos los scripts"""
        env = {
            "AWS_BUCKET_NAME": settings.AWS_BUCKET_NAME,
            "AWS_REGION": settings.AWS_REGION,
            "AWS_ACCESS_KEY_ID": settings.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": settings.AWS_SECRET_ACCESS_KEY,
        }

        # Agregar session token si existe
        if settings.AWS_SESSION_TOKEN:
            env["AWS_SESSION_TOKEN"] = settings.AWS_SESSION_TOKEN

        return env

    def _parse_container_output(self, output: bytes) -> Dict[str, Any]:
        """Parsea la salida del contenedor"""
        try:
            output_str = output.decode('utf-8').strip()
            # Intenta parsear como JSON
            return json.loads(output_str)
        except json.JSONDecodeError:
            # Si no es JSON, retorna como texto
            return {"output": output_str}
        except Exception as e:
            return {"error": f"Error parseando salida: {str(e)}", "raw_output": output_str}

    async def run_mongodb_script(self) -> Dict[str, Any]:
        """Ejecuta el script de ingesta de MongoDB en un contenedor efímero"""
        try:
            env_vars = self._get_common_env()
            env_vars.update({
                "MONGO_HOST": settings.MONGO_HOST,
                "MONGO_PORT": str(settings.MONGO_PORT),
                "MONGO_USER": settings.MONGO_USER,
                "MONGO_PASSWORD": settings.MONGO_PASSWORD,
                "MONGO_DATABASE": settings.MONGO_DATABASE,
            })

            container = self.client.containers.run(
                image="pharmavida-ingesta-mongodb:latest",
                environment=env_vars,
                network=settings.DOCKER_NETWORK,
                remove=True,
                detach=False
            )

            result = self._parse_container_output(container)
            return {
                "status": "success",
                "database": "mongodb",
                "result": result
            }

        except ImageNotFound:
            return {
                "status": "error",
                "database": "mongodb",
                "error": "Imagen pharmavida-ingesta-mongodb:latest no encontrada. Ejecuta: docker build -t pharmavida-ingesta-mongodb:latest ./scripts/mongodb"
            }
        except ContainerError as e:
            return {
                "status": "error",
                "database": "mongodb",
                "error": f"Error en la ejecución del contenedor: {str(e)}"
            }
        except APIError as e:
            return {
                "status": "error",
                "database": "mongodb",
                "error": f"Error de Docker API: {str(e)}"
            }
        except Exception as e:
            return {
                "status": "error",
                "database": "mongodb",
                "error": f"Error inesperado: {str(e)}"
            }

    async def run_mysql_script(self) -> Dict[str, Any]:
        """Ejecuta el script de ingesta de MySQL en un contenedor efímero"""
        try:
            env_vars = self._get_common_env()
            env_vars.update({
                "MYSQL_HOST": settings.MYSQL_HOST,
                "MYSQL_PORT": str(settings.MYSQL_PORT),
                "MYSQL_USER": settings.MYSQL_USER,
                "MYSQL_PASSWORD": settings.MYSQL_PASSWORD,
                "MYSQL_DATABASE": settings.MYSQL_DATABASE,
            })

            container = self.client.containers.run(
                image="pharmavida-ingesta-mysql:latest",
                environment=env_vars,
                network=settings.DOCKER_NETWORK,
                remove=True,
                detach=False
            )

            result = self._parse_container_output(container)
            return {
                "status": "success",
                "database": "mysql",
                "result": result
            }

        except ImageNotFound:
            return {
                "status": "error",
                "database": "mysql",
                "error": "Imagen pharmavida-ingesta-mysql:latest no encontrada. Ejecuta: docker build -t pharmavida-ingesta-mysql:latest ./scripts/mysql"
            }
        except ContainerError as e:
            return {
                "status": "error",
                "database": "mysql",
                "error": f"Error en la ejecución del contenedor: {str(e)}"
            }
        except APIError as e:
            return {
                "status": "error",
                "database": "mysql",
                "error": f"Error de Docker API: {str(e)}"
            }
        except Exception as e:
            return {
                "status": "error",
                "database": "mysql",
                "error": f"Error inesperado: {str(e)}"
            }

    async def run_postgresql_script(self) -> Dict[str, Any]:
        """Ejecuta el script de ingesta de PostgreSQL en un contenedor efímero"""
        try:
            env_vars = self._get_common_env()
            env_vars.update({
                "POSTGRES_HOST": settings.POSTGRES_HOST,
                "POSTGRES_PORT": str(settings.POSTGRES_PORT),
                "POSTGRES_USER": settings.POSTGRES_USER,
                "POSTGRES_PASSWORD": settings.POSTGRES_PASSWORD,
                "POSTGRES_DATABASE": settings.POSTGRES_DATABASE,
            })

            container = self.client.containers.run(
                image="pharmavida-ingesta-postgresql:latest",
                environment=env_vars,
                network=settings.DOCKER_NETWORK,
                remove=True,
                detach=False
            )

            result = self._parse_container_output(container)
            return {
                "status": "success",
                "database": "postgresql",
                "result": result
            }

        except ImageNotFound:
            return {
                "status": "error",
                "database": "postgresql",
                "error": "Imagen pharmavida-ingesta-postgresql:latest no encontrada. Ejecuta: docker build -t pharmavida-ingesta-postgresql:latest ./scripts/postgresql"
            }
        except ContainerError as e:
            return {
                "status": "error",
                "database": "postgresql",
                "error": f"Error en la ejecución del contenedor: {str(e)}"
            }
        except APIError as e:
            return {
                "status": "error",
                "database": "postgresql",
                "error": f"Error de Docker API: {str(e)}"
            }
        except Exception as e:
            return {
                "status": "error",
                "database": "postgresql",
                "error": f"Error inesperado: {str(e)}"
            }