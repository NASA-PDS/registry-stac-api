import os
from fastapi import FastAPI
from stac_fastapi.api.app import StacApi
from stac_fastapi.opensearch.config import OpensearchSettings
from stac_fastapi.core.core import CoreClient

from pds.registry.stac.database_logic import PDSDatabaseLogic
from pds.registry.stac.PDSClient import PDSClient

# Create the FastAPI app
app = FastAPI(title="PDS Registry STAC API")

database_logic = PDSDatabaseLogic()
client = CoreClient(database=database_logic)
settings = OpensearchSettings()
settings.stac_fastapi_version = "1.0.0"
settings.stac_fastapi_title = "PDS Registry STAC API"
settings.stac_fastapi_description = "STAC API for the PDS Registry"

# Mount the STAC API
api = StacApi(
    client=client,
    settings=settings
)
app = api.app
app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "pds.registry.stac.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="debug",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()
