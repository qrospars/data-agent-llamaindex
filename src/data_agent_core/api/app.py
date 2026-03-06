from fastapi import FastAPI

from data_agent_core.config.env_loader import load_env_file
from data_agent_core.api.routes import router

load_env_file()


def create_app() -> FastAPI:
    app = FastAPI(title="data-agent-core API")
    app.include_router(router)
    return app


app = create_app()
