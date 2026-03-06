from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from data_agent_core.config.env_loader import load_env_file
from data_agent_core.api.routes import router

load_env_file()
UI_DIR = Path(__file__).resolve().parent.parent / "ui"


def create_app() -> FastAPI:
    app = FastAPI(title="data-agent-core API")
    app.include_router(router)
    if UI_DIR.exists():
        app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")

        @app.get("/", include_in_schema=False)
        def ui_index() -> FileResponse:
            return FileResponse(UI_DIR / "index.html")
    return app


app = create_app()
