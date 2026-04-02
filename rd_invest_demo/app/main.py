from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .db import init_db
from .routers.api import router as api_router
from .services.bootstrap import auto_load_initial_data_if_needed

app = FastAPI(title="R&D Investment Control Demo", version="1.0.0")

init_db()
auto_load_initial_data_if_needed()
app.include_router(api_router)

static_dir = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")
