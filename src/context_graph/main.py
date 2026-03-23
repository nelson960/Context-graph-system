from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from context_graph.api import router as api_router
from context_graph.runtime import build_runtime


def create_app() -> FastAPI:
    app = FastAPI(title="Context Graph API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    runtime = build_runtime()
    app.state.runtime = runtime
    app.title = runtime.settings.api_title
    app.include_router(api_router)

    @app.get("/", include_in_schema=False)
    def serve_index() -> FileResponse:
        if not runtime.settings.frontend_index.exists():
            raise HTTPException(status_code=503, detail="Frontend bundle has not been built yet")
        return FileResponse(runtime.settings.frontend_index)

    @app.get("/{asset_path:path}", include_in_schema=False)
    def serve_spa(asset_path: str) -> FileResponse:
        if asset_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        if not runtime.settings.frontend_dist.exists():
            raise HTTPException(status_code=503, detail="Frontend bundle has not been built yet")
        candidate = (runtime.settings.frontend_dist / asset_path).resolve()
        frontend_root = runtime.settings.frontend_dist.resolve()
        if frontend_root not in candidate.parents and candidate != frontend_root:
            raise HTTPException(status_code=400, detail="Invalid asset path")
        if candidate.exists() and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(runtime.settings.frontend_index)

    return app


app = create_app()
