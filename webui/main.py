"""
RB20 v2.5 可视化监控平台 — FastAPI 主入口
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import os

from config import APP_TITLE, APP_VERSION, DEBUG
from api.dashboard import router as dashboard_router
from api.explorer import router as explorer_router
from api.research import router as research_router
from api.rules import router as rules_router
from api.rules import init_rules_table


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup/shutdown lifecycle."""
    print(f"🚀 {APP_TITLE} v{APP_VERSION} starting...")
    await init_rules_table()
    yield
    print("🛑 Shutting down...")


app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if DEBUG else None,
    redoc_url="/redoc" if DEBUG else None,
)

# --- API Routes ---
app.include_router(dashboard_router)
app.include_router(explorer_router)
app.include_router(research_router)
app.include_router(rules_router)

# --- Static Files (SPA) ---
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")


@app.get("/")
async def index():
    """Serve the SPA index.html"""
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": f"{APP_TITLE} API is running. Visit /docs for API documentation."}


@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=DEBUG)
