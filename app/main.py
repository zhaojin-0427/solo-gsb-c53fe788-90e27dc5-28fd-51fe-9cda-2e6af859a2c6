"""FastAPI 入口：联锁推演 REST API + 静态前端。

写接口（放行/上报/复位/导入）在全局写锁内串行执行，配合 SQLite
BEGIN IMMEDIATE，保证百万级日志下并发反馈稳定。
"""

from __future__ import annotations

import threading
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import interlocking as il
from .db import connect, init_db

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

_db_lock = threading.RLock()
_conn = connect()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="站场进路联锁推演台", version="1.0.0", lifespan=lifespan)


@app.exception_handler(il.ClientError)
async def client_error_handler(request: Request, exc: il.ClientError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, **exc.extra}},
    )


class RouteRequest(BaseModel):
    route_id: str


class ResetRequest(BaseModel):
    instance_id: int


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/state")
def get_state() -> dict:
    with _db_lock:
        return il.snapshot(_conn)


@app.post("/api/yard")
async def post_yard(request: Request) -> dict:
    raw = await request.json()
    with _db_lock:
        return il.import_yard(_conn, raw)


@app.post("/api/routes/request")
def post_route(body: RouteRequest) -> dict:
    with _db_lock:
        return il.request_route(_conn, body.route_id)


@app.post("/api/events")
async def post_event(request: Request) -> dict:
    payload = await request.json()
    with _db_lock:
        return il.report_event(_conn, payload)


@app.post("/api/reset")
def post_reset(body: ResetRequest) -> dict:
    with _db_lock:
        return il.reset_instance(_conn, body.instance_id)


@app.get("/api/decisions")
def get_decisions(
    before_id: int | None = None,
    limit: int = 100,
    kind: str | None = None,
    route_id: str | None = None,
) -> dict:
    with _db_lock:
        return il.list_decisions(_conn, before_id, limit, kind, route_id)


@app.get("/api/replay")
def get_replay(after_id: int = 0, limit: int = 100) -> dict:
    with _db_lock:
        return il.replay(_conn, after_id, limit)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
