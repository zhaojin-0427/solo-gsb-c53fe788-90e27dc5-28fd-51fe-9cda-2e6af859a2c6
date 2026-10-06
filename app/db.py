"""SQLite 持久化层。

数据分为两部分：

* 只增日志（``events``、``event_results``、``approvals``、``decisions``），
  由触发器禁止 UPDATE/DELETE，已发放行与事件记录不可追溯改写；
* 可变运行态（``instances``、``locks``、``occupancy``），始终可由事件日志
  回放重建，重启后直接读出即可恢复当前锁闭状态。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DEFAULT_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "interlocking.db")


def _db_path(path: str | None) -> str:
    return path or os.environ.get("INTERLOCKING_DB", DEFAULT_DB_PATH)

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 站场配置（导入后可整体替换；替换时日志与序号永不回退） ----------------
CREATE TABLE IF NOT EXISTS yards (
    yard_id     TEXT PRIMARY KEY,
    config      TEXT NOT NULL,          -- 原始 JSON
    imported_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS sections (
    yard_id     TEXT NOT NULL REFERENCES yards(yard_id),
    section_id  TEXT NOT NULL,
    a           TEXT NOT NULL,
    b           TEXT NOT NULL,
    PRIMARY KEY (yard_id, section_id)
);
CREATE TABLE IF NOT EXISTS detectors (
    detector_id TEXT PRIMARY KEY,
    yard_id     TEXT NOT NULL REFERENCES yards(yard_id),
    section_id  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS switches (
    yard_id     TEXT NOT NULL REFERENCES yards(yard_id),
    switch_id   TEXT NOT NULL,
    node        TEXT NOT NULL,
    positions   TEXT NOT NULL,          -- JSON 位置键列表
    PRIMARY KEY (yard_id, switch_id)
);
CREATE TABLE IF NOT EXISTS resources (                -- 侧防资源
    yard_id     TEXT NOT NULL REFERENCES yards(yard_id),
    resource_id TEXT NOT NULL,
    kind        TEXT NOT NULL,          -- section | switch | generic
    ref         TEXT NOT NULL,
    PRIMARY KEY (yard_id, resource_id)
);
CREATE TABLE IF NOT EXISTS routes (
    yard_id     TEXT NOT NULL REFERENCES yards(yard_id),
    route_id    TEXT NOT NULL,
    data        TEXT NOT NULL,          -- 完整 JSON（含区段/道岔/侧防）
    PRIMARY KEY (yard_id, route_id)
);

-- 只增日志 --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY,    -- 全站连续序号，从 1 开始
    detector_id TEXT NOT NULL,
    kind        TEXT NOT NULL,          -- occupy | clear
    content_hash TEXT NOT NULL,         -- 与 seq 绑定的内容指纹
    payload     TEXT NOT NULL,          -- 原始报文 JSON
    received_at REAL NOT NULL
);
-- 缓冲状态可由 seq 与 meta.next_seq 派生，保证 events 纯只增。
CREATE TABLE IF NOT EXISTS event_results (
    seq           INTEGER PRIMARY KEY REFERENCES events(seq),
    status        TEXT NOT NULL,        -- processed | duplicate | conflict
    route_id      TEXT,
    instance_id   INTEGER,
    fault         INTEGER NOT NULL DEFAULT 0,
    state         TEXT,                 -- JSON：处理后的状态摘要
    processed_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS approvals (
    approval_id INTEGER PRIMARY KEY AUTOINCREMENT,
    yard_id     TEXT NOT NULL,
    route_id    TEXT NOT NULL,
    decision    TEXT NOT NULL,          -- permitted | denied
    signal      TEXT,                  -- green | red
    reasons     TEXT,                  -- JSON 冲突依据
    created_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,          -- import|approval|event|fault|complete|reset
    ref         TEXT,                   -- 序号/批准号/实例号
    route_id    TEXT,
    instance_id INTEGER,
    summary     TEXT NOT NULL,
    detail      TEXT NOT NULL,          -- JSON
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_route ON decisions(route_id, id);
CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);

-- 运行态 ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instances (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    yard_id     TEXT NOT NULL,
    route_id    TEXT NOT NULL,
    status      TEXT NOT NULL,          -- active | faulted | completed
    position    INTEGER NOT NULL DEFAULT -1,  -- 已确认占用的最大段下标
    final_cleared INTEGER NOT NULL DEFAULT 0,
    approval_id INTEGER NOT NULL,
    created_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS locks (
    yard_id     TEXT NOT NULL,
    resource_type TEXT NOT NULL,        -- section | switch | flank
    resource_id TEXT NOT NULL,
    route_id    TEXT NOT NULL,
    instance_id INTEGER NOT NULL,
    switch_position TEXT,
    locked_at   REAL NOT NULL,
    PRIMARY KEY (yard_id, resource_type, resource_id)
);
CREATE TABLE IF NOT EXISTS occupancy (
    yard_id     TEXT NOT NULL,
    section_id  TEXT NOT NULL,
    occupied    INTEGER NOT NULL,
    PRIMARY KEY (yard_id, section_id)
);
CREATE TABLE IF NOT EXISTS meta (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
);

-- 只增保护：事件/结果/批准/裁决不可 UPDATE/DELETE ----------------------
CREATE TRIGGER IF NOT EXISTS trg_events_no_upd BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_events_no_del BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_event_results_no_upd BEFORE UPDATE ON event_results
BEGIN
    SELECT RAISE(ABORT, 'event_results append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_event_results_no_del BEFORE DELETE ON event_results
BEGIN
    SELECT RAISE(ABORT, 'event_results append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_approvals_no_upd BEFORE UPDATE ON approvals
BEGIN
    SELECT RAISE(ABORT, 'approvals append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_approvals_no_del BEFORE DELETE ON approvals
BEGIN
    SELECT RAISE(ABORT, 'approvals append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_decisions_no_upd BEFORE UPDATE ON decisions
BEGIN
    SELECT RAISE(ABORT, 'decisions append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_decisions_no_del BEFORE DELETE ON decisions
BEGIN
    SELECT RAISE(ABORT, 'decisions append-only');
END;
"""


def connect(path: str | None = None) -> sqlite3.Connection:
    target = _db_path(path)
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(path: str | None = None) -> None:
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 串行化写事务，避免 SQLite database locked。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
