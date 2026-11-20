"""窗口恢复接口：去重窗口从历史交付记录文件恢复的导入、续导、重放核对与清单查询。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.window_recovery import window_recovery_service

router = APIRouter(prefix="/api/dedup_window/recovery", tags=["去重窗口恢复"])


class ImportPayload(BaseModel):
    file_name: str
    content: str
    batch_size: int | None = Field(default=None, ge=0)


class ResumePayload(BaseModel):
    fingerprint: str
    batch_size: int | None = Field(default=None, ge=0)


class ReplayPayload(BaseModel):
    source_instance: str
    source_table: str
    dedup_key: str
    binlog_file: str
    binlog_pos: int
    event_id: str = ""
    operation: str = ""


@router.get("/stats")
def recovery_stats() -> dict[str, Any]:
    return window_recovery_service.recovery_stats()


@router.get("/manifest")
def recovery_manifest(
    源实例: str | None = None,
    来源表: str | None = None,
    去重键: str | None = None,
    回执状态: str | None = None,
    处置结果: str | None = None,
) -> dict[str, Any]:
    filters = {
        "源实例": 源实例 or "",
        "来源表": 来源表 or "",
        "去重键": 去重键 or "",
        "回执状态": 回执状态 or "",
        "处置结果": 处置结果 or "",
    }
    items = window_recovery_service.list_manifest(filters)
    return {"items": items, "total": len(items)}


@router.get("/imports")
def recovery_imports() -> dict[str, Any]:
    items = window_recovery_service.list_imports()
    return {"items": items, "total": len(items)}


@router.get("/pending")
def recovery_pending(fingerprint: str | None = None) -> dict[str, Any]:
    items = window_recovery_service.list_pending(fingerprint)
    return {"items": items, "total": len(items)}


@router.post("/imports")
def import_delivery_file(payload: ImportPayload) -> dict[str, Any]:
    return window_recovery_service.import_delivery_file(
        file_name=payload.file_name, content=payload.content,
        batch_size=payload.batch_size)


@router.post("/imports/resume")
def resume_delivery_import(payload: ResumePayload) -> dict[str, Any]:
    return window_recovery_service.resume_delivery_import(
        fingerprint=payload.fingerprint, batch_size=payload.batch_size)


@router.post("/replay-check")
def replay_check(payload: ReplayPayload) -> dict[str, Any]:
    return window_recovery_service.replay_check(
        source_instance=payload.source_instance,
        source_table=payload.source_table,
        dedup_key=payload.dedup_key,
        binlog_file=payload.binlog_file,
        binlog_pos=payload.binlog_pos,
        event_id=payload.event_id,
        operation=payload.operation,
    )
