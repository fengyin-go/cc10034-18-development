"""窗口恢复领域服务：从历史交付记录文件恢复去重窗口。

业务背景
--------
去重窗口本身只保留短期滑动状态，故障重启后无法判断哪些变更事件已经下发。
本服务把下游回执沉淀成的「交付记录文件」按来源导入、归并，生成窗口恢复
数据集（恢复清单），让消费端在重放时能够核对事件身份，保证已交付事件
不会被二次派送。

关键规则
--------
1. 事件身份由 源实例 + 来源表(source_table) + 去重键 + 下发位点(日志文件+位点)
   联合刻画；同去重键但来源实例不同（orders-west / orders-east）身份不同，
   西侧回执不能抵扣东侧事件。
2. 一个交付文件只证明其自身源实例的交付事实；未核实（无回执）记录不拦截下发。
3. 同一文件按文件指纹去重，重复导入只生效一次；导入中断后原始条目停留在
   待核对暂存区，可续导核对，已入清单的归属不受影响。
4. 窗口版本从 源实例 / 日志文件 / 日志位置 三个源坐标比对。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.pipeline import pipeline_bus

# 交付记录字段别名：兼容英文键、简写与中文键
FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "source_instance": ("source_instance", "instance", "源实例"),
    "source_table": ("source_table", "table", "来源表"),
    "dedup_key": ("dedup_key", "key", "去重键"),
    "binlog_file": ("binlog_file", "file", "日志文件"),
    "binlog_pos": ("binlog_pos", "binlog_position", "position", "pos", "日志位点"),
    "receipt_confirmed": ("receipt_confirmed", "confirmed", "receipt", "回执确认", "是否确认"),
    "event_id": ("event_id", "事件ID"),
    "operation": ("operation", "操作类型"),
    "event_time": ("event_time", "事件时间"),
}
REQUIRED_FIELDS = ("source_instance", "source_table", "dedup_key", "binlog_file", "binlog_pos")

# 清单状态取值
RECEIPT_CONFIRMED = "已确认"
RECEIPT_UNVERIFIED = "未核实"
VERIFY_DONE = "已核实"
VERIFY_PENDING = "待核实"
DISPOSITION_WAIT = "待重放"
DISPOSITION_BLOCKED = "已拦截"
DISPOSITION_PASSED = "已放行"

SESSION_RUNNING = "进行中"
SESSION_INTERRUPTED = "中断"
SESSION_COMPLETED = "已完成"


class WindowRecoveryService:
    """窗口恢复数据集的导入、归并、核对与回写。"""

    def __init__(self) -> None:
        self._bootstrap_demo_data()

    # ------------------------------------------------------------------ 解析

    def parse_delivery_file(self, file_name: str, content: str) -> tuple[dict[str, Any] | None, str]:
        """解析交付记录文件内容，返回 (文件对象, 错误信息)。"""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return None, "交付记录文件不是合法 JSON，无法核对来源"
        if isinstance(data, list):
            raw_records = data
            file_defaults: dict[str, Any] = {}
        elif isinstance(data, dict):
            raw_records = data.get("records") or data.get("deliveries") or []
            file_defaults = {k: v for k, v in data.items() if k in ("source_instance",)}
            file_name = str(data.get("file_name") or file_name)
        else:
            return None, "交付记录文件结构需为对象或数组"
        if not isinstance(raw_records, list) or not raw_records:
            return None, "交付记录文件中没有可导入的交付条目"
        return {"file_name": file_name, "defaults": file_defaults, "records": raw_records}, ""

    def _normalize_entry(self, raw: dict[str, Any],
                         defaults: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """把一条原始交付条目规范化为内部结构，返回 (条目, 缺失字段)。"""
        picked: dict[str, Any] = {}
        for canonical, aliases in FIELD_ALIASES.items():
            value = None
            for alias in aliases:
                if alias in raw and raw[alias] not in (None, ""):
                    value = raw[alias]
                    break
            if value is None and canonical in defaults:
                value = defaults[canonical]
            picked[canonical] = value
        missing = [f for f in REQUIRED_FIELDS if picked.get(f) in (None, "")]
        if missing:
            return None, missing
        try:
            picked["binlog_pos"] = int(str(picked["binlog_pos"]).strip())
        except (TypeError, ValueError):
            return None, ["binlog_pos(日志位点)需为整数"]
        picked["source_instance"] = str(picked["source_instance"]).strip()
        picked["source_table"] = str(picked["source_table"]).strip()
        picked["dedup_key"] = str(picked["dedup_key"]).strip()
        picked["binlog_file"] = str(picked["binlog_file"]).strip()
        picked["receipt_confirmed"] = bool(picked.get("receipt_confirmed"))
        return picked, []

    # ------------------------------------------------------------------ 导入

    def import_delivery_file(self, *, file_name: str, content: str,
                             batch_size: int | None = None) -> dict[str, Any]:
        """导入交付记录文件，生成/归并窗口恢复数据集。

        batch_size 用于模拟上传中断：只核对前 N 条，其余进入暂存区等待续导。
        """
        parsed, error = self.parse_delivery_file(file_name, content)
        if parsed is None:
            return {"ok": False, "message": error}
        fingerprint = pipeline_bus.recovery_file_fingerprint(parsed["file_name"], content)
        existing = self._find_session(fingerprint)
        if existing is not None:
            # 重复导入只生效一次
            if existing["状态"] == SESSION_COMPLETED:
                return {
                    "ok": True, "idempotent": True,
                    "message": "该交付记录文件已完成导入，重复导入不重复生效",
                    "session": existing,
                }
            return {
                "ok": False, "idempotent": True,
                "message": "该文件导入曾中断，请使用续导接口继续核对来源",
                "session": existing,
            }

        entries: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for seq, raw in enumerate(parsed["records"]):
            if not isinstance(raw, dict):
                invalid.append({"序号": seq, "原因": "条目不是对象", "原始": str(raw)[:120]})
                continue
            entry, problems = self._normalize_entry(raw, parsed["defaults"])
            if entry is None:
                invalid.append({"序号": seq, "原因": "、".join(problems), "原始": raw})
            else:
                entry["_seq"] = seq
                entries.append(entry)
        if not entries:
            return {"ok": False, "message": "交付条目全部缺少必填字段，未生成恢复数据",
                    "invalid": invalid}

        session = {
            "id": self._next_session_id(),
            "文件名称": parsed["file_name"],
            "文件指纹": fingerprint,
            "状态": SESSION_RUNNING,
            "条目总数": len(entries),
            "已核对条数": 0,
            "无效条数": len(invalid),
            "新增条数": 0,
            "归并更新条数": 0,
            "重复条数": 0,
            "无效明细": invalid,
            "创建时间": datetime.now().isoformat(),
            "最近更新时间": datetime.now().isoformat(),
        }
        pipeline_bus.recovery_imports.append(session)
        # 全部条目先进暂存区，处理一条移除一条，中断后仍可续导
        for entry in entries:
            pipeline_bus.recovery_pending.append(
                {"文件指纹": fingerprint, "序号": entry["_seq"], "条目": entry})
        self._process_pending(session, batch_size)
        pipeline_bus.write_audit(
            "窗口恢复导入", parsed["file_name"],
            f"交付记录文件导入：{session['状态']}，已核对 {session['已核对条数']}/{session['条目总数']}，"
            f"新增 {session['新增条数']}，归并 {session['归并更新条数']}，重复 {session['重复条数']}",
            "dedup_window", session["状态"],
        )
        return {"ok": True, "idempotent": False, "session": session}

    def resume_delivery_import(self, fingerprint: str,
                               batch_size: int | None = None) -> dict[str, Any]:
        """续导：继续核对中断文件暂存区中的剩余来源条目。"""
        session = self._find_session(fingerprint)
        if session is None:
            return {"ok": False, "message": f"未找到文件指纹 {fingerprint} 的导入会话"}
        if session["状态"] == SESSION_COMPLETED:
            return {"ok": True, "idempotent": True,
                    "message": "该文件已完成导入，续导不重复生效", "session": session}
        before = session["已核对条数"]
        self._process_pending(session, batch_size)
        pipeline_bus.write_audit(
            "窗口恢复续导", session["文件名称"],
            f"续导核对 {session['已核对条数'] - before} 条，会话状态：{session['状态']}",
            "dedup_window", session["状态"],
        )
        return {"ok": True, "idempotent": False, "session": session}

    def _process_pending(self, session: dict[str, Any], batch_size: int | None) -> None:
        fingerprint = session["文件指纹"]
        pending = [p for p in pipeline_bus.recovery_pending if p["文件指纹"] == fingerprint]
        chunk = pending if batch_size is None else pending[: max(int(batch_size), 0)]
        for item in chunk:
            outcome = self._merge_manifest(item["条目"], session)
            session[f"{outcome}条数"] = session.get(f"{outcome}条数", 0) + 1
            pipeline_bus.recovery_pending.remove(item)
            session["已核对条数"] += 1
        remaining = [p for p in pipeline_bus.recovery_pending if p["文件指纹"] == fingerprint]
        session["状态"] = SESSION_INTERRUPTED if remaining else SESSION_COMPLETED
        session["最近更新时间"] = datetime.now().isoformat()

    def _merge_manifest(self, entry: dict[str, Any], session: dict[str, Any]) -> str:
        """按事件身份把交付条目归并进恢复清单，返回 新增/归并更新/重复。"""
        bus = pipeline_bus
        identity = bus.recovery_event_identity(
            entry["source_instance"], entry["source_table"], entry["dedup_key"],
            entry["binlog_file"], entry["binlog_pos"])
        existing = bus.find_recovery_record(identity)
        now_iso = datetime.now().isoformat()
        if existing is not None:
            existing["归并次数"] += 1
            if session["文件名称"] not in existing["来源文件"]:
                existing["来源文件"].append(session["文件名称"])
            # 回执升级：未核实 -> 已确认；其余重复导入不改变归属
            if entry["receipt_confirmed"] and existing["回执状态"] != RECEIPT_CONFIRMED:
                existing["回执状态"] = RECEIPT_CONFIRMED
                existing["核实状态"] = VERIFY_DONE
                existing["最近回执位点"] = entry["binlog_pos"]
                return "归并更新"
            return "重复"
        record = {
            "id": self._next_manifest_id(),
            "源实例": entry["source_instance"],
            "来源表": entry["source_table"],
            "去重键": entry["dedup_key"],
            "日志文件": entry["binlog_file"],
            "日志位点": entry["binlog_pos"],
            "事件身份": identity,
            "窗口版本": bus.window_version(
                entry["source_instance"], entry["binlog_file"], entry["binlog_pos"]),
            "事件ID": entry.get("event_id") or "",
            "操作类型": (entry.get("operation") or "").upper(),
            "事件时间": entry.get("event_time") or "",
            "回执状态": RECEIPT_CONFIRMED if entry["receipt_confirmed"] else RECEIPT_UNVERIFIED,
            "核实状态": VERIFY_DONE if entry["receipt_confirmed"] else VERIFY_PENDING,
            "处置结果": DISPOSITION_WAIT,
            "下发判定": "",
            "处置备注": "",
            "处置时间": None,
            "最近回执位点": entry["binlog_pos"] if entry["receipt_confirmed"] else None,
            "来源文件": [session["文件名称"]],
            "导入批次": session["id"],
            "归并次数": 1,
            "导入时间": now_iso,
        }
        bus.recovery_manifest.append(record)
        return "新增"

    # ------------------------------------------------------------------ 重放

    def replay_check(self, *, source_instance: str, source_table: str, dedup_key: str,
                     binlog_file: str, binlog_pos: int,
                     event_id: str = "", operation: str = "") -> dict[str, Any]:
        """消费端关联恢复数据集校验重放，返回下发决定并把处置结果回写清单。

        仅同源实例、同来源表、同去重键且带已确认回执的记录参与拦截判定；
        跨源回执（如同键的 orders-west 回执对 orders-east 事件）只展示、不抵扣。
        """
        bus = pipeline_bus
        try:
            binlog_pos = int(binlog_pos)
        except (TypeError, ValueError):
            return {"ok": False, "message": "日志位点需为整数"}
        identity = bus.recovery_event_identity(
            source_instance, source_table, dedup_key, binlog_file, binlog_pos)
        current_version = bus.window_version(source_instance, binlog_file, binlog_pos)

        same_key = [
            r for r in bus.recovery_manifest
            if r["来源表"] == source_table and r["去重键"] == dedup_key
        ]
        same_source = [r for r in same_key if r["源实例"] == source_instance]
        cross_source = [r for r in same_key if r["源实例"] != source_instance]
        confirmed = [r for r in same_source if r["回执状态"] == RECEIPT_CONFIRMED]
        unverified = [r for r in same_source if r["回执状态"] == RECEIPT_UNVERIFIED]
        exact = next((r for r in same_source if r["事件身份"] == identity), None)
        # 已确认回执构成前缀窗口：同文件内，确认位点不早于当前位点时，
        # 说明该坐标落在「此前已交付」的窗口范围内（确认位点 P 认证所有 <= P 的位置）。
        # 反过来，当前位点晚于全部已确认位点时，属于窗口之外的新事件，未核实记录不拦截。
        covering = [
            r for r in confirmed
            if r["日志文件"] == binlog_file and int(r["日志位点"]) >= binlog_pos
        ]
        covering.sort(key=lambda r: int(r["日志位点"]))

        if exact is not None and exact["回执状态"] == RECEIPT_CONFIRMED:
            decision, reason = "拦截", "事件身份与已交付回执完全一致，该事件已下发，禁止重复派送"
            hit = exact
        elif covering:
            hit = covering[0]
            decision, reason = (
                "拦截",
                f"同源窗口版本 {hit['窗口版本']} 已确认交付至位点 {hit['日志位点']}，"
                f"当前坐标 {binlog_pos} 落在此前已交付窗口内",
            )
        else:
            hit = None
            decision = "放行"
            if cross_source:
                reason = (
                    f"恢复清单仅有其他源实例（{ '、'.join(sorted({r['源实例'] for r in cross_source})) }）"
                    "的同键回执，来源不同不能抵扣；本实例无已确认交付记录，准予下发"
                )
            elif unverified:
                reason = "同键存在未核实记录（无回执），不构成交付证明，准予下发并保留待核实"
            else:
                reason = "恢复清单中无同源已交付记录，准予下发"

        version_compare = {
            "当前窗口版本": current_version,
            "历史窗口版本": hit["窗口版本"] if hit else None,
            "比对结论": self._compare_version(current_version, hit),
        }
        if decision == "拦截":
            bus.pipeline_metrics["recovery_replay_blocked"] += 1
        else:
            bus.pipeline_metrics["recovery_replay_passed"] += 1
        writeback = self._writeback_disposition(
            exact=exact, covering=covering, decision=decision, reason=reason,
            identity=identity, current_version=current_version,
            event_id=event_id, operation=operation)

        bus.write_audit(
            "重放核对", f"{source_instance}/{source_table}#{dedup_key}",
            f"事件身份 {identity} 重放校验决定：{decision}（{reason}）",
            "dedup_window", DISPOSITION_BLOCKED if decision == "拦截" else DISPOSITION_PASSED,
        )
        return {
            "ok": True,
            "事件身份": identity,
            "下发决定": decision,
            "决定原因": reason,
            "源坐标核对": {
                "源实例": source_instance, "来源表": source_table, "去重键": dedup_key,
                "日志文件": binlog_file, "日志位点": binlog_pos,
                "事件ID": event_id, "操作类型": operation,
            },
            "窗口版本比对": version_compare,
            "同源已确认记录": [self._summary(r) for r in confirmed],
            "未核实记录": [self._summary(r) for r in unverified],
            "跨源回执不可抵扣": [self._summary(r) for r in cross_source],
            "命中清单ID": hit["id"] if hit else None,
            "回写结果": writeback,
        }

    def _compare_version(self, current: str, hit: dict[str, Any] | None) -> str:
        if hit is None:
            return "无历史窗口版本，按新事件处理"
        if hit["窗口版本"] == current:
            return "窗口版本完全一致：同源同文件同位点，历史已交付"
        return f"窗口版本相邻：当前 {current} 落在历史确认窗口 {hit['窗口版本']} 之内"

    def _writeback_disposition(self, *, exact: dict[str, Any] | None,
                               covering: list[dict[str, Any]], decision: str,
                               reason: str, identity: str, current_version: str,
                               event_id: str, operation: str) -> dict[str, Any]:
        """把处置结果回写到恢复清单。"""
        now_iso = datetime.now().isoformat()
        disposition = DISPOSITION_BLOCKED if decision == "拦截" else DISPOSITION_PASSED
        target = exact or (covering[0] if covering else None)
        if target is None:
            return {"已回写": False, "说明": "无匹配清单记录，处置结果仅在响应中给出"}
        target["处置结果"] = disposition
        target["下发判定"] = reason
        target["处置时间"] = now_iso
        note = f"[{now_iso}] 身份 {identity} 版本 {current_version}"
        if event_id:
            note += f" 事件 {event_id}"
        if operation:
            note += f" {operation}"
        target["处置备注"] = (target["处置备注"] + " | " + note).strip(" |")
        return {"已回写": True, "清单ID": target["id"], "处置结果": disposition}

    # ------------------------------------------------------------------ 查询

    def list_manifest(self, filters: dict[str, str] | None = None) -> list[dict[str, Any]]:
        rows = list(pipeline_bus.recovery_manifest)
        for field, expected in (filters or {}).items():
            expected = expected.strip()
            if expected:
                rows = [r for r in rows if expected.casefold() in str(r.get(field, "")).casefold()]
        return rows

    def list_imports(self) -> list[dict[str, Any]]:
        return list(pipeline_bus.recovery_imports)

    def list_pending(self, fingerprint: str | None = None) -> list[dict[str, Any]]:
        rows = pipeline_bus.recovery_pending
        if fingerprint:
            rows = [p for p in rows if p["文件指纹"] == fingerprint]
        return rows

    def recovery_stats(self) -> dict[str, Any]:
        manifest = pipeline_bus.recovery_manifest
        sessions = pipeline_bus.recovery_imports
        metrics = pipeline_bus.pipeline_metrics
        return {
            "恢复清单总数": len(manifest),
            "已确认回执数": sum(1 for r in manifest if r["回执状态"] == RECEIPT_CONFIRMED),
            "待核实记录数": sum(1 for r in manifest if r["回执状态"] == RECEIPT_UNVERIFIED),
            # 清单当前处置状态（会被后续重放覆盖）
            "清单当前已拦截数": sum(1 for r in manifest if r["处置结果"] == DISPOSITION_BLOCKED),
            "清单当前已放行数": sum(1 for r in manifest if r["处置结果"] == DISPOSITION_PASSED),
            "待重放数": sum(1 for r in manifest if r["处置结果"] == DISPOSITION_WAIT),
            # 重放决定累计值（含无清单记录可回写的跨源事件）
            "累计拦截重放数": metrics["recovery_replay_blocked"],
            "累计放行重放数": metrics["recovery_replay_passed"],
            "导入会话数": len(sessions),
            "中断会话数": sum(1 for s in sessions if s["状态"] == SESSION_INTERRUPTED),
            "暂存待核对条数": len(pipeline_bus.recovery_pending),
        }

    # ------------------------------------------------------------------ 辅助

    def _find_session(self, fingerprint: str) -> dict[str, Any] | None:
        return next((s for s in pipeline_bus.recovery_imports
                     if s["文件指纹"] == fingerprint), None)

    def _next_manifest_id(self) -> int:
        return max((int(r["id"]) for r in pipeline_bus.recovery_manifest), default=0) + 1

    def _next_session_id(self) -> int:
        return max((int(s["id"]) for s in pipeline_bus.recovery_imports), default=0) + 1

    @staticmethod
    def _summary(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "清单ID": record["id"], "源实例": record["源实例"], "来源表": record["来源表"],
            "去重键": record["去重键"], "窗口版本": record["窗口版本"],
            "日志位点": record["日志位点"], "回执状态": record["回执状态"],
            "核实状态": record["核实状态"], "处置结果": record["处置结果"],
            "事件身份": record["事件身份"],
        }

    # ---------------------------------------------------------- 演示数据引导

    def _bootstrap_demo_data(self) -> None:
        """预置一组西侧交付场景：一个已完成文件 + 一个中断文件，便于界面演示。"""
        bus = pipeline_bus
        if getattr(bus, "_recovery_bootstrapped", False):
            return
        bus._recovery_bootstrapped = True  # type: ignore[attr-defined]
        west_file = json.dumps({
            "file_name": "delivery-orders-west-20261118.json",
            "source_instance": "orders-west",
            "records": [
                {"source_table": "orders", "dedup_key": "ORD-1001",
                 "binlog_file": "mysql-bin.000123", "binlog_pos": 4580,
                 "receipt_confirmed": True, "operation": "UPDATE",
                 "event_id": "evt-west-1001"},
                {"source_table": "orders", "dedup_key": "ORD-1002",
                 "binlog_file": "mysql-bin.000123", "binlog_pos": 4720,
                 "receipt_confirmed": True, "operation": "INSERT",
                 "event_id": "evt-west-1002"},
                {"source_table": "orders", "dedup_key": "ORD-1003",
                 "binlog_file": "mysql-bin.000124", "binlog_pos": 128,
                 "receipt_confirmed": True, "operation": "UPDATE",
                 "event_id": "evt-west-1003"},
            ],
        }, ensure_ascii=False)
        self.import_delivery_file(
            file_name="delivery-orders-west-20261118.json", content=west_file)
        partial_file = json.dumps({
            "file_name": "delivery-orders-west-20261119.json",
            "source_instance": "orders-west",
            "records": [
                {"source_table": "orders", "dedup_key": "ORD-1004",
                 "binlog_file": "mysql-bin.000124", "binlog_pos": 902,
                 "receipt_confirmed": True, "operation": "UPDATE",
                 "event_id": "evt-west-1004"},
                {"source_table": "orders", "dedup_key": "ORD-1005",
                 "binlog_file": "mysql-bin.000124", "binlog_pos": 1155,
                 "receipt_confirmed": True, "operation": "UPDATE",
                 "event_id": "evt-west-1005"},
                {"source_table": "orders", "dedup_key": "ORD-1006",
                 "binlog_file": "mysql-bin.000124", "binlog_pos": 1320,
                 "receipt_confirmed": False, "operation": "DELETE",
                 "event_id": "evt-west-1006"},
            ],
        }, ensure_ascii=False)
        self.import_delivery_file(
            file_name="delivery-orders-west-20261119.json",
            content=partial_file, batch_size=1)


window_recovery_service = WindowRecoveryService()
