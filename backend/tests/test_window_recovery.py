"""窗口恢复端到端核对：覆盖需求中的全部关键场景。

场景：导入只含 orders-west 已确认日志位置、无 orders-east 回执的交付文件；
恢复期间两侧都提交同键变更；中断后重新导入；核对下发时比对两侧源坐标与
未核实记录，确认西侧已交付事件不会再派送，而同键东侧事件不受西侧回执抵扣。
"""
from __future__ import annotations

import json
import sys

from app.pipeline import pipeline_bus
from app.services.window_recovery import (
    SESSION_COMPLETED,
    SESSION_INTERRUPTED,
    WindowRecoveryService,
)

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✔ {name}")
    else:
        FAIL += 1
        print(f"  ✘ {name} {detail}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    # 用全新服务实例，避免引导数据干扰
    pipeline_bus.recovery_manifest.clear()
    pipeline_bus.recovery_imports.clear()
    pipeline_bus.recovery_pending.clear()
    svc = WindowRecoveryService()
    # 清掉引导数据（构造函数已重新导入一次）
    pipeline_bus.recovery_manifest.clear()
    pipeline_bus.recovery_imports.clear()
    pipeline_bus.recovery_pending.clear()

    west_only_file = json.dumps({
        "file_name": "delivery-orders-west-cutover.json",
        "source_instance": "orders-west",
        "records": [
            # 同键 ORD-7788：西侧已确认交付，日志位置 mysql-bin.000201@8821
            {"source_table": "orders", "dedup_key": "ORD-7788",
             "binlog_file": "mysql-bin.000201", "binlog_pos": 8821,
             "receipt_confirmed": True, "event_id": "evt-w-7788",
             "operation": "UPDATE", "event_time": "2026-11-19T10:12:03"},
            # 西侧另一条已确认
            {"source_table": "orders", "dedup_key": "ORD-7789",
             "binlog_file": "mysql-bin.000201", "binlog_pos": 9044,
             "receipt_confirmed": True, "event_id": "evt-w-7789",
             "operation": "INSERT"},
            # 西侧一条无回执的未核实记录
            {"source_table": "orders", "dedup_key": "ORD-7790",
             "binlog_file": "mysql-bin.000202", "binlog_pos": 310,
             "receipt_confirmed": False, "event_id": "evt-w-7790",
             "operation": "UPDATE"},
        ],
    }, ensure_ascii=False)

    section("1. 上传中断：首批只核对 1 条")
    r1 = svc.import_delivery_file(
        file_name="delivery-orders-west-cutover.json",
        content=west_only_file, batch_size=1)
    check("导入请求受理", r1["ok"], str(r1))
    session1 = r1["session"]
    check("会话标记为中断", session1["状态"] == SESSION_INTERRUPTED,
          f"实际={session1['状态']}")
    check("已核对 1 条", session1["已核对条数"] == 1)
    check("暂存区保留 2 条", len(svc.list_pending()) == 2)
    check("清单只落 1 条归属", len(svc.list_manifest()) == 1)
    fingerprint = session1["文件指纹"]

    section("2. 中断期间同键事件从两侧重新到达（先核对西侧，再核对东侧）")
    # 西侧：与已确认回执完全同身份的重放
    west_replay = svc.replay_check(
        source_instance="orders-west", source_table="orders",
        dedup_key="ORD-7788", binlog_file="mysql-bin.000201",
        binlog_pos=8821, event_id="evt-w-7788", operation="UPDATE")
    check("西侧同身份重放被拦截", west_replay["下发决定"] == "拦截",
          west_replay.get("决定原因", ""))
    check("拦截原因说明已交付", "已下发" in west_replay["决定原因"])
    west_record = svc.list_manifest({"去重键": "ORD-7788"})[0]
    check("处置结果回写清单（已拦截）", west_record["处置结果"] == "已拦截")
    check("回写含处置时间", bool(west_record["处置时间"]))

    # 东侧：同去重键，但源实例是 orders-east，文件里没有东侧回执
    east_replay = svc.replay_check(
        source_instance="orders-east", source_table="orders",
        dedup_key="ORD-7788", binlog_file="mysql-bin.000307",
        binlog_pos=5520, event_id="evt-e-7788", operation="UPDATE")
    check("东侧同键事件放行", east_replay["下发决定"] == "放行",
          east_replay.get("决定原因", ""))
    check("明确提示跨源回执不可抵扣",
          "不能抵扣" in east_replay["决定原因"], east_replay["决定原因"])
    check("响应列出跨源西侧回执",
          len(east_replay["跨源回执不可抵扣"]) == 1
          and east_replay["跨源回执不可抵扣"][0]["源实例"] == "orders-west")
    check("东侧无清单记录可回写，响应如实说明",
          east_replay["回写结果"]["已回写"] is False)
    check("东西两侧事件身份不同",
          west_replay["事件身份"] != east_replay["事件身份"])
    check("窗口版本按源实例/文件/位点区分",
          west_replay["窗口版本比对"]["当前窗口版本"].startswith("orders-west/")
          and east_replay["窗口版本比对"]["当前窗口版本"].startswith("orders-east/"))

    section("3. 重复导入同一文件只生效一次（中断态拒绝重复开单）")
    r_dup = svc.import_delivery_file(
        file_name="delivery-orders-west-cutover.json", content=west_only_file)
    check("重复导入被幂等识别", r_dup.get("idempotent") is True and r_dup["ok"] is False)
    check("提示走续导接口", "续导" in r_dup["message"])
    check("暂存区未被重复填充", len(svc.list_pending()) == 2)

    section("4. 续导：继续核对来源，中断后可分批续传")
    r_resume1 = svc.resume_delivery_import(fingerprint, batch_size=1)
    check("续导受理", r_resume1["ok"])
    check("续导后仍处中断（还剩 1 条）",
          r_resume1["session"]["状态"] == SESSION_INTERRUPTED)
    check("累计已核对 2 条", r_resume1["session"]["已核对条数"] == 2)
    check("已确认窗口仍保留此前归属",
          len(svc.list_manifest()) == 2
          and svc.list_manifest({"去重键": "ORD-7788"})[0]["处置结果"] == "已拦截")

    r_resume2 = svc.resume_delivery_import(fingerprint)
    check("全部核对完成", r_resume2["session"]["状态"] == SESSION_COMPLETED)
    check("暂存区清空", len(svc.list_pending()) == 0)
    check("清单共 3 条", len(svc.list_manifest()) == 3)

    section("5. 完成态重复导入仍然只生效一次")
    r_dup2 = svc.import_delivery_file(
        file_name="delivery-orders-west-cutover.json", content=west_only_file)
    check("完成态重复导入幂等返回", r_dup2["ok"] is True and r_dup2.get("idempotent") is True)
    check("清单不膨胀", len(svc.list_manifest()) == 3)
    check("无新增会话", len(svc.list_imports()) == 1)

    section("6. 续导完成后再次核对两侧源坐标与未核实记录")
    # 西侧窗口内坐标重放：同文件已确认位点 8821/9044 构成前缀窗口，9000 落在窗口内
    west_inside = svc.replay_check(
        source_instance="orders-west", source_table="orders",
        dedup_key="ORD-7789", binlog_file="mysql-bin.000201",
        binlog_pos=9000, event_id="evt-w-7789a", operation="UPDATE")
    check("西侧窗口内坐标被此前已交付窗口拦截",
          west_inside["下发决定"] == "拦截", west_inside.get("决定原因", ""))
    check("命中历史窗口版本",
          west_inside["窗口版本比对"]["历史窗口版本"] == "orders-west/mysql-bin.000201@9044")

    # 窗口之外的新坐标 9100 晚于全部已确认位点，属于新事件，应可下发
    west_later = svc.replay_check(
        source_instance="orders-west", source_table="orders",
        dedup_key="ORD-7789", binlog_file="mysql-bin.000201",
        binlog_pos=9100, event_id="evt-w-7789b", operation="UPDATE")
    check("已确认窗口之外的新事件准予下发",
          west_later["下发决定"] == "放行", west_later.get("决定原因", ""))

    # 未核实记录（无回执）不能拦截
    west_unverified = svc.replay_check(
        source_instance="orders-west", source_table="orders",
        dedup_key="ORD-7790", binlog_file="mysql-bin.000202",
        binlog_pos=480, event_id="evt-w-7790b", operation="UPDATE")
    check("未核实记录不拦截，准予下发",
          west_unverified["下发决定"] == "放行", west_unverified.get("决定原因", ""))
    check("响应中能看到未核实记录",
          len(west_unverified["未核实记录"]) == 1
          and west_unverified["未核实记录"][0]["回执状态"] == "未核实")

    # 东侧再次到达：西侧回执依旧不能抵扣
    east_again = svc.replay_check(
        source_instance="orders-east", source_table="orders",
        dedup_key="ORD-7789", binlog_file="mysql-bin.000307",
        binlog_pos=5900, event_id="evt-e-7789", operation="INSERT")
    check("东侧事件始终可实际下发", east_again["下发决定"] == "放行")

    section("7. 归并语义：第二个文件对同身份记录做回执升级")
    followup = json.dumps({
        "file_name": "delivery-orders-west-receipts.json",
        "source_instance": "orders-west",
        "records": [
            # 与 ORD-7790 同身份，但这次带回执 → 未核实升级为已确认
            {"source_table": "orders", "dedup_key": "ORD-7790",
             "binlog_file": "mysql-bin.000202", "binlog_pos": 310,
             "receipt_confirmed": True, "event_id": "evt-w-7790",
             "operation": "UPDATE"},
            # 与 ORD-7788 完全重复的已确认条目 → 记重复，不改归属
            {"source_table": "orders", "dedup_key": "ORD-7788",
             "binlog_file": "mysql-bin.000201", "binlog_pos": 8821,
             "receipt_confirmed": True, "event_id": "evt-w-7788",
             "operation": "UPDATE"},
        ],
    }, ensure_ascii=False)
    r3 = svc.import_delivery_file(
        file_name="delivery-orders-west-receipts.json", content=followup)
    check("第二个文件导入完成", r3["ok"] and r3["session"]["状态"] == SESSION_COMPLETED)
    check("1 条归并更新 + 1 条重复",
          r3["session"]["归并更新条数"] == 1 and r3["session"]["重复条数"] == 1
          and r3["session"]["新增条数"] == 0, json.dumps(r3["session"], ensure_ascii=False))
    upgraded = next(r for r in svc.list_manifest() if r["去重键"] == "ORD-7790")
    check("ORD-7790 回执升级为已确认", upgraded["回执状态"] == "已确认")
    check("来源文件保留两个出处", upgraded["来源文件"] == [
        "delivery-orders-west-cutover.json",
        "delivery-orders-west-receipts.json",
    ])
    check("归并次数累计为 2", upgraded["归并次数"] == 2)

    # 升级后同身份重放应被拦截
    west_now_blocked = svc.replay_check(
        source_instance="orders-west", source_table="orders",
        dedup_key="ORD-7790", binlog_file="mysql-bin.000202",
        binlog_pos=310, event_id="evt-w-7790", operation="UPDATE")
    check("回执升级后西侧事件不再派送",
          west_now_blocked["下发决定"] == "拦截")

    section("8. 统计与审计")
    stats = svc.recovery_stats()
    check("清单总数保持 3（归并不新增）", stats["恢复清单总数"] == 3, json.dumps(stats))
    check("已确认回执 3 条", stats["已确认回执数"] == 3)
    check("待核实记录归零", stats["待核实记录数"] == 0)
    check("拦截/放行累计统计与场景一致（3 拦 4 放）",
          stats["累计拦截重放数"] == 3 and stats["累计放行重放数"] == 4, json.dumps(stats))
    check("清单当前处置状态反映最终归属（放行无清单记录可回写，3 条均为拦截）",
          stats["清单当前已拦截数"] == 3 and stats["清单当前已放行数"] == 0)
    audits = [a for a in pipeline_bus.audit_log if a["操作来源"] == "dedup_window"]
    check("导入/续导/重放均有审计轨迹", len(audits) >= 6, f"实际 {len(audits)} 条")

    section("9. 非法文件防护")
    bad1 = svc.import_delivery_file(file_name="x.json", content="{not-json")
    check("非法 JSON 被拒绝", bad1["ok"] is False)
    bad2 = svc.import_delivery_file(file_name="x.json",
                                    content=json.dumps({"records": [{"dedup_key": "k"}]}))
    check("缺必填字段被拒绝", bad2["ok"] is False and "必填" in bad2["message"])
    bad3 = svc.resume_delivery_import("not-exist-fingerprint")
    check("未知指纹续导被拒绝", bad3["ok"] is False)

    print(f"\n结果：通过 {PASS} 项，失败 {FAIL} 项")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
