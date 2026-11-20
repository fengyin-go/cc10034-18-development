"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recovery, resource

# 恢复路由为多段固定路径，先注册以避免被 /api/{module} 动态路径抢先匹配。
ROUTERS = [recovery, resource]
