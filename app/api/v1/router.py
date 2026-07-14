"""v1 路由聚合。"""

from fastapi import APIRouter

from app.api.v1.endpoints import documents


api_router = APIRouter()
api_router.include_router(documents.router)
