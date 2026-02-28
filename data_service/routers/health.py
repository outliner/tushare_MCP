"""健康检查路由"""
from fastapi import APIRouter
from data_service.db.connection import db_manager
from data_service.models.common import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health_check():
    """健康检查端点"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    return HealthResponse(
        status="healthy",
        db_path=str(db_manager.db_path),
        tables=tables
    )
