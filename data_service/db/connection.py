"""数据库连接管理

提供线程安全的 SQLite 连接池，
未来可替换为 SQLAlchemy + PostgreSQL。
"""
import sqlite3
import threading
from pathlib import Path
from data_service.config import DATA_SERVICE_DB_PATH


class DatabaseManager:
    """线程安全的 SQLite 数据库管理器"""

    def __init__(self, db_path: Path = DATA_SERVICE_DB_PATH):
        self.db_path = db_path
        self._local = threading.local()

    def get_connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接（懒初始化）"""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30
            )
            self._local.conn.execute('PRAGMA journal_mode=WAL')
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def close(self):
        """关闭当前线程连接"""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


# 全局数据库管理器实例
db_manager = DatabaseManager()
