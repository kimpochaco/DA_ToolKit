"""
지원 DB : Oracle, PostgreSQL, MSSQL, MariaDB/MySQL
필요 패키지:
    pip install oracledb psycopg2-binary pyodbc pymysql
"""

from __future__ import annotations

import logging
import queue
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("connet_any_DB")


# ══════════════════════════════════════════════════════════════
# 추상 베이스 커넥터
# ══════════════════════════════════════════════════════════════
class BaseConnector(ABC):
    """모든 DB 커넥터의 공통 인터페이스."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @abstractmethod
    def _acquire(self): ...         # 풀에서 커넥션 꺼내기

    @abstractmethod
    def _release(self, conn): ...   # 풀에 커넥션 반납

    @abstractmethod
    def close_pool(self): ...       # 풀 전체 종료

    @abstractmethod
    def test(self) -> bool: ...     # 연결 상태 확인

    # ── 공통 메서드 (모든 DB에서 동일하게 사용) ─────────────
    @contextmanager
    def transaction(self):
        """트랜잭션 — 정상 종료 시 commit, 예외 시 rollback."""
        conn = self._acquire()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("[%s] 롤백: %s", self.conn_id, e)
            raise
        finally:
            self._release(conn)

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List:
        """SELECT 쿼리 실행 후 전체 결과 반환."""
        conn = self._acquire()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
            self._release(conn)

    def execute_dml(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        INSERT / UPDATE / DELETE 실행 후 영향받은 행 수 반환.
        ALL or Nothing
        """

        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.rowcount

    def execute_many(self, query: str, data: List[Tuple]) -> int:
        """
        다건 DML을 일괄 처리.
        ALL or Nothing
        """
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            return cursor.rowcount


# ══════════════════════════════════════════════════════════════
# DB별 커넥터 구현
# ══════════════════════════════════════════════════════════════
class OracleConnector(BaseConnector):
    def __init__(self, conn_id, host, port=1521, service_name="", sid="",
                 user="", password="", min_pool=2, max_pool=10):
        super().__init__(conn_id)
        try:
            import oracledb
        except ImportError:
            raise ImportError("pip install oracledb")

        dsn = oracledb.makedsn(host, port,
                               service_name=service_name if service_name else None,
                               sid=sid if sid else None)
        self._pool = oracledb.create_pool(
            user=user, password=password, dsn=dsn, min=min_pool, max=max_pool, increment=1
        )
        logger.info("[%s] Oracle 풀 생성 → %s:%s", conn_id, host, port)

    def _acquire(self):       return self._pool.acquire()
    def _release(self, conn): self._pool.release(conn)
    def close_pool(self):     self._pool.close(); logger.info("[%s] Oracle 풀 종료", self.conn_id)
    def test(self) -> bool:
        try:
            self.execute_query("SELECT 1 FROM DUAL")
            return True
        except Exception:
            return False


class PostgresConnector(BaseConnector):
    def __init__(self, conn_id, host="localhost", port=5432,
                 database="", user="", password="", min_pool=2, max_pool=10):
        super().__init__(conn_id)
        try:
            from psycopg2 import pool as pg_pool
            self._pool = pg_pool.ThreadedConnectionPool(
                minconn=min_pool, maxconn=max_pool,
                host=host, port=port, dbname=database, user=user, password=password
            )
        except ImportError:
            raise ImportError("pip install psycopg2-binary")
        logger.info("[%s] PostgreSQL 풀 생성 → %s:%s/%s", conn_id, host, port, database)

    def _acquire(self):       return self._pool.getconn()
    def _release(self, conn): self._pool.putconn(conn)
    def close_pool(self):     self._pool.closeall(); logger.info("[%s] PostgreSQL 풀 종료", self.conn_id)
    def test(self) -> bool:
        try:
            self.execute_query("SELECT 1")
            return True
        except Exception:
            return False


class MSSQLConnector(BaseConnector):
    def __init__(self, conn_id, host="localhost", port=1433, database="",
                 user="", password="", driver="ODBC Driver 17 for SQL Server", pool_size=5):
        super().__init__(conn_id)
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pip install pyodbc")

        conn_str = (f"DRIVER={{{driver}}};SERVER={host},{port};"
                    f"DATABASE={database};UID={user};PWD={password};")
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(pyodbc.connect(conn_str, autocommit=False))
        logger.info("[%s] MSSQL 풀 생성(%d) → %s:%s/%s", conn_id, pool_size, host, port, database)

    def _acquire(self):       return self._pool.get(timeout=10)
    def _release(self, conn): self._pool.put(conn)
    def close_pool(self):
        while not self._pool.empty(): self._pool.get_nowait().close()
        logger.info("[%s] MSSQL 풀 종료", self.conn_id)
    def test(self) -> bool:
        try:
            self.execute_query("SELECT 1")
            return True
        except Exception:
            return False


class MariaDBConnector(BaseConnector):
    def __init__(self, conn_id, host="localhost", port=3306, database="",
                 user="", password="", charset="utf8mb4", pool_size=5):
        super().__init__(conn_id)
        try:
            import pymysql
            self._pymysql = pymysql
        except ImportError:
            raise ImportError("pip install pymysql")

        self._config = dict(host=host, port=port, db=database,
                            user=user, password=password, charset=charset,
                            cursorclass=pymysql.cursors.DictCursor, autocommit=False)
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(pymysql.connect(**self._config))
        logger.info("[%s] MariaDB 풀 생성(%d) → %s:%s/%s", conn_id, pool_size, host, port, database)

    def _acquire(self):
        conn = self._pool.get(timeout=10)
        conn.ping(reconnect=True)   # 끊겼으면 자동 재연결
        return conn
    def _release(self, conn): self._pool.put(conn)
    def close_pool(self):
        while not self._pool.empty(): self._pool.get_nowait().close()
        logger.info("[%s] MariaDB 풀 종료", self.conn_id)
    def test(self) -> bool:
        try:
            self.execute_query("SELECT 1")
            return True
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════
# DBConFactory
# ══════════════════════════════════════════════════════════════
class DBConFactory:
    """
    DB 커넥터를 생성·관리하는 DBConFactory.

    같은 DB 타입이라도 conn_id 가 다르면 독립된 풀로 관리됩니다.

    [사용 예시]
        factory = DBConFactory()
        factory.register_oracle("oracle_prod", host="prod.db.com", ...)
        factory.register_postgres("pg_res", host="res.db.com", ...)

        prod = factory.get("oracle_prod")
        rows = prod.execute_query("SELECT * FROM orders")

        factory.close_all()
    """

    def __init__(self):
        self._registry: Dict[str, BaseConnector] = {}

    # ── 등록 ────────────────────────────────────────────────
    def register_oracle(self, conn_id: str, **kwargs) -> OracleConnector:
        return self._add(conn_id, OracleConnector, **kwargs)

    def register_postgres(self, conn_id: str, **kwargs) -> PostgresConnector:
        return self._add(conn_id, PostgresConnector, **kwargs)

    def register_mssql(self, conn_id: str, **kwargs) -> MSSQLConnector:
        return self._add(conn_id, MSSQLConnector, **kwargs)

    def register_mariadb(self, conn_id: str, **kwargs) -> MariaDBConnector:
        return self._add(conn_id, MariaDBConnector, **kwargs)

    def _add(self, conn_id: str, cls, **kwargs) -> BaseConnector:
        if conn_id in self._registry:
            logger.warning("[%s] 이미 등록된 conn_id — 기존 반환", conn_id)
            return self._registry[conn_id]
        connector = cls(conn_id=conn_id, **kwargs)
        self._registry[conn_id] = connector
        return connector

    # ── 조회 / 관리 ─────────────────────────────────────────
    def get(self, conn_id: str) -> BaseConnector:
        if conn_id not in self._registry:
            raise KeyError(f"등록되지 않은 conn_id: '{conn_id}' | 등록 목록: {self.list()}")
        return self._registry[conn_id]

    def list(self) -> List[str]:
        return list(self._registry.keys())

    def health_check(self) -> Dict[str, bool]:
        result = {cid: c.test() for cid, c in self._registry.items()}
        for cid, ok in result.items():
            logger.info("헬스체크 [%s]: %s", cid, "✅ 정상" if ok else "❌ 비정상")
        return result

    def close(self, conn_id: str):
        connector = self._registry.pop(conn_id, None)
        if connector:
            connector.close_pool()

    def close_all(self):
        for connector in self._registry.values():
            connector.close_pool()
        self._registry.clear()
        logger.info("모든 커넥터 종료")


# ══════════════════════════════════════════════════════════════
# 커넥터 종류 판별 유틸리티
# ══════════════════════════════════════════════════════════════
def _flavor(conn: BaseConnector) -> str:
    """커넥터 종류 반환 : 'oracle' | 'postgres' | 'mysql' | 'mssql'"""
    if isinstance(conn, OracleConnector):   return "oracle"
    if isinstance(conn, PostgresConnector): return "postgres"
    if isinstance(conn, MariaDBConnector):  return "mysql"
    if isinstance(conn, MSSQLConnector):    return "mssql"
    return "oracle"


if __name__ == "__main__":
    factory = DBConFactory()
    print("등록된 커넥터:", factory.list())
