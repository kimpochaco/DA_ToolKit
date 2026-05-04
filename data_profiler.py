"""
data_profiler.py  —  데이터 프로파일링

Step 1. 딕셔너리 수집 : target DB 메타데이터 SELECT → result DB(PostgreSQL) ENC_DIC_* 저장
"""

from tqdm import tqdm
from any_db_connector import (BaseConnector, OracleConnector, PostgresConnector,
                               MariaDBConnector, MSSQLConnector, DBConFactory)


TARGET_ORACLE_SCHEMA = "'E2218030','E2618005'"


def _flavor(conn: BaseConnector) -> str:
    """커넥터 종류 반환 : 'oracle' | 'postgres' | 'mysql' | 'mssql'"""
    if isinstance(conn, OracleConnector):   return "oracle"
    if isinstance(conn, PostgresConnector): return "postgres"
    if isinstance(conn, MariaDBConnector):  return "mysql"
    if isinstance(conn, MSSQLConnector):    return "mssql"
    return "oracle"


# ══════════════════════════════════════════════════════════════
# Step 1 : 딕셔너리 수집
# ══════════════════════════════════════════════════════════════
class DicCollector:
    """
    target DB(Oracle/PostgreSQL/MySQL/MSSQL)에서 딕셔너리를 수집해
    result DB(PostgreSQL) ENC_DIC_* 테이블에 저장한다.

    사용 예시:
        dc = DicCollector(target=oracle_conn, result=pg_conn, db_nm="Oracle_DB1")
        dc._reg_chasu(chasu=1)
        dc._collect_tables(chasu=1, target_schema="'SKIMES','PACKMES','CELLMES'")
    """

    def __init__(self, target: BaseConnector, result: BaseConnector, db_nm: str):
        self.target = target
        self.result = result          # 항상 PostgreSQL
        self.db_nm  = db_nm
        self._tg_db    = _flavor(target)

    # ── 차수 등록 (AR_ DB) ──────────────────────────────────
    def _reg_chasu(self, chasu: int):
        sql = 'INSERT INTO ENC_DIC_CHASU_MAS (CHASU, DB_NM, CR_DT) VALUES (%s,%s,NOW())'
        self.result.execute_dml(sql, (chasu, self.db_nm))

    # ── 테이블 수집 ────────────────────────────────────────────
    def _collect_tables(self, chasu: int, target_schema: str):
        # r[0]=OWNER_NM  r[1]=TAB_NM  r[2]=TAB_COMMENT
        # r[3]=TABLESPACE_NM  r[4]=ROW_CNT  r[5]=LAST_ANALYZED_DT  r[6]=PART_YN
        # r[7]=TAB_MB_SIZE  r[8]=TAB_GB_SIZE
        if self._tg_db == "oracle":
            sel = f"""
                SELECT A.OWNER, A.TABLE_NAME, B.COMMENTS,
                       A.TABLESPACE_NAME, A.NUM_ROWS, A.LAST_ANALYZED,
                       CASE WHEN A.PARTITIONED = 'YES' THEN 'Y' ELSE 'N' END,
                       TRUNC(NVL(SUM(S.BYTES), 0) / 1024 / 1024),
                       TRUNC(NVL(SUM(S.BYTES), 0) / 1024 / 1024 / 1024)
                FROM ALL_TABLES A
                LEFT JOIN ALL_TAB_COMMENTS B
                    ON A.OWNER = B.OWNER AND A.TABLE_NAME = B.TABLE_NAME AND B.TABLE_TYPE = 'TABLE'
                LEFT JOIN DBA_SEGMENTS S
                    ON A.OWNER = S.OWNER AND A.TABLE_NAME = S.SEGMENT_NAME
                   AND S.SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION')
                WHERE A.OWNER IN ({target_schema})
                GROUP BY A.OWNER, A.TABLE_NAME, B.COMMENTS,
                         A.TABLESPACE_NAME, A.NUM_ROWS, A.LAST_ANALYZED, A.PARTITIONED
                ORDER BY A.OWNER, A.TABLE_NAME
            """
        elif self._tg_db == "postgres":
            sel = f"""
                SELECT t.table_schema, t.table_name,
                       obj_description(
                           (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass
                       ),
                       ts.spcname,
                       s.n_live_tup,
                       s.last_analyze,
                       'N',
                       TRUNC(pg_total_relation_size(
                           (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass
                       ) / 1024.0 / 1024),
                       TRUNC(pg_total_relation_size(
                           (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass
                       ) / 1024.0 / 1024 / 1024)
                FROM information_schema.tables t
                LEFT JOIN pg_class pc
                    ON pc.relname = t.table_name
                   AND pc.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema)
                LEFT JOIN pg_tablespace ts ON pc.reltablespace = ts.oid
                LEFT JOIN pg_stat_user_tables s
                    ON s.schemaname = t.table_schema AND s.relname = t.table_name
                WHERE t.table_type = 'BASE TABLE'
                  AND t.table_schema IN ({target_schema})
                ORDER BY t.table_schema, t.table_name
            """
        elif self._tg_db == "mysql":
            sel = f"""
                SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_COMMENT,
                       NULL, t.TABLE_ROWS, t.UPDATE_TIME, 'N',
                       ROUND(COALESCE(t.DATA_LENGTH + t.INDEX_LENGTH, 0) / 1024 / 1024, 2),
                       ROUND(COALESCE(t.DATA_LENGTH + t.INDEX_LENGTH, 0) / 1024 / 1024 / 1024, 4)
                FROM information_schema.TABLES t
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND t.TABLE_SCHEMA IN ({target_schema})
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """
        else:  # mssql
            sel = f"""
                SELECT s.name, t.name,
                       CAST(ep.value AS NVARCHAR(MAX)),
                       NULL,
                       SUM(p.rows),
                       NULL,
                       'N',
                       CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)),
                       CAST(SUM(a.total_pages) * 8.0 / 1024 / 1024 AS DECIMAL(10,4))
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.extended_properties ep
                    ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
                LEFT JOIN sys.partitions p
                    ON p.object_id = t.object_id AND p.index_id <= 1
                LEFT JOIN sys.allocation_units a
                    ON a.container_id = p.partition_id
                WHERE s.name IN ({target_schema})
                GROUP BY s.name, t.name, ep.value
                ORDER BY s.name, t.name
            """

        rows = self.target.execute_query(sel)
        if not rows:
            return

        self.result.execute_dml(
            'DELETE FROM ENC_DIC_TAB_MAS WHERE DB_NM=%s AND CHASU=%s',
            (self.db_nm, chasu)
        )
        self.result.execute_many(
            'INSERT INTO ENC_DIC_TAB_MAS '
            '(CHASU,DB_NM,OWNER_NM,TAB_NM,TAB_COMMENT,TABLESPACE_NM,'
            'ROW_CNT,LAST_ANALYZED_DT,PART_YN,TAB_MB_SIZE,TAB_GB_SIZE,CR_DT) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE)',
            [(chasu,       # CHASU
              self.db_nm,  # DB_NM
              r[0],        # OWNER_NM         : 스키마명
              r[1],        # TAB_NM           : 테이블명
              r[2] or "",  # TAB_COMMENT      : 테이블 코멘트
              r[3],        # TABLESPACE_NM    : 테이블스페이스명
              r[4],        # ROW_CNT          : 행 수
              r[5],        # LAST_ANALYZED_DT : 마지막 통계 수집일
              r[6],        # PART_YN          : 파티션 여부 Y/N
              r[7],        # TAB_MB_SIZE      : 크기 MB
              r[8])        # TAB_GB_SIZE      : 크기 GB
             for r in rows]
        )
        print(f"[DIC] 테이블 {len(rows)}건 수집")

    # ── 컬럼 수집 ──────────────────────────────────────────────
    def _collect_columns(self, chasu: int, target_schema: str):
        # r[0]=OWNER_NM  r[1]=TAB_NM  r[2]=COL_NM  r[3]=COL_COMMENT
        # r[4]=COL_SEQ  r[5]=DATA_TYPE  r[6]=DATA_LEN  r[7]=NULL_YN
        # r[8]=DATA_PRECISION  r[9]=DATA_SCALE  r[10]=DEF_VALUE
        if self._tg_db == "oracle":
            sel = f"""
                SELECT A.OWNER, A.TABLE_NAME, A.COLUMN_NAME, B.COMMENTS,
                       A.COLUMN_ID, A.DATA_TYPE,
                       A.DATA_LENGTH, A.NULLABLE,
                       A.DATA_PRECISION, A.DATA_SCALE, A.DATA_DEFAULT
                FROM ALL_TAB_COLUMNS A
                LEFT JOIN ALL_COL_COMMENTS B
                    ON A.OWNER = B.OWNER AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME
                WHERE A.OWNER IN ({target_schema})
                  AND NOT EXISTS (
                      SELECT 'X' FROM ALL_VIEWS V
                      WHERE A.OWNER = V.OWNER AND A.TABLE_NAME = V.VIEW_NAME
                  )
                ORDER BY A.OWNER, A.TABLE_NAME, A.COLUMN_ID
            """
        elif self._tg_db == "postgres":
            sel = f"""
                SELECT c.table_schema, c.table_name, c.column_name, pd.description,
                       c.ordinal_position, c.data_type,
                       COALESCE(c.character_maximum_length, c.numeric_precision, c.datetime_precision),
                       CASE WHEN c.is_nullable = 'YES' THEN 'Y' ELSE 'N' END,
                       c.numeric_precision, c.numeric_scale, c.column_default
                FROM information_schema.columns c
                LEFT JOIN pg_stat_all_tables st
                    ON st.schemaname = c.table_schema AND st.relname = c.table_name
                LEFT JOIN pg_catalog.pg_description pd
                    ON pd.objoid = st.relid AND pd.objsubid = c.ordinal_position
                WHERE c.table_schema IN ({target_schema})
                  AND NOT EXISTS (
                      SELECT 1 FROM information_schema.views v
                      WHERE v.table_schema = c.table_schema AND v.table_name = c.table_name
                  )
                ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """
        elif self._tg_db == "mysql":
            sel = f"""
                SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_COMMENT,
                       c.ORDINAL_POSITION, c.DATA_TYPE,
                       COALESCE(c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.DATETIME_PRECISION),
                       CASE WHEN c.IS_NULLABLE = 'YES' THEN 'Y' ELSE 'N' END,
                       c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.COLUMN_DEFAULT
                FROM information_schema.COLUMNS c
                WHERE c.TABLE_SCHEMA IN ({target_schema})
                  AND NOT EXISTS (
                      SELECT 1 FROM information_schema.VIEWS v
                      WHERE v.TABLE_SCHEMA = c.TABLE_SCHEMA AND v.TABLE_NAME = c.TABLE_NAME
                  )
                ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """
        else:  # mssql
            sel = f"""
                SELECT s.name, t.name, c.name,
                       CAST(ep.value AS NVARCHAR(MAX)),
                       c.column_id, tp.name,
                       COALESCE(c.max_length, c.precision),
                       CASE WHEN c.is_nullable = 1 THEN 'Y' ELSE 'N' END,
                       c.precision, c.scale,
                       CAST(dc.definition AS NVARCHAR(MAX))
                FROM sys.columns c
                JOIN sys.tables t ON c.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                LEFT JOIN sys.extended_properties ep
                    ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
                LEFT JOIN sys.default_constraints dc
                    ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                WHERE s.name IN ({target_schema})
                ORDER BY s.name, t.name, c.column_id
            """

        rows = self.target.execute_query(sel)
        if not rows:
            return

        self.result.execute_dml(
            'DELETE FROM ENC_DIC_COL_MAS WHERE DB_NM=%s AND CHASU=%s',
            (self.db_nm, chasu)
        )
        self.result.execute_many(
            'INSERT INTO ENC_DIC_COL_MAS '
            '(CHASU,DB_NM,OWNER_NM,TAB_NM,COL_NM,COL_COMMENT,'
            'COL_SEQ,DATA_TYPE,DATA_LEN,NULL_YN,DATA_PRECISION,DATA_SCALE,DEF_VALUE,CR_DT) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE)',
            [(chasu,        # CHASU
              self.db_nm,   # DB_NM
              r[0],         # OWNER_NM      : 스키마명
              r[1],         # TAB_NM        : 테이블명
              r[2],         # COL_NM        : 컬럼명
              r[3] or "",   # COL_COMMENT   : 컬럼 코멘트 (ALL_COL_COMMENTS.COMMENTS)
              r[4],         # COL_SEQ       : 컬럼 순서
              r[5],         # DATA_TYPE     : 데이터 타입
              r[6],         # DATA_LEN      : 길이
              r[7],         # NULL_YN       : NULL 허용 여부 Y/N
              r[8],         # DATA_PRECISION: 정밀도
              r[9],         # DATA_SCALE    : 소수점 자릿수
              r[10])        # DEF_VALUE     : 기본값
             for r in rows]
        )
        print(f"[DIC] 컬럼 {len(rows)}건 수집")

    # ── 세그먼트 수집 ──────────────────────────────────────────
    def _collect_segments(self, chasu: int, target_schema: str):

        if self._tg_db == "oracle":
            sel = f"""
                SELECT OWNER,
                    SEGMENT_NAME,
                    NVL(PARTITION_NAME, 'NOPART') AS PARTITION_NAME,
                    SEGMENT_TYPE,
                    TABLESPACE_NAME,
                    0, -- HEAD_BLOCK_CNT (미지원 → 0)
                    NVL(BYTES,0),
                    0  -- BLOCK_CNT (미지원 → 0)
                FROM DBA_SEGMENTS
                WHERE SEGMENT_TYPE IN ('TABLE','TABLE PARTITION','INDEX','INDEX PARTITION')
                AND OWNER IN ({target_schema})
                AND SEGMENT_NAME NOT LIKE 'BIN$%'
            """
        elif self._tg_db == "postgres":
            sel = f"""
                SELECT n.nspname,
                    c.relname,
                    'NOPART',
                    CASE c.relkind
                        WHEN 'r' THEN 'TABLE'
                        WHEN 'p' THEN 'TABLE PARTITION'
                        WHEN 'i' THEN 'INDEX'
                        WHEN 'I' THEN 'INDEX PARTITION'
                    END,
                    NULL,
                    0,
                    pg_relation_size(c.oid),
                    0
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p','i','I')
                AND n.nspname IN ({target_schema})
            """
        else:
            # mysql / mssql 동일 처리
            sel = f"""
                SELECT TABLE_SCHEMA,
                    TABLE_NAME,
                    'NOPART',
                    'TABLE',
                    NULL,
                    0,
                    COALESCE(DATA_LENGTH + INDEX_LENGTH, 0),
                    0
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA IN ({target_schema})
            """

        rows = self.target.execute_query(sel)
        if not rows:
            return

        self.result.execute_dml(
            'DELETE FROM ENC_DIC_SEGMENTS WHERE DB_NM=%s AND CHASU=%s',
            (self.db_nm, chasu)
        )

        self.result.execute_many(
            'INSERT INTO ENC_DIC_SEGMENTS '
            '(CHASU,DB_NM,OWNER_NM,SEGMENT_NM,PARTITION_NM,SEGMENT_TYPE,'
            'TABLESPACE_NM,HEAD_BLOCK_CNT,SEGMENT_SIZE,BLOCK_CNT,CR_DT) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE)',
            [(chasu,       # CHASU
              self.db_nm,  # DB_NM
              r[0],        # OWNER_NM       : 소유자 스키마
              r[1],        # SEGMENT_NM     : 세그먼트(테이블/인덱스)명
              r[2],        # PARTITION_NM   : 파티션명 (없으면 'NOPART')
              r[3],        # SEGMENT_TYPE   : TABLE/TABLE PARTITION/INDEX/INDEX PARTITION
              r[4],        # TABLESPACE_NM  : 테이블스페이스명
              r[5],        # HEAD_BLOCK_CNT : 헤더 블록 수 (미지원 → 0)
              r[6],        # SEGMENT_SIZE   : 세그먼트 크기(BYTES)
              r[7])        # BLOCK_CNT      : 블록 수 (미지원 → 0)
             for r in rows]
        )

        print(f"[DIC] 세그먼트 {len(rows)}건 수집")

    # ── 제약조건 수집 ──────────────────────────────────────────
    def _collect_constraints(self, chasu: int, target_schema: str):

        if self._tg_db == "oracle":
            cons_sel = f"""
                SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                FROM ALL_CONSTRAINTS
                WHERE STATUS = 'ENABLED'
                AND CONSTRAINT_TYPE IN ('P','U','R')
                AND TABLE_NAME NOT LIKE 'BIN$%'
                AND OWNER IN ({target_schema})
            """
            col_sel = f"""
                SELECT A.OWNER,
                    A.CONSTRAINT_NAME,
                    A.COLUMN_NAME,
                    NVL(A.POSITION, 0),
                    B.TABLE_NAME,
                    B.CONSTRAINT_TYPE
                FROM ALL_CONS_COLUMNS A
                JOIN ALL_CONSTRAINTS B
                ON A.OWNER = B.OWNER AND A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
                WHERE B.STATUS = 'ENABLED'
                AND B.CONSTRAINT_TYPE IN ('P','U','R')
                AND A.OWNER IN ({target_schema})
            """
        else:
            cons_sel = f"""
                SELECT tc.table_schema, tc.table_name, tc.constraint_name,
                    CASE tc.constraint_type
                        WHEN 'PRIMARY KEY' THEN 'P'
                        WHEN 'UNIQUE' THEN 'U'
                        WHEN 'FOREIGN KEY' THEN 'R'
                    END
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE','FOREIGN KEY')
                AND tc.table_schema IN ({target_schema})
            """
            col_sel = f"""
                SELECT kcu.table_schema,
                    kcu.constraint_name,
                    kcu.column_name,
                    kcu.ordinal_position,
                    tc.table_name,
                    CASE tc.constraint_type
                        WHEN 'PRIMARY KEY' THEN 'P'
                        WHEN 'UNIQUE' THEN 'U'
                        WHEN 'FOREIGN KEY' THEN 'R'
                    END
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
                WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE','FOREIGN KEY')
                AND kcu.table_schema IN ({target_schema})
            """

        rows = self.target.execute_query(cons_sel)

        if rows:
            self.result.execute_dml(
                'DELETE FROM ENC_DIC_CONS WHERE DB_NM=%s AND CHASU=%s',
                (self.db_nm, chasu)
            )

            self.result.execute_many(
                'INSERT INTO ENC_DIC_CONS '
                '(CHASU,DB_NM,OWNER_NM,CONS_NM,CONS_TYPE,TABLE_NM,INDEX_OWNER_NM,INDEX_NM,CR_DT) '
                'VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,CURRENT_DATE)',
                [(chasu,       # CHASU
                  self.db_nm,  # DB_NM
                  r[0],        # OWNER_NM  : 소유자 스키마
                  r[2],        # CONS_NM   : 제약조건명
                  r[3],        # CONS_TYPE : P/U/R
                  r[1])        # TABLE_NM  : 테이블명
                 for r in rows]
            )

        rows2 = self.target.execute_query(col_sel)

        if rows2:
            self.result.execute_dml(
                'DELETE FROM ENC_DIC_CONS_COL WHERE DB_NM=%s AND CHASU=%s',
                (self.db_nm, chasu)
            )

            self.result.execute_many(
                'INSERT INTO ENC_DIC_CONS_COL '
                '(CHASU,DB_NM,OWNER_NM,CONS_NM,COL_NM,CONS_TYPE,TABLE_NM,POS,CR_DT) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE)',
                [(chasu,       # CHASU
                  self.db_nm,  # DB_NM
                  r[0],        # OWNER_NM  : 소유자 스키마
                  r[1],        # CONS_NM   : 제약조건명
                  r[2],        # COL_NM    : 컬럼명
                  r[5],        # CONS_TYPE : P/U/R (JOIN으로 가져온 타입)
                  r[4],        # TABLE_NM  : 테이블명 (JOIN으로 가져온 테이블)
                  r[3])        # POS       : 컬럼 순서 (NVL POSITION,0)
                 for r in rows2]
            )

        print(f"[DIC] 제약조건 {len(rows or [])}건 / 제약컬럼 {len(rows2 or [])}건 수집")
    # ── 인덱스 수집 ────────────────────────────────────────────
    def _collect_indexes(self, chasu: int, target_schema: str):
        # r[0]=OWNER_NM  r[1]=INDEX_NM  r[2]=TABLE_NM  r[3]=IND_TYPE
        # r[4]=UNIQUENESS  r[5]=PART_YN  r[6]=STATUS  r[7]=TABLESPACE_NM
        if self._tg_db == "oracle":
            sel = f"""
                SELECT OWNER, INDEX_NAME, TABLE_NAME, INDEX_TYPE,
                       UNIQUENESS,
                       CASE WHEN PARTITIONED = 'YES' THEN 'Y' ELSE 'N' END,
                       STATUS, TABLESPACE_NAME
                FROM ALL_INDEXES
                WHERE OWNER IN ({target_schema})
                ORDER BY OWNER, TABLE_NAME, INDEX_NAME
            """
        elif self._tg_db == "postgres":
            sel = f"""
                SELECT n.nspname, i.relname, t.relname,
                       am.amname,
                       CASE WHEN ix.indisunique THEN 'UNIQUE' ELSE 'NONUNIQUE' END,
                       'N',
                       CASE WHEN ix.indisvalid THEN 'VALID' ELSE 'UNUSABLE' END,
                       ts.spcname
                FROM pg_index ix
                JOIN pg_class i  ON i.oid  = ix.indexrelid
                JOIN pg_class t  ON t.oid  = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_am am    ON am.oid  = i.relam
                LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
                WHERE n.nspname IN ({target_schema})
                ORDER BY n.nspname, t.relname, i.relname
            """
        elif self._tg_db == "mysql":
            sel = f"""
                SELECT TABLE_SCHEMA, INDEX_NAME, TABLE_NAME,
                       INDEX_TYPE,
                       CASE WHEN NON_UNIQUE = 0 THEN 'UNIQUE' ELSE 'NONUNIQUE' END,
                       'N',
                       'VALID',
                       NULL
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA IN ({target_schema})
                GROUP BY TABLE_SCHEMA, INDEX_NAME, TABLE_NAME, INDEX_TYPE, NON_UNIQUE
                ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
            """
        else:  # mssql
            sel = f"""
                SELECT s.name, i.name, t.name,
                       i.type_desc,
                       CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE 'NONUNIQUE' END,
                       'N',
                       CASE WHEN i.is_disabled = 0 THEN 'VALID' ELSE 'UNUSABLE' END,
                       NULL
                FROM sys.indexes i
                JOIN sys.tables t  ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name IN ({target_schema})
                  AND i.name IS NOT NULL
                ORDER BY s.name, t.name, i.name
            """

        rows = self.target.execute_query(sel)
        if not rows:
            return

        self.result.execute_dml(
            'DELETE FROM ENC_DIC_IND_MAS WHERE DB_NM=%s AND CHASU=%s',
            (self.db_nm, chasu)
        )
        self.result.execute_many(
            'INSERT INTO ENC_DIC_IND_MAS '
            '(CHASU,DB_NM,OWNER_NM,INDEX_NM,TAB_NM,INDEX_TYPE,UNIQUENESS,PART_YN,TABLESPACE_NM,CR_DT) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE)',
            [(chasu,       # CHASU
              self.db_nm,  # DB_NM
              r[0],        # OWNER_NM    : 소유자 스키마
              r[1],        # INDEX_NM      : 인덱스명
              r[2],        # TAB_NM    : 대상 테이블명
              r[3],        # INDEX_TYPE    : BTREE/BITMAP/NORMAL 등
              r[4],        # UNIQUENESS  : UNIQUE/NONUNIQUE
              r[5],        # PART_YN     : 파티션 여부 Y/N
              r[6])        # TABLESPACE_NM : 테이블스페이스명
             for r in rows]
        )
        print(f"[DIC] 인덱스 {len(rows)}건 수집")

    # ── 전체 실행 ──────────────────────────────────────────────
    def collect(self, chasu: int, target_schema: str):
        """
        딕셔너리 전체 수집.

        chasu         : 차수 (호출 전 _reg_chasu로 등록)
        target_schema : 수집할 스키마(owner) 목록 — SQL IN 절 형식의 문자열
                        예) "'SKIMES','PACKMES','CELLMES'"
        """
        print(f"[Step1] 시작 — db_nm={self.db_nm}  chasu={chasu}  target={self._tg_db}  schema={target_schema}")
        steps = [
            ("테이블",   self._collect_tables),
            ("컬럼",     self._collect_columns),
            ("세그먼트", self._collect_segments),
            ("제약조건", self._collect_constraints),
            ("인덱스",   self._collect_indexes),
        ]
        with tqdm(steps, desc="[Step1] 딕셔너리 수집", unit="항목", ncols=80) as pbar:
            for name, fn in pbar:
                pbar.set_postfix_str(name)
                fn(chasu, target_schema)
        print(f"[Step1] 완료 — chasu={chasu}")


# ══════════════════════════════════════════════════════════════
# Step 2 : 모수 등록 (ENC_DIC_* → ENC_TAB_MAS / ENC_COL_MAS)
# ══════════════════════════════════════════════════════════════
class MosuRegister:
    """
    ENC_DIC_* (result DB) → ENC_TAB_MAS / ENC_COL_MAS UPSERT.

    전제 조건 (PostgreSQL DDL):
        ALTER TABLE ENC_TAB_MAS ADD CONSTRAINT uq_tab_mas UNIQUE (DB_NM, OWNER_NM, TAB_NM);
        ALTER TABLE ENC_COL_MAS ADD CONSTRAINT uq_col_mas UNIQUE (DB_NM, OWNER_NM, TAB_NM, COL_NM);

    사용 예시:
        mr = MosuRegister(result=pg_conn, db_nm="Oracle_DB1")
        mr.register(target_schema="'E2218030','E2618005'")
    """

    def __init__(self, result: BaseConnector, db_nm: str, prfl_id: str = 'P01'):
        self.result  = result
        self.db_nm   = db_nm
        self.prfl_id = prfl_id

    def _get_max_chasu(self) -> int:
        rows = self.result.execute_query(
            'SELECT MAX(CHASU) FROM ENC_DIC_CHASU_MAS WHERE DB_NM = %s',
            (self.db_nm,)
        )
        val = rows[0][0] if rows and rows[0][0] is not None else 0
        return int(val)

    def _reg_tab_mas(self, chasu: int, target_schema: str):
        sql = f"""
            INSERT INTO ENC_TAB_MAS
                (PRFL_ID, DB_NM, OWNER_NM, TAB_NM, TAB_COMMENT, USE_YN, PART_TAB_YN,
                 CHASU, CR_DT, COL_CNT, TAB_SIZE, EXEC_DOP, INDEX_CNT, IND_SIZE, TAB_WORKGRP_NO)
            SELECT
                %s,
                A.DB_NM, A.OWNER_NM, A.TAB_NM, A.TAB_COMMENT, 'Y',
                A.PART_YN,
                A.CHASU,
                NOW(),
                (SELECT COUNT(*) FROM ENC_DIC_COL_MAS C
                 WHERE C.CHASU = {chasu} AND C.DB_NM = %s
                   AND C.OWNER_NM = A.OWNER_NM AND C.TAB_NM = A.TAB_NM),
                COALESCE(B.MB, 0),
                CASE WHEN COALESCE(B.MB,0) >= 100000 THEN 16
                     WHEN COALESCE(B.MB,0) >= 50000  THEN 8
                     WHEN COALESCE(B.MB,0) >= 100    THEN 4
                     ELSE 2 END,
                COALESCE(C.IND_CNT, 0),
                COALESCE(C.IND_SIZE, 0),
                MOD(ROW_NUMBER() OVER(ORDER BY COALESCE(B.MB,0) DESC), 3) + 1
            FROM ENC_DIC_TAB_MAS A
            LEFT JOIN (
                SELECT OWNER_NM, SEGMENT_NM,
                       FLOOR(SUM(SEGMENT_SIZE) / 1024.0 / 1024) AS MB
                FROM ENC_DIC_SEGMENTS
                WHERE SEGMENT_TYPE IN ('TABLE','TABLE PARTITION')
                  AND OWNER_NM IN ({target_schema})
                  AND DB_NM = %s AND CHASU = {chasu}
                GROUP BY OWNER_NM, SEGMENT_NM
            ) B ON B.OWNER_NM = A.OWNER_NM AND B.SEGMENT_NM = A.TAB_NM
            LEFT JOIN (
                SELECT S.OWNER_NM, S.SEGMENT_NM AS TAB_NM,
                       COALESCE(MAX(I.IND_CNT), 0) AS IND_CNT,
                       FLOOR(SUM(S.SEGMENT_SIZE) / 1024.0 / 1024) AS IND_SIZE
                FROM ENC_DIC_SEGMENTS S
                LEFT JOIN (
                    SELECT OWNER_NM, TAB_NM, COUNT(*) AS IND_CNT
                    FROM ENC_DIC_IND_MAS
                    WHERE OWNER_NM IN ({target_schema})
                      AND DB_NM = %s AND CHASU = {chasu}
                    GROUP BY OWNER_NM, TAB_NM
                ) I ON I.OWNER_NM = S.OWNER_NM AND I.TAB_NM = S.SEGMENT_NM
                WHERE S.SEGMENT_TYPE = 'INDEX'
                  AND S.OWNER_NM IN ({target_schema})
                  AND S.DB_NM = %s AND S.CHASU = {chasu}
                GROUP BY S.OWNER_NM, S.SEGMENT_NM
            ) C ON C.OWNER_NM = A.OWNER_NM AND C.TAB_NM = A.TAB_NM
            WHERE A.DB_NM = %s
              AND A.OWNER_NM IN ({target_schema})
              AND A.CHASU = {chasu}
            ON CONFLICT (DB_NM, OWNER_NM, TAB_NM) DO UPDATE SET
                PART_TAB_YN    = EXCLUDED.PART_TAB_YN,
                PRFL_ID        = EXCLUDED.PRFL_ID,
                USE_YN         = EXCLUDED.USE_YN,
                UP_DT          = NOW(),
                CHASU          = EXCLUDED.CHASU,
                COL_CNT        = EXCLUDED.COL_CNT,
                TAB_SIZE       = EXCLUDED.TAB_SIZE,
                EXEC_DOP       = EXCLUDED.EXEC_DOP,
                INDEX_CNT      = EXCLUDED.INDEX_CNT,
                IND_SIZE       = EXCLUDED.IND_SIZE,
                TAB_WORKGRP_NO = EXCLUDED.TAB_WORKGRP_NO,
                TAB_COMMENT    = EXCLUDED.TAB_COMMENT
        """
        # %s 순서: prfl_id, db_nm(COL_CNT), db_nm(seg B), db_nm(idx I), db_nm(seg C), db_nm(outer WHERE)
        cnt = self.result.execute_dml(
            sql, (self.prfl_id, self.db_nm, self.db_nm, self.db_nm, self.db_nm, self.db_nm)
        )
        print(f"[모수] ENC_TAB_MAS UPSERT {cnt}건")

    def _reg_col_mas(self, chasu: int, target_schema: str):
        sql = f"""
            INSERT INTO ENC_COL_MAS
                (PRFL_ID, DB_NM, OWNER_NM, TAB_NM, COL_NM, COL_COMMENT, USE_YN,
                 CHASU, COL_SEQ, COL_WORKGRP_NO, PK_COL_SEQ,
                 COL_DATA_TYPE, COL_DATA_LENGTH, NULL_YN, COL_DATA_PRECISION, COL_DATA_SCALE, CR_DT)
            SELECT
                %s,
                A.DB_NM, A.OWNER_NM, A.TAB_NM, A.COL_NM,
                A.COL_COMMENT, 'Y',
                A.CHASU,
                A.COL_SEQ,
                FLOOR(A.COL_SEQ / 30) + 1,
                C.POS,
                A.DATA_TYPE,
                A.DATA_LEN,
                A.NULL_YN,
                A.DATA_PRECISION,
                A.DATA_SCALE,
                NOW()
            FROM ENC_DIC_COL_MAS A
            LEFT JOIN ENC_DIC_CONS B
                ON B.CHASU = A.CHASU AND B.DB_NM = A.DB_NM AND B.OWNER_NM = A.OWNER_NM
               AND B.TABLE_NM = A.TAB_NM AND B.CONS_TYPE = 'P'
            LEFT JOIN ENC_DIC_CONS_COL C
                ON C.CHASU = B.CHASU AND C.DB_NM = B.DB_NM AND C.OWNER_NM = B.OWNER_NM
               AND C.CONS_NM = B.CONS_NM AND C.COL_NM = A.COL_NM
            WHERE A.DB_NM = %s
              AND A.OWNER_NM IN ({target_schema})
              AND A.CHASU = {chasu}
            ON CONFLICT (DB_NM, OWNER_NM, TAB_NM, COL_NM) DO UPDATE SET
                CHASU              = EXCLUDED.CHASU,
                COL_SEQ            = EXCLUDED.COL_SEQ,
                COL_WORKGRP_NO     = EXCLUDED.COL_WORKGRP_NO,
                COL_DATA_TYPE      = EXCLUDED.COL_DATA_TYPE,
                COL_DATA_LENGTH    = EXCLUDED.COL_DATA_LENGTH,
                NULL_YN            = EXCLUDED.NULL_YN,
                COL_DATA_PRECISION = EXCLUDED.COL_DATA_PRECISION,
                COL_DATA_SCALE     = EXCLUDED.COL_DATA_SCALE,
                PK_COL_SEQ         = EXCLUDED.PK_COL_SEQ,
                UP_DT              = NOW(),
                COL_COMMENT        = EXCLUDED.COL_COMMENT
        """
        # %s 순서: prfl_id, db_nm
        cnt = self.result.execute_dml(sql, (self.prfl_id, self.db_nm))
        print(f"[모수] ENC_COL_MAS UPSERT {cnt}건")

        # 문자형 컬럼에 COLVAL_EXEC_TYP 기본값 'Y' 자동 설정 (미설정 컬럼만)
        # 대상: CHAR / VARCHAR / NCHAR / NVARCHAR / VARCHAR2 / NVARCHAR2 / CHARACTER VARYING 등
        upd = self.result.execute_dml(
            """
            UPDATE ENC_COL_MAS
               SET COLVAL_EXEC_TYP = 'Y', UP_DT = NOW()
             WHERE DB_NM   = %s
               AND PRFL_ID = %s
               AND COLVAL_EXEC_TYP IS NULL
               AND (UPPER(COL_DATA_TYPE) LIKE '%%CHAR%%'
                    OR UPPER(COL_DATA_TYPE) = 'STRING')
            """,
            (self.db_nm, self.prfl_id)
        )
        print(f"[모수] 문자형 컬럼 COLVAL_EXEC_TYP 기본설정 {upd}건")

        # 문자형 컬럼에 COLPAT_EXEC_TYP 기본값 'Y' 자동 설정 (미설정 컬럼만)
        # 대상: CHAR / VARCHAR / NCHAR / NVARCHAR / VARCHAR2 / NVARCHAR2 / CHARACTER VARYING 등
        upd = self.result.execute_dml(
            """
            UPDATE ENC_COL_MAS
               SET COLPAT_EXEC_TYP = 'Y', UP_DT = NOW()
             WHERE DB_NM   = %s
               AND PRFL_ID = %s
               AND COLPAT_EXEC_TYP IS NULL
               AND (UPPER(COL_DATA_TYPE) LIKE '%%CHAR%%'
                    OR UPPER(COL_DATA_TYPE) = 'STRING')
            """,
            (self.db_nm, self.prfl_id)
        )
        print(f"[모수] 문자형 컬럼 COLVAL_EXEC_TYP 기본설정 {upd}건")


    def register(self, target_schema: str):
        """ENC_DIC_* → ENC_TAB_MAS / ENC_COL_MAS UPSERT."""
        chasu = self._get_max_chasu()
        if chasu == 0:
            raise ValueError(f"[{self.db_nm}] ENC_DIC_CHASU_MAS 등록 차수 없음")
        print(f"[Step2] 시작 — db_nm={self.db_nm}  chasu={chasu}")
        steps = [
            ("ENC_TAB_MAS", self._reg_tab_mas),
            ("ENC_COL_MAS", self._reg_col_mas),
        ]
        with tqdm(steps, desc="[Step2] 모수 등록", unit="테이블", ncols=80) as pbar:
            for name, fn in pbar:
                pbar.set_postfix_str(name)
                fn(chasu, target_schema)
        print(f"[Step2] 완료")


# ══════════════════════════════════════════════════════════════
# Step 3/4 : 프로파일링 수행
# ══════════════════════════════════════════════════════════════
class Profiler:
    """
    ENC_TAB_MAS / ENC_COL_MAS 기반으로 target DB에서 프로파일링 수행.
      - 테이블레벨 : COUNT(*) → ENC_TAB_MAS.TAB_CNT
      - 컬럼레벨   : 통계 → ENC_COL_MAS (NOTNULL_CNT, COL_KIND, MAX/MIN_COL_VAL, MAX_LEN)
      - 컬럼값레벨 : 값분포 → ENC_COL_VAL  (do_col_val=True 시 활성화)

    사용 예시:
        pf = Profiler(target=oracle_conn, result=pg_conn, db_nm="Oracle_DB1")
        pf.run()                  # 테이블/컬럼 레벨
        pf.run(do_col_val=True)   # 컬럼값 레벨까지
    """

    # LOB / 비정형 타입 — 통계 제외 (모두 대문자, .upper() 비교 전제)
    _LOB_TYPES = frozenset({
        'TEXT', 'NTEXT', 'IMAGE', 'XML', 'XMLTYPE',
        'BLOB', 'CLOB', 'NCLOB', 'BFILE', 'LONG', 'LONG RAW', 'RAW',
        'BYTEA', 'JSON', 'JSONB',
    })

    def __init__(self, target: BaseConnector, result: BaseConnector,
                 db_nm: str, prfl_id: str = 'P01'):
        self.target  = target
        self.result  = result
        self.db_nm   = db_nm
        self.prfl_id = prfl_id
        self._tg_db  = _flavor(target)

    # ── 식별자 인용부호 ──────────────────────────────────────────
    def _qi(self, name: str) -> str:
        if self._tg_db == "mssql":
            return f"[{name}]"
        return f'"{name}"'

    def _oi(self, owner: str, tab: str) -> str:
        return f"{self._qi(owner)}.{self._qi(tab)}"

    # ── 테이블 레벨 ─────────────────────────────────────────────
    def _profile_tab_cnt(self, owner: str, tab: str) -> int:
        rows = self.target.execute_query(
            f"SELECT COUNT(*) FROM {self._oi(owner, tab)}"
        )
        return int(rows[0][0]) if rows else 0

    # ── 컬럼 레벨 (30컬럼 배치 — 테이블 1회 스캔) ───────────────
    def _profile_cols_batch(self, owner: str, tab: str, cols: list):
        """cols : list of (col_nm, data_type)"""
        if not cols:
            return
        ot = self._oi(owner, tab)

        for i in range(0, len(cols), 30):
            batch = cols[i : i + 30]
            parts, col_names = [], []

            for col_nm, _ in batch:
                qc = self._qi(col_nm)
                col_names.append(col_nm)
                if self._tg_db == "oracle":
                    cast = f"TO_CHAR({qc})"
                    llen = f"NVL(MAX(LENGTH(TO_CHAR({qc}))), 0)"
                elif self._tg_db == "mssql":
                    cast = f"CONVERT(NVARCHAR(MAX), {qc})"
                    llen = f"ISNULL(MAX(LEN(CONVERT(NVARCHAR(MAX), {qc}))), 0)"
                else:  # postgres / mysql
                    cast = f"CAST({qc} AS TEXT)"
                    llen = f"COALESCE(MAX(LENGTH(CAST({qc} AS TEXT))), 0)"
                parts += [
                    f"COUNT({qc})",
                    f"COUNT(DISTINCT {qc})",
                    f"MAX({cast})",
                    f"MIN({cast})",
                    llen,
                ]

            rows = self.target.execute_query(
                f"SELECT {', '.join(parts)} FROM {ot}"
            )
            if not rows:
                continue

            row = rows[0]
            upd = []
            for j, col_nm in enumerate(col_names):
                b = j * 5
                upd.append((
                    row[b],                                            # NOTNULL_CNT
                    row[b+1],                                          # COL_KIND
                    str(row[b+2])[:4000] if row[b+2] is not None else None,  # MAX_COL_VAL
                    str(row[b+3])[:4000] if row[b+3] is not None else None,  # MIN_COL_VAL
                    row[b+4],                                          # MAX_LEN
                    self.db_nm, owner, tab, col_nm, self.prfl_id
                ))
            self.result.execute_many(
                'UPDATE ENC_COL_MAS '
                'SET NOTNULL_CNT=%s, COL_KIND=%s, MAX_COL_VAL=%s, MIN_COL_VAL=%s, MAX_LEN=%s, UP_DT=NOW() '
                'WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND COL_NM=%s AND PRFL_ID=%s',
                upd
            )

    # ── 컬럼값 레벨 ─────────────────────────────────────────────
    def _val_bucket(self, col_nm: str, data_type: str, colval_exec_typ: str) -> str:
        """COLVAL_EXEC_TYP 에 따른 값 버킷 표현식 반환."""
        qc = self._qi(col_nm)
        dt = (data_type or '').upper()
        is_num = any(k in dt for k in
                     ('INT', 'FLOAT', 'DECIMAL', 'NUMERIC', 'DOUBLE',
                      'MONEY', 'NUMBER', 'BINARY_FLOAT', 'BINARY_DOUBLE'))

        _range_case = (
            f"CASE WHEN {qc} IS NULL THEN '##'"
            f" WHEN {{cast}} = 0 THEN '0'"
            f" WHEN {{cast}} BETWEEN 1 AND 100 THEN '1~100'"
            f" WHEN {{cast}} BETWEEN 101 AND 1000 THEN '101~1000'"
            f" WHEN {{cast}} BETWEEN 1001 AND 10000 THEN '1001~10000'"
            f" WHEN {{cast}} BETWEEN 10001 AND 100000 THEN '10001~100000'"
            f" WHEN {{cast}} BETWEEN 100001 AND 1000000 THEN '100001~1000000'"
            f" WHEN {{cast}} < 0 THEN '0미만' ELSE '1000000초과' END"
        )

        if self._tg_db == "oracle":
            null_str = f"NVL(TO_CHAR({qc}), '##')"
            if colval_exec_typ == 'B' and is_num:
                return _range_case.format(cast=qc)
            if colval_exec_typ in ('A', 'B'):
                return f"SUBSTR({null_str}, 1, 6)"
            return f"SUBSTR({null_str}, 1, 100)"

        if self._tg_db == "postgres":
            null_str = f"COALESCE(CAST({qc} AS TEXT), '##')"
        elif self._tg_db == "mssql":
            null_str = f"ISNULL(CONVERT(NVARCHAR(MAX), {qc}), '##')"
        else:  # mysql
            null_str = f"COALESCE(CAST({qc} AS CHAR), '##')"

        if colval_exec_typ == 'B' and is_num:
            return _range_case.format(cast=f"CAST({qc} AS DECIMAL)")
        if colval_exec_typ in ('A', 'B'):
            return f"SUBSTRING({null_str}, 1, 6)"
        return f"SUBSTRING({null_str}, 1, 100)"

    def _build_colval_sql(self, ot: str, cols: list) -> str:
        """2단계 최적화 CROSS JOIN unpivot — (col_nm, col_val, cnt) 반환.

        핵심: 버킷 GROUP BY를 CROSS JOIN 전에 수행 (첨부 SQL 03.04 방식)
          1) 버킷 표현식으로 GROUP BY → 테이블 1회 스캔 + 행수 대폭 감소
          2) 소규모 집계 결과에 CROSS JOIN 언피벗 → 최종 GROUP BY

        기존 문제:
          원본 N행 × CROSS JOIN k배 → k×N행을 GROUP BY  (느림)
        수정 후:
          버킷 GROUP BY → M행(M≪N) → CROSS JOIN k배 → k×M행 GROUP BY  (빠름)
        """
        k = len(cols)
        buckets = [self._val_bucket(cn, dt, et) for cn, dt, et in cols]
        # v1..vk: 위치 기반 별칭 (예약어·컬럼명 충돌 방지)
        va = [f"v{i+1}" for i in range(k)]

        # 1단계: 버킷 표현식 집계 (서브쿼리로 별칭 확정 → GROUP BY 별칭 참조 가능)
        pre_inner = ", ".join(f"{b} AS {a}" for b, a in zip(buckets, va))
        pre_agg = (
            f"SELECT {', '.join(va)}, COUNT(*) AS cnt "
            f"FROM (SELECT {pre_inner} FROM {ot}) b "
            f"GROUP BY {', '.join(va)}"
        )

        # 2단계: 언피벗 CASE — pre_agg의 v1..vk 컬럼 참조
        nm_case = "CASE n " + " ".join(
            f"WHEN {i+1} THEN '{cn}'" for i, (cn, _, _) in enumerate(cols)
        ) + " END AS col_nm"
        val_case = "CASE n " + " ".join(
            f"WHEN {i+1} THEN {a}" for i, a in enumerate(va)
        ) + " END AS col_val"

        outer = (
            f"SELECT col_nm, col_val, SUM(cnt) AS cnt "
            f"FROM (SELECT {nm_case}, {val_case}, cnt "
            f"FROM ({pre_agg}) pre CROSS JOIN {{nums}}) u "
            f"WHERE col_nm IS NOT NULL "
            f"GROUP BY col_nm, col_val"
        )

        if self._tg_db == "oracle":
            return outer.format(
                nums=f"(SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= {k}) nums"
            )
        if self._tg_db == "postgres":
            return outer.format(nums=f"generate_series(1, {k}) AS nums(n)")
        if self._tg_db == "mssql":
            vals = ", ".join(f"({i+1})" for i in range(k))
            return outer.format(nums=f"(VALUES {vals}) AS nums(n)")
        # mysql/mariadb — WITH RECURSIVE는 최상위에만 허용
        cte = (f"WITH RECURSIVE nums AS "
               f"(SELECT 1 AS n UNION ALL SELECT n+1 FROM nums WHERE n < {k})")
        return (
            f"{cte} "
            f"SELECT col_nm, col_val, SUM(cnt) AS cnt "
            f"FROM (SELECT {nm_case}, {val_case}, cnt "
            f"FROM ({pre_agg}) pre CROSS JOIN nums) u "
            f"WHERE col_nm IS NOT NULL "
            f"GROUP BY col_nm, col_val"
        )

    # ── 공통 : 대상 테이블 목록 조회 ────────────────────────────
    def _get_tables(self) -> list:
        rows = self.result.execute_query(
            "SELECT OWNER_NM, TAB_NM FROM ENC_TAB_MAS "
            "WHERE DB_NM=%s AND PRFL_ID=%s AND USE_YN='Y' "
            "ORDER BY OWNER_NM, TAB_NM",
            (self.db_nm, self.prfl_id)
        )
        if not rows:
            print("[Profiler] 대상 없음. MosuRegister.register() 를 먼저 실행하세요.")
        return rows or []

    # ── Step 3-1 : 테이블레벨 (TAB_CNT) ─────────────────────────
    def run_tab(self):
        """COUNT(*) → ENC_TAB_MAS.TAB_CNT / TABCNT_STR_DT / TABCNT_END_DT"""
        from datetime import datetime
        tables = self._get_tables()
        if not tables:
            return
        for owner, tab in tqdm(tables, desc="[Step3] 테이블레벨", unit="테이블", ncols=80):
            str_dt  = datetime.now()
            tab_cnt = self._profile_tab_cnt(owner, tab)
            end_dt  = datetime.now()
            self.result.execute_dml(
                'UPDATE ENC_TAB_MAS '
                'SET TAB_CNT=%s, TABCNT_STR_DT=%s, TABCNT_END_DT=%s, UP_DT=NOW() '
                'WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND PRFL_ID=%s',
                (tab_cnt, str_dt, end_dt, self.db_nm, owner, tab, self.prfl_id)
            )
        print(f"[Step3] 테이블레벨 완료 — {len(tables)}건")

    # ── Step 3-2 : 컬럼레벨 (통계) ──────────────────────────────
    def run_col(self):
        """NOTNULL_CNT / COL_KIND / MAX_COL_VAL / MIN_COL_VAL / MAX_LEN → ENC_COL_MAS"""
        tables = self._get_tables()
        if not tables:
            return
        for owner, tab in tqdm(tables, desc="[Step3] 컬럼레벨", unit="테이블", ncols=80):
            cols_stat = self.result.execute_query(
                "SELECT COL_NM, COL_DATA_TYPE FROM ENC_COL_MAS "
                "WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND PRFL_ID=%s AND USE_YN='Y' "
                "ORDER BY COL_SEQ",
                (self.db_nm, owner, tab, self.prfl_id)
            )
            stat_cols = [(r[0], r[1]) for r in cols_stat
                         if (r[1] or '').upper() not in self._LOB_TYPES]
            self._profile_cols_batch(owner, tab, stat_cols)
        print(f"[Step3] 컬럼레벨 완료 — {len(tables)}건")

    # ── Step 3-3 : 컬럼값레벨 (값분포) ─────────────────────────
    def run_colval(self):
        """COLVAL_EXEC_TYP 설정 컬럼 값분포 → ENC_COL_VAL
        tqdm을 배치 단위로 진행 — 어떤 테이블·컬럼을 처리 중인지 실시간 표시.
        """
        tables = self._get_tables()
        if not tables:
            return

        # 컬럼 목록 미리 조회 → 전체 배치 수 확정 (tqdm total)
        tab_cols: dict = {}
        for owner, tab in tables:
            rows = self.result.execute_query(
                "SELECT COL_NM, COL_DATA_TYPE, COLVAL_EXEC_TYP FROM ENC_COL_MAS "
                "WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND PRFL_ID=%s "
                "AND COLVAL_EXEC_TYP IN ('Y','A','B') AND USE_YN='Y' ORDER BY COL_SEQ",
                (self.db_nm, owner, tab, self.prfl_id)
            )
            val_cols = [(r[0], r[1], r[2]) for r in rows
                        if (r[1] or '').upper() not in self._LOB_TYPES]
            if val_cols:
                tab_cols[(owner, tab)] = val_cols

        total_batches = sum((len(v) + 29) // 30 for v in tab_cols.values())

        with tqdm(total=total_batches, desc="[Step4] 컬럼값레벨",
                  unit="배치", ncols=120) as pbar:
            for (owner, tab), val_cols in tab_cols.items():
                ot = self._oi(owner, tab)
                self.result.execute_dml(
                    'DELETE FROM ENC_COL_VAL '
                    'WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND PRFL_ID=%s',
                    (self.db_nm, owner, tab, self.prfl_id)
                )
                total_cols = len(val_cols)
                for i in range(0, total_cols, 30):
                    batch = val_cols[i:i+30]
                    pbar.set_postfix_str(
                        f"{owner}.{tab}  {i+1}~{min(i+30, total_cols)}/{total_cols}",
                        refresh=True
                    )
                    rows = self.target.execute_query(
                        self._build_colval_sql(ot, batch)
                    )
                    if rows:
                        self.result.execute_many(
                            'INSERT INTO ENC_COL_VAL '
                            '(DB_NM, OWNER_NM, TAB_NM, COL_NM, COL_VAL, '
                            'COL_VAL_CNT, MIN_REG_DT, MAX_REG_DT, PRFL_ID) '
                            'VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,%s)',
                            [(self.db_nm, owner, tab, r[0],
                              str(r[1])[:4000] if r[1] is not None else '##',
                              r[2], self.prfl_id)
                             for r in rows]
                        )
                    pbar.update(1)

        # ENC_COL_MAS.SAMPLE_VAL: ENC_COL_VAL 기준 빈도 상위 3개 샘플 값 업데이트
        self.result.execute_dml(
            """
            UPDATE ENC_COL_MAS m
               SET SAMPLE_DATA = s.vals,
                   UP_DT      = NOW()
              FROM (
                    SELECT DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID,
                           STRING_AGG(COL_VAL, ' | ' ORDER BY COL_VAL_CNT DESC) AS vals
                      FROM (
                            SELECT DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID,
                                   COL_VAL, COL_VAL_CNT,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID
                                       ORDER BY COL_VAL_CNT DESC
                                   ) AS rn
                              FROM ENC_COL_VAL
                             WHERE DB_NM = %s AND PRFL_ID = %s
                               AND COL_VAL != '##'
                           ) ranked
                     WHERE rn <= 3
                     GROUP BY DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID
                   ) s
             WHERE m.DB_NM    = s.DB_NM
               AND m.OWNER_NM = s.OWNER_NM
               AND m.TAB_NM   = s.TAB_NM
               AND m.COL_NM   = s.COL_NM
               AND m.PRFL_ID  = s.PRFL_ID
            """,
            (self.db_nm, self.prfl_id)
        )
        print(f"[Step4] 컬럼값레벨 완료 — {len(tab_cols)}테이블 / {total_batches}배치 / SAMPLE_DATA 업데이트 완료")

    # ── Step 5 : 컬럼패턴 (ENC_COL_VAL → ENC_COL_PAT) ──────────
    def run_colpat(self):
        """ENC_COL_VAL 기반 패턴분석 → ENC_COL_PAT
        타겟 DB 스캔 없음 — result DB(PostgreSQL) 내부 연산만 수행.

        패턴 변환 규칙 (첨부 SQL 기준):
          숫자     → N
          대문자   → C
          소문자   → c
          한글     → 가
          점(.)   → *
          공백     → #
          기타특수 → $
        """
        print("[Step5] 컬럼패턴 시작 …", flush=True)
        self.result.execute_dml(
            'DELETE FROM ENC_COL_PAT WHERE DB_NM=%s AND PRFL_ID=%s',
            (self.db_nm, self.prfl_id)
        )
        # ENC_COL_VAL → 패턴 변환 집계 → COL_PAT_RT(비율) 윈도우 함수까지 한 번에
        cnt = self.result.execute_dml(
            """
            INSERT INTO ENC_COL_PAT
                (DB_NM, OWNER_NM, TAB_NM, COL_NM, COL_PAT, COL_PAT_CNT, COL_PAT_RT, PRFL_ID)
            SELECT DB_NM, OWNER_NM, TAB_NM, COL_NM,
                   COL_PAT,
                   COL_PAT_CNT,
                   ROUND(COL_PAT_CNT * 100.0 / NULLIF(
                       SUM(COL_PAT_CNT) OVER (
                           PARTITION BY DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID
                       ), 0), 2)::TEXT AS COL_PAT_RT,
                   PRFL_ID
              FROM (
                    SELECT DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID,
                           COL_PAT, SUM(COL_VAL_CNT) AS COL_PAT_CNT
                      FROM (
                            SELECT DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID, COL_VAL_CNT,
                                REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                      REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                          REGEXP_REPLACE(
                                            REGEXP_REPLACE(COL_VAL, '[0-9]', 'N', 'g'),
                                          '[A-Z]', 'C', 'g'),
                                        '[a-z]', 'c', 'g'),
                                      '[가-힣]', '가', 'g'),
                                    '[.]', '*', 'g'),
                                  ' ', '#', 'g'),
                                '[^NCc가*#]', '$', 'g') AS COL_PAT
                            FROM ENC_COL_VAL
                            WHERE COL_VAL != '##'
                              AND DB_NM = %s
                              AND PRFL_ID = %s
                           ) pat_src
                     GROUP BY DB_NM, OWNER_NM, TAB_NM, COL_NM, COL_PAT, PRFL_ID
                   ) agg
            """,
            (self.db_nm, self.prfl_id)
        )
        # ENC_COL_MAS.TOP_COL_PAT : COL_PAT_CNT 최다 패턴 업데이트
        self.result.execute_dml(
            """
            UPDATE ENC_COL_MAS m
               SET TOP_COL_PAT = top_pat.COL_PAT,
                   UP_DT       = NOW()
              FROM (
                    SELECT DISTINCT ON (DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID)
                           DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID, COL_PAT
                      FROM ENC_COL_PAT
                     WHERE DB_NM = %s AND PRFL_ID = %s
                     ORDER BY DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID,
                              COL_PAT_CNT DESC
                   ) top_pat
             WHERE m.DB_NM    = top_pat.DB_NM
               AND m.OWNER_NM = top_pat.OWNER_NM
               AND m.TAB_NM   = top_pat.TAB_NM
               AND m.COL_NM   = top_pat.COL_NM
               AND m.PRFL_ID  = top_pat.PRFL_ID
            """,
            (self.db_nm, self.prfl_id)
        )
        print(f"[Step5] 컬럼패턴 완료 — {cnt}건 / ENC_COL_MAS.TOP_COL_PAT 업데이트 완료")

    # ── Step 5B : 컬럼패턴 직접 (타겟 DB 스캔) ──────────────────
    def _build_colpat_basic_sql(self, ot: str, col_nm: str) -> str:
        """타겟 DB 컬럼 직접 스캔 → 패턴 집계 SQL — (pat_val, cnt) 반환."""
        qc = self._qi(col_nm)
        if self._tg_db == 'oracle':
            expr = (
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"SUBSTR(NVL(TO_CHAR({qc}), '##'), 1, 100),"
                f" '[0-9]', 'N'), '[A-Z]', 'C'), '[a-z]', 'c'),"
                f" '[가-힣]', '가'), '\\.', '*'), ' ', '#'), '[^NCc가*#]', '$')"
            )
        elif self._tg_db == 'postgres':
            expr = (
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"SUBSTRING(COALESCE(CAST({qc} AS TEXT), '##'), 1, 100),"
                f" '[0-9]', 'N', 'g'), '[A-Z]', 'C', 'g'), '[a-z]', 'c', 'g'),"
                f" '[가-힣]', '가', 'g'), '[.]', '*', 'g'), ' ', '#', 'g'),"
                f" '[^NCc가*#]', '$', 'g')"
            )
        elif self._tg_db == 'mysql':
            expr = (
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
                f"SUBSTRING(COALESCE(CAST({qc} AS CHAR), '##'), 1, 100),"
                f" '[0-9]', 'N'), '[A-Z]', 'C'), '[a-z]', 'c'),"
                f" '[가-힣]', '가'), '[.]', '*'), ' ', '#'), '[^NCc가*#]', '$')"
            )
        else:  # mssql — raw distinct values, Python-side 패턴 변환
            return (
                f"SELECT SUBSTRING(ISNULL(CONVERT(NVARCHAR(500), {qc}), '##'), 1, 100) AS pat_val,"
                f" COUNT(*) AS cnt FROM {ot} WHERE {qc} IS NOT NULL"
                f" GROUP BY SUBSTRING(ISNULL(CONVERT(NVARCHAR(500), {qc}), '##'), 1, 100)"
            )
        return (f"SELECT {expr} AS pat_val, COUNT(*) AS cnt "
                f"FROM {ot} WHERE {qc} IS NOT NULL GROUP BY {expr}")

    def run_colpat_basic(self):
        """COLPAT_EXEC_TYP='Y' 컬럼을 타겟 DB에서 직접 스캔 → 패턴분석 → ENC_COL_PAT.
        ENC_COL_VAL 없이 원본 테이블에서 바로 패턴을 집계한다.
        """
        tables = self._get_tables()
        if not tables:
            return

        tab_cols: dict = {}
        for owner, tab in tables:
            rows = self.result.execute_query(
                "SELECT COL_NM, COL_DATA_TYPE FROM ENC_COL_MAS "
                "WHERE DB_NM=%s AND OWNER_NM=%s AND TAB_NM=%s AND PRFL_ID=%s "
                "AND COLPAT_EXEC_TYP='Y' AND USE_YN='Y' ORDER BY COL_SEQ",
                (self.db_nm, owner, tab, self.prfl_id)
            )
            pat_cols = [(r[0], r[1]) for r in rows
                        if (r[1] or '').upper() not in self._LOB_TYPES]
            if pat_cols:
                tab_cols[(owner, tab)] = pat_cols

        if not tab_cols:
            print("[Step5B] COLPAT_EXEC_TYP=Y 컬럼 없음")
            return

        print("[Step5B] 컬럼패턴(직접) 시작 …", flush=True)
        self.result.execute_dml(
            'DELETE FROM ENC_COL_PAT WHERE DB_NM=%s AND PRFL_ID=%s',
            (self.db_nm, self.prfl_id)
        )

        total_cols = sum(len(v) for v in tab_cols.values())
        ins_total = 0

        with tqdm(total=total_cols, desc="[Step5B] 컬럼패턴(직접)",
                  unit="컬럼", ncols=120) as pbar:
            for (owner, tab), pat_cols in tab_cols.items():
                ot = self._oi(owner, tab)
                for col_nm, _ in pat_cols:
                    pbar.set_postfix_str(f"{owner}.{tab}.{col_nm}", refresh=True)
                    sql = self._build_colpat_basic_sql(ot, col_nm)
                    rows = self.target.execute_query(sql)

                    if rows:
                        if self._tg_db == 'mssql':
                            import re as _re
                            def _pat(v: str) -> str:
                                v = _re.sub(r'[0-9]', 'N', v[:100])
                                v = _re.sub(r'[A-Z]', 'C', v)
                                v = _re.sub(r'[a-z]', 'c', v)
                                v = _re.sub(r'[가-힣]', '가', v)
                                v = _re.sub(r'[.]', '*', v)
                                v = _re.sub(r' ', '#', v)
                                v = _re.sub(r'[^NCc가*#]', '$', v)
                                return v
                            agg: dict = {}
                            for rv, cnt in rows:
                                p = _pat(str(rv)) if rv else '##'
                                agg[p] = agg.get(p, 0) + int(cnt)
                            pat_rows = list(agg.items())
                        else:
                            pat_rows = [(r[0], r[1]) for r in rows]

                        total_cnt = sum(c for _, c in pat_rows)
                        data = [
                            (self.db_nm, owner, tab, col_nm,
                             pat, cnt,
                             str(round(cnt * 100.0 / total_cnt, 2)) if total_cnt else '0',
                             self.prfl_id)
                            for pat, cnt in pat_rows
                        ]
                        self.result.execute_many(
                            'INSERT INTO ENC_COL_PAT '
                            '(DB_NM, OWNER_NM, TAB_NM, COL_NM, COL_PAT, COL_PAT_CNT, COL_PAT_RT, PRFL_ID) '
                            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                            data
                        )
                        ins_total += len(data)
                    pbar.update(1)

        self.result.execute_dml(
            """
            UPDATE ENC_COL_MAS m
               SET TOP_COL_PAT = top_pat.COL_PAT,
                   UP_DT       = NOW()
              FROM (
                    SELECT DISTINCT ON (DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID)
                           DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID, COL_PAT
                      FROM ENC_COL_PAT
                     WHERE DB_NM = %s AND PRFL_ID = %s
                     ORDER BY DB_NM, OWNER_NM, TAB_NM, COL_NM, PRFL_ID,
                              COL_PAT_CNT DESC
                   ) top_pat
             WHERE m.DB_NM    = top_pat.DB_NM
               AND m.OWNER_NM = top_pat.OWNER_NM
               AND m.TAB_NM   = top_pat.TAB_NM
               AND m.COL_NM   = top_pat.COL_NM
               AND m.PRFL_ID  = top_pat.PRFL_ID
            """,
            (self.db_nm, self.prfl_id)
        )
        print(f"[Step5B] 컬럼패턴(직접) 완료 — {ins_total}건 / TOP_COL_PAT 업데이트 완료")


# ── 실행 예시 ──────────────────────────────────────────────────
if __name__ == "__main__":
    factory = DBConFactory()
    factory.register_oracle(
        "src", host="172.16.105.117", port=1521,
        service_name="PDB1", user="E2218030", password="password"
    )
    factory.register_postgres(
        "res", host="172.16.105.117", port=5432,
        database="postgres", user="e2218030", password="p2218030"
    )

    dc = DicCollector(
        target = factory.get("src"),
        result = factory.get("res"),
        db_nm  = "Oracle_DB1",
    )
    dc._reg_chasu(chasu=1)
    dc.collect(chasu=1, target_schema=TARGET_ORACLE_SCHEMA)

    # Step 2 : 모수 등록
    mr = MosuRegister(result=factory.get("res"), db_nm="Oracle_DB1")
    mr.register(target_schema=TARGET_ORACLE_SCHEMA)

    # Step 3/4 : 프로파일링
    pf = Profiler(
        target = factory.get("src"),
        result = factory.get("res"),
        db_nm  = "Oracle_DB1",
    )
    pf.run_tab()      # 테이블레벨 : TAB_CNT
    pf.run_col()      # 컬럼레벨  : 통계
    # pf.run_colval() # 컬럼값레벨 : 값분포 (COLVAL_EXEC_TYP 설정 후 실행)

    factory.close_all()
