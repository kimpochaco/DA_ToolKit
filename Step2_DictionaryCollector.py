"""
Step 2. 딕셔너리 수집 : target DB 메타데이터 SELECT → result DB(PostgreSQL) ENC_DIC_* 저장
"""

from tqdm import tqdm
from Step1_DBConnector import BaseConnector, _flavor


class DicCollector:
    """
    target DB(Oracle/PostgreSQL/MySQL/MSSQL)에서 딕셔너리를 수집해
    result DB(PostgreSQL) ENC_DIC_* 테이블에 저장한다.

    사용 예시:
        dc = DicCollector(target=oracle_conn, result=pg_conn, db_nm="Oracle_DB1")
        dc._reg_chasu(chasu=1)
        dc.collect(chasu=1, target_schema="'SKIMES','PACKMES','CELLMES'")
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
              r[3] or "",   # COL_COMMENT   : 컬럼 코멘트
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
                  r[5],        # CONS_TYPE : P/U/R
                  r[4],        # TABLE_NM  : 테이블명
                  r[3])        # POS       : 컬럼 순서
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
              r[0],        # OWNER_NM      : 소유자 스키마
              r[1],        # INDEX_NM      : 인덱스명
              r[2],        # TAB_NM        : 대상 테이블명
              r[3],        # INDEX_TYPE    : BTREE/BITMAP/NORMAL 등
              r[4],        # UNIQUENESS    : UNIQUE/NONUNIQUE
              r[5],        # PART_YN       : 파티션 여부 Y/N
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
