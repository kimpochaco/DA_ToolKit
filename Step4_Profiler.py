"""
Step 4. 프로파일링 수행 : target DB 스캔 → result DB(PostgreSQL) ENC_* 저장

실행 순서:
    1. DicCollector.collect()        → ENC_DIC_* (메타데이터 수집)
    2. MosuRegister.register()       → ENC_TAB_MAS / ENC_COL_MAS (모수 등록)
    3. Profiler.run_tab()            → ENC_TAB_MAS.TAB_CNT (테이블 건수)
    4. Profiler.run_col()            → ENC_COL_MAS 통계 (컬럼레벨)
    5. Profiler.run_colval()         → ENC_COL_VAL (컬럼값 분포)
    6. Profiler.run_colpat()         → ENC_COL_PAT (패턴 — ENC_COL_VAL 기반)
       Profiler.run_colpat_basic()   → ENC_COL_PAT (패턴 — 타겟 DB 직접 스캔)
"""

from tqdm import tqdm
from Step1_DBConnector import (BaseConnector, OracleConnector, PostgresConnector,
                                MariaDBConnector, MSSQLConnector, DBConFactory, _flavor)
from Step2_DictionaryCollector import DicCollector
from Step3_RegProfilingTarget import MosuRegister


TARGET_ORACLE_SCHEMA = "'E2218030','E2618005'"


class Profiler:
    """
    ENC_TAB_MAS / ENC_COL_MAS 기반으로 target DB에서 프로파일링 수행.
      - 테이블레벨  : COUNT(*) → ENC_TAB_MAS.TAB_CNT
      - 컬럼레벨    : 통계 → ENC_COL_MAS (NOTNULL_CNT, COL_KIND, MAX/MIN_COL_VAL, MAX_LEN)
      - 컬럼값레벨  : 값분포 → ENC_COL_VAL  (COLVAL_EXEC_TYP 설정 컬럼)
      - 컬럼패턴    : ENC_COL_VAL 기반 → ENC_COL_PAT  (run_colpat)
      - 컬럼패턴직접: 타겟 DB 직접 스캔 → ENC_COL_PAT  (run_colpat_basic)

    사용 예시:
        pf = Profiler(target=oracle_conn, result=pg_conn, db_nm="Oracle_DB1")
        pf.run_tab()
        pf.run_col()
        pf.run_colval()
        pf.run_colpat()          # ENC_COL_VAL 결과 기반
        pf.run_colpat_basic()    # 타겟 DB 직접 스캔
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

        핵심: 버킷 GROUP BY를 CROSS JOIN 전에 수행
          1) 버킷 표현식으로 GROUP BY → 테이블 1회 스캔 + 행수 대폭 감소
          2) 소규모 집계 결과에 CROSS JOIN 언피벗 → 최종 GROUP BY
        """
        k = len(cols)
        buckets = [self._val_bucket(cn, dt, et) for cn, dt, et in cols]
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

        # ENC_COL_MAS.SAMPLE_DATA: ENC_COL_VAL 기준 빈도 상위 3개 샘플 값 업데이트
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

        패턴 변환 규칙:
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


# ══════════════════════════════════════════════════════════════
# 실행 예시
# ══════════════════════════════════════════════════════════════
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

    # Step 1 : 딕셔너리 수집
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

    # Step 3/4/5 : 프로파일링
    pf = Profiler(
        target = factory.get("src"),
        result = factory.get("res"),
        db_nm  = "Oracle_DB1",
    )
    pf.run_tab()           # 테이블레벨  : TAB_CNT
    pf.run_col()           # 컬럼레벨    : 통계
    # pf.run_colval()      # 컬럼값레벨  : 값분포 (COLVAL_EXEC_TYP 설정 후 실행)
    # pf.run_colpat()      # 컬럼패턴    : ENC_COL_VAL 기반
    # pf.run_colpat_basic()# 컬럼패턴    : 타겟 DB 직접 스캔

    factory.close_all()
