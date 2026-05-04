"""
Step 3. 모수 등록 : ENC_DIC_* → ENC_TAB_MAS / ENC_COL_MAS UPSERT
"""

from tqdm import tqdm
from Step1_DBConnector import BaseConnector


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
        print(f"[모수] 문자형 컬럼 COLPAT_EXEC_TYP 기본설정 {upd}건")

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
