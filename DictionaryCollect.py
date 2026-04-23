import pandas as pd
from tqdm.notebook import tqdm


class DictionaryCollect:
    """ 프로파일링에 필요한 Dictionary 정보 수집 클래스 """
    def __init__(self):
        self.MandatoryExceptOwnerList = ['SYS', 'SYSCAT', 'SYSGIS', 'SYSTEM', 'OUTLN', ]
    
    def RepositoryDBConnect(self, RepConn):
        """ 리포지터리 DB에 접속한다."""
        self.RepConn = RepConn
        
    def ProfilingDBConnect(self, ProfConn):
        """ 프로파일링을 수행할 데이터베이스 접속한다 """
        self.ProfConn = ProfConn
        
    def ProfilingDBSetting(self, DB번호):
        self.DB번호 = DB번호
        
    def Collect(self, **kwargs):
        """ 지정된 DB에 대한 스키마정보를 수집함"""
        return_type = kwargs['return_type'] if 'return_type' in kwargs.keys() else 'DataFrame' ##기본은 데이터프레임을 리턴함, DB저장, csv 압축파일. dump 지원
        ignore = kwargs['ignore'] if 'ignore' in kwargs.keys() else False ### Repository에 적재할 경우에만 해당되는 것으로, 해당 데이터가 존재할 적재를 시도하지 않음
        
        ### 데이터 존재여부를 체크함
        if return_type == 'Repository':
            DataExistReturnValue = self.RepositoryDataExistCheck(self.DB번호)
            if not DataExistReturnValue and not ignore:
                assert DataExistReturnValue, '해당 데이터 존재, 무시하고자 할 경우 ignore=True 조건 설정 필요!'
        
        ### 데이터를 조회
        SchemaList = self._SchemaSelect(self.DB번호)
        TableList = self._TableSelect(self.DB번호)
        ColumnList = self._ColumnSelect(self.DB번호)
        
        if return_type == 'Repository':            
            ### 리포지터리에서 스키마정보 데이터가 존재할 경우 삭제함
            ### 앞단에서 ignore False일 경우에는 오류가 발생함
            cur = self.RepConn.cursor()
            if not DataExistReturnValue:
                DataDeleteTargetTableList = ['프로파일링_스키마정보', '프로파일링_테이블정보', '프로파일링_컬럼정보']
                DataDeleteSQL = """DELETE {테이블명} WHERE DB번호 = :DB번호"""    
                for TargetTable in DataDeleteTargetTableList:
                    cur.execute(DataDeleteSQL.format(테이블명=TargetTable), self.DB번호)

            ### 데이터 적재
            SchemaInsertSQL = """INSERT INTO 프로파일링_스키마정보 (DB번호, 스키마명)
                                 VALUES (?, ?)"""
            TableInsertSQL = """INSERT INTO 프로파일링_테이블정보 (DB번호, 스키마명, 테이블명, 테이블코멘트, 테이블사이즈, 테이블생성일시)
                                 VALUES (?, ?, ?, ?, ?, ?)"""    
            ColumnInsertSQL = """INSERT INTO 프로파일링_컬럼정보 (DB번호, 스키마명, 테이블명, 컬럼명, 컬럼코멘트, 
                                                         데이터타입, 컬럼순서, NULL여부, PK여부, 길이, 소수점)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, replace(?, 0, null) )"""
            
            [cur.execute(SchemaInsertSQL, SchemaDataRow) for SchemaDataRow in tqdm(SchemaList.values.tolist(), desc='스키마 적재중', ncols=800)]
            [cur.execute(TableInsertSQL, TableDataRow) for TableDataRow in tqdm(TableList.values.tolist(), desc='테이블 적재중', ncols=800)]
            [cur.execute(ColumnInsertSQL, ColumnDataRow) for ColumnDataRow in tqdm(ColumnList.values.tolist(), desc='컬럼 적재중', ncols=800)]
            cur.execute('commit')
        
        ### 데이터 프레임으로 요청할 경우, 값을 리턴시킴
        if return_type == 'DataFrame':
            return SchemaList, TableList, ColumnList
        

    def RepositoryDataExistCheck(self, DB번호):
        """ 리포지터리에 수집대상DB의 데이터가 존재하는지 확인 """
        sql = """SELECT SUM(CNT) 
                   FROM  (SELECT COUNT(*) CNT FROM 프로파일링_스키마정보 WHERE DB번호 = :DB번호 AND ROWNUM = 1
                            UNION ALL
                          SELECT COUNT(*) FROM 프로파일링_테이블정보 WHERE DB번호 = :DB번호 AND ROWNUM = 1
                            UNION ALL
                          SELECT COUNT(*) FROM 프로파일링_컬럼정보 WHERE DB번호 = :DB번호 AND ROWNUM = 1)"""
        
        ReturnValue = True
        cur = self.RepConn.cursor()
        cur.execute(sql, DB번호, DB번호, DB번호)
        rowcount, = cur.fetchone()
        ReturnValue = False if rowcount > 0 else True
        
        return ReturnValue
        
    def _SchemaSelect(self, DB번호):
        """ 스키마정보를 조회한다. """
        MandatoryExceptOwnerList = "', '".join(self.MandatoryExceptOwnerList)
        sql = """SELECT {DB번호} AS DB번호
                      , USERNAME AS 스키마명
                    FROM ALL_USERS 
                    WHERE USERNAME NOT IN ('{MandatoryExceptOwnerList}')""".format(DB번호=DB번호, MandatoryExceptOwnerList=MandatoryExceptOwnerList)
        SchemaList = pd.read_sql(sql, self.ProfConn).astype({'DB번호':int})

        return SchemaList
        
        
    def _TableSelect(self, DB번호):
        """ 테이블정보를 조회한다. """
        MandatoryExceptOwnerList = "', '".join(self.MandatoryExceptOwnerList)
        sql = """SELECT {DB번호} AS DB번호
                      , A.OWNER AS 스키마명
                      , A.TABLE_NAME AS 테이블명
                      , B.COMMENTS AS 테이블코멘트
                      , NVL(C.MBYTES, 0) AS 테이블사이즈
                      , D.CREATED AS 테이블최초생성일시
                   FROM ALL_TABLES A
                      , ALL_TAB_COMMENTS B
                      , (SELECT OWNER, SEGMENT_NAME, SUM(BYTES)/1024/1024 AS MBYTES
                           FROM DBA_SEGMENTS
                          GROUP BY OWNER, SEGMENT_NAME) C
                      , (SELECT OWNER, OBJECT_NAME, MIN(CREATED) AS CREATED
                           FROM DBA_OBJECTS
                          WHERE OBJECT_TYPE LIKE 'TABLE%'
                          GROUP BY OWNER, OBJECT_NAME) D
                  WHERE A.OWNER = B.OWNER
                    AND A.TABLE_NAME = B.TABLE_NAME
                    AND A.OWNER = C.OWNER(+)
                    AND A.TABLE_NAME = C.SEGMENT_NAME(+)
                    AND A.OWNER = D.OWNER(+)
                    AND A.TABLE_NAME = D.OBJECT_NAME(+)
                    AND A.OWNER NOT IN ('{MandatoryExceptOwnerList}')""".format(DB번호=DB번호, MandatoryExceptOwnerList=MandatoryExceptOwnerList)
        TableList = pd.read_sql(sql, self.ProfConn).astype({'DB번호':int})
        TableList['테이블코멘트'] = TableList['테이블코멘트'].fillna("")

        return TableList

    
    def _ColumnSelect(self, DB번호):
        """ 컬럼정보를 조회한다. """
        MandatoryExceptOwnerList = "', '".join(self.MandatoryExceptOwnerList)
        sql = """SELECT {DB번호} AS DB번호
                      , D.OWNER AS 스키마명
                      , D.TABLE_NAME AS 테이블명
                      , D.COLUMN_NAME AS 컬럼명
                      , D.COMMENTS AS 컬럼코멘트
                      , D.DATA_TYPE AS 데이터타입
                      , D.COLUMN_ID AS 컬럼순서
                      , D.NULLABLE AS NULL여부
                      , DECODE(E.OWNER, NULL, NULL, 'Y') AS PK여부
                      , D.DATA_LENGTH AS 길이
                      , D.DATA_SCALE AS 소수점
                  FROM (SELECT A.OWNER, A.TABLE_NAME, A.COLUMN_NAME, B.COMMENTS, A.DATA_TYPE, A.COLUMN_ID, A.NULLABLE,
                               CASE WHEN A.DATA_PRECISION > 0 THEN A.DATA_PRECISION
                                    ELSE A.DATA_LENGTH END AS DATA_LENGTH
                            , CASE WHEN A.DATA_SCALE = 0 THEN NULL ELSE A.DATA_SCALE END AS DATA_SCALE
                            , C.CONSTRAINT_NAME
                          FROM  ALL_TAB_COLUMNS A
                             , ALL_COL_COMMENTS B
                             , ALL_CONSTRAINTS C
                         WHERE A.OWNER = B.OWNER
                           AND A.TABLE_NAME = B.TABLE_NAME
                           AND A.COLUMN_NAME = B.COLUMN_NAME
                           AND A.OWNER = C.OWNER(+)
                           AND A.TABLE_NAME = C.TABLE_NAME(+)
                           AND C.CONSTRAINT_TYPE(+) = 'P'
                           AND A.OWNER NOT IN ('{MandatoryExceptOwnerList}')
                           AND NOT EXISTS (SELECT 'X' 
                                             FROM ALL_VIEWS X
                                             WHERE A.OWNER = X.OWNER
                                               AND A.TABLE_NAME = X.VIEW_NAME)) D
                     , ALL_CONS_COLUMNS E
                  WHERE D.OWNER = E.OWNER(+)
                    AND D.TABLE_NAME = E.TABLE_NAME(+)
                    AND D.COLUMN_NAME = E.COLUMN_NAME(+)
                    AND D.CONSTRAINT_NAME = E.CONSTRAINT_NAME(+)
                    """.format(DB번호=DB번호, MandatoryExceptOwnerList=MandatoryExceptOwnerList)
        ColumnList = pd.read_sql(sql, self.ProfConn).astype({'DB번호':int, '컬럼순서':int, '길이':int,}) #,  '소수점':int
        ColumnList['컬럼코멘트'] = ColumnList['컬럼코멘트'].fillna("")
        ColumnList['PK여부'] = ColumnList['PK여부'].fillna("")
        ColumnList['소수점'] = ColumnList['소수점'].fillna(0)
        
        return ColumnList
