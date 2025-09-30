-- PROCEDURE PROC_COMPARE_SCHEMAS_TABLES2_BKP_160925_DO_NOT_MODIFY (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PROC_COMPARE_SCHEMAS_TABLES2_BKP_160925_DO_NOT_MODIFY" (V_SCHEMA_A     IN VARCHAR2,
                                                         V_SCHEMA_B     IN VARCHAR2,
                                                         V_COMN_COLS    IN VARCHAR2,
                                                         V_TABLE_NAME_A IN VARCHAR2,
                                                         V_TABLE_NAME_B IN VARCHAR2,
                                                         V_DB_LINK      IN VARCHAR2,
                                                         V_ERROR_MSG    OUT VARCHAR2) IS
  V_TABLE_EXISTS_A NUMBER := 0;
  V_TABLE_EXISTS_B NUMBER := 0;
  V_ERR_STACK      VARCHAR2(32767);
  V_ERR_BT         VARCHAR2(32767);

  TYPE COLUMN_REC IS RECORD(
    COLUMN_NAME VARCHAR2(128),
    DATA_TYPE   VARCHAR2(128));
  TYPE COLUMN_TAB IS TABLE OF COLUMN_REC;

  V_COLUMNS      COLUMN_TAB;
  V_SQL_STRING   VARCHAR2(4000);
  V_COUNT        NUMBER;
  V_PK_CONCAT    VARCHAR2(4000);
  V_ON_CONDITION VARCHAR2(4000);
  V_CURSOR       SYS_REFCURSOR;

  TYPE FETCH_REC IS RECORD(
    PK_VAL  VARCHAR2(4000),
    VALUE_A VARCHAR2(4000),
    VALUE_B VARCHAR2(4000));
  V_FETCH_REC FETCH_REC;

  V_COUNT_A NUMBER := 0;
  V_COUNT_B NUMBER := 0;
  V_SQLERRM VARCHAR2(2000);
  V_SQLCODE VARCHAR2(2000);

  V_TOTAL_MISMATCH NUMBER := 0; -- NEW: aggregate mismatches across columns
BEGIN
  -----------------------------------------------------------------
  -- Table existence checks
  -----------------------------------------------------------------
  V_SQL_STRING := 'SELECT COUNT(*) FROM ALL_TABLES@' || V_DB_LINK ||
                  ' WHERE TABLE_NAME = ''' || UPPER(V_TABLE_NAME_A) ||
                  ''' AND OWNER = ''' || UPPER(V_SCHEMA_A) || '''';
  EXECUTE IMMEDIATE V_SQL_STRING
    INTO V_TABLE_EXISTS_A;

  SELECT COUNT(*)
    INTO V_TABLE_EXISTS_B
    FROM ALL_TABLES
   WHERE TABLE_NAME = UPPER(V_TABLE_NAME_B)
     AND OWNER = UPPER(V_SCHEMA_B);

  IF V_TABLE_EXISTS_A = 0 THEN
    V_ERROR_MSG := 'Error: Table ' || V_SCHEMA_A || '.' || V_TABLE_NAME_A ||
                   ' does not exist';
    RETURN;
  ELSIF V_TABLE_EXISTS_B = 0 THEN
    V_ERROR_MSG := 'Error: Table ' || V_SCHEMA_B || '.' || V_TABLE_NAME_B ||
                   ' does not exist';
    RETURN;
  END IF;

  -----------------------------------------------------------------
  -- Row counts
  -----------------------------------------------------------------
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || V_SCHEMA_A || '.' ||
                    V_TABLE_NAME_A || '@' || V_DB_LINK
    INTO V_COUNT_A;
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || V_SCHEMA_B || '.' ||
                    V_TABLE_NAME_B
    INTO V_COUNT_B;

  IF V_COUNT_A = 0 AND V_COUNT_B = 0 THEN
    V_ERROR_MSG := 'No rows to compare';
    RETURN;
  ELSIF V_COUNT_A = 0 AND V_COUNT_B != 0 THEN
    V_ERROR_MSG := 'V12  table is empty';
    RETURN;
  ELSIF V_COUNT_A != 0 AND V_COUNT_B = 0 THEN
    V_ERROR_MSG := 'V14  table is empty';
    RETURN;
  END IF;

  -----------------------------------------------------------------
  -- Prepare PK concat & join condition
  -----------------------------------------------------------------
  V_PK_CONCAT    := 'A.' || REPLACE(V_COMN_COLS, '~', ' || ''~'' || A.');
  V_ON_CONDITION := '';
  FOR COL IN (SELECT REGEXP_SUBSTR(V_COMN_COLS, '[^~]+', 1, LEVEL) AS COL_NAME
                FROM DUAL
              CONNECT BY REGEXP_SUBSTR(V_COMN_COLS, '[^~]+', 1, LEVEL) IS NOT NULL) LOOP
    IF LENGTH(V_ON_CONDITION) > 0 THEN
      V_ON_CONDITION := V_ON_CONDITION || ' AND ';
    END IF;
    V_ON_CONDITION := V_ON_CONDITION || 'A.' || COL.COL_NAME || ' = B.' ||
                      COL.COL_NAME;
  END LOOP;

  -----------------------------------------------------------------
  -- Fetch columns to compare
  -----------------------------------------------------------------
  V_SQL_STRING := 'SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS@' ||
                  V_DB_LINK || ' WHERE TABLE_NAME = ''' ||
                  UPPER(V_TABLE_NAME_A) || '''' || '   AND OWNER = ''' ||
                  UPPER(V_SCHEMA_A) || '''' ||
                  '   AND COLUMN_NAME NOT IN (' ||
                  '       SELECT REGEXP_SUBSTR(''' || V_COMN_COLS ||
                  ''', ''[^~]+'', 1, LEVEL)' || '         FROM DUAL' ||
                  '       CONNECT BY REGEXP_SUBSTR(''' || V_COMN_COLS ||
                  ''', ''[^~]+'', 1, LEVEL) IS NOT NULL' || '   )' ||
                  '   AND COLUMN_NAME IN (' ||
                  '       SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS' ||
                  '        WHERE TABLE_NAME = ''' || UPPER(V_TABLE_NAME_B) || '''' ||
                  '          AND OWNER = ''' || UPPER(V_SCHEMA_B) || '''' ||
                  '   )';
  EXECUTE IMMEDIATE V_SQL_STRING BULK COLLECT
    INTO V_COLUMNS;

  -----------------------------------------------------------------
  -- Compare each column with error handling
  -----------------------------------------------------------------
  FOR I IN 1 .. V_COLUMNS.COUNT LOOP
    V_COUNT := 0;
    BEGIN
      -- Skip and log unsupported types
      IF V_COLUMNS(I).DATA_TYPE IN ('BLOB', 'CLOB', 'LONG', 'NCLOB') THEN
        INSERT INTO POST_MIG_COLUMN_ERRORS
          (TABLE_NAME, COLUMN_NAME, ERROR_MSG)
        VALUES
          (V_TABLE_NAME_A,
           V_COLUMNS(I).COLUMN_NAME,
           'Skipped: Unsupported datatype ' || V_COLUMNS(I).DATA_TYPE);
        COMMIT;
        CONTINUE;
      END IF;
    
      -- Build comparison SQL
      IF V_COLUMNS(I).DATA_TYPE = 'DATE' THEN
        -- Pre-filter invalid dates to avoid ORA-01847
                     --changing parallel to 8 on 070925
        V_SQL_STRING := 'SELECT ' || V_PK_CONCAT ||
                        ', TO_CHAR(A.' || V_COLUMNS(I).COLUMN_NAME ||
                        ', ''YYYY-MM-DD HH24:MI:SS''), TO_CHAR(B.' || V_COLUMNS(I).COLUMN_NAME ||
                        ', ''YYYY-MM-DD HH24:MI:SS'') ' || 'FROM ' ||
                        V_SCHEMA_A || '.' || V_TABLE_NAME_A || '@' ||
                        V_DB_LINK || ' A JOIN ' || V_SCHEMA_B || '.' ||
                        V_TABLE_NAME_B || ' B ON ' || V_ON_CONDITION ||
                        ' WHERE (A.' || V_COLUMNS(I).COLUMN_NAME ||
                        ' IS NULL OR A.' || V_COLUMNS(I).COLUMN_NAME ||
                        ' BETWEEN TO_DATE(''1900-01-01'',''YYYY-MM-DD'') AND TO_DATE(''9999-12-31'',''YYYY-MM-DD''))' ||
                        ' AND (B.' || V_COLUMNS(I).COLUMN_NAME ||
                        ' IS NULL OR B.' || V_COLUMNS(I).COLUMN_NAME ||
                        ' BETWEEN TO_DATE(''1900-01-01'',''YYYY-MM-DD'') AND TO_DATE(''9999-12-31'',''YYYY-MM-DD''))' ||
                        ' AND NVL(A.' || V_COLUMNS(I).COLUMN_NAME ||
                        ', TO_DATE(''1900-01-01'',''YYYY-MM-DD'')) <> NVL(B.' || V_COLUMNS(I).COLUMN_NAME ||
                        ', TO_DATE(''1900-01-01'',''YYYY-MM-DD''))';
      ELSE
        -- Use TO_CHAR to avoid ORA-01722 (invalid number)
                     --changing parallel to 8 on 070925
        V_SQL_STRING := 'SELECT  ' || V_PK_CONCAT ||
                        ', TO_CHAR(A.' || V_COLUMNS(I).COLUMN_NAME ||
                        '), TO_CHAR(B.' || V_COLUMNS(I).COLUMN_NAME || ')' ||
                        ' FROM ' || V_SCHEMA_A || '.' || V_TABLE_NAME_A || '@' ||
                        V_DB_LINK || ' A JOIN ' || V_SCHEMA_B || '.' ||
                        V_TABLE_NAME_B || ' B ON ' || V_ON_CONDITION ||
                        ' WHERE NVL(TO_CHAR(A.' || V_COLUMNS(I).COLUMN_NAME ||
                        '), ''NULL'') <> NVL(TO_CHAR(B.' || V_COLUMNS(I).COLUMN_NAME ||
                        '), ''NULL'')';
      END IF;
    
      OPEN V_CURSOR FOR V_SQL_STRING;
      LOOP
        FETCH V_CURSOR
          INTO V_FETCH_REC.PK_VAL, V_FETCH_REC.VALUE_A, V_FETCH_REC.VALUE_B;
        EXIT WHEN V_CURSOR%NOTFOUND;
      
        IF V_COUNT < 5 THEN
          INSERT INTO POST_MIG_MIS_MATCH_COLS
            (TABLE_NAME, COLUMN_NAME, COMMON_COLUMN_VAL, VALUE_A, VALUE_B)
          VALUES
            (V_TABLE_NAME_A,
             V_COLUMNS(I).COLUMN_NAME,
             V_FETCH_REC.PK_VAL,
             V_FETCH_REC.VALUE_A,
             V_FETCH_REC.VALUE_B);
          COMMIT;
        END IF;
      
        V_COUNT := V_COUNT + 1;
      END LOOP;
      CLOSE V_CURSOR;
    
      IF V_COUNT > 0 THEN
        INSERT INTO POST_MIG_MIS_MATCH_TAB
          (TABLE_NAME, COLUMN_NAME, MISMATCH_COUNT, QUERY_STRING)
        VALUES
          (V_TABLE_NAME_A, V_COLUMNS(I).COLUMN_NAME, V_COUNT, V_SQL_STRING);
        COMMIT;
      
        V_TOTAL_MISMATCH := V_TOTAL_MISMATCH + V_COUNT; -- NEW: accumulate
      END IF;
    
    EXCEPTION
      WHEN OTHERS THEN
        V_SQLCODE := SQLCODE;
        V_SQLERRM := SQLERRM;
        INSERT INTO POST_MIG_COLUMN_ERRORS
          (TABLE_NAME, COLUMN_NAME, ERROR_MSG)
        VALUES
          (V_TABLE_NAME_A,
           V_COLUMNS(I).COLUMN_NAME,
           'SQLCODE=' || TO_CHAR(V_SQLCODE) || ' | STACK=' ||
           SUBSTR(V_ERR_STACK, 1, 2000) || ' | BT=' ||
           SUBSTR(V_ERR_BT, 1, 2000) || ' | SQL=' ||
           SUBSTR(V_SQL_STRING, 1, 500));
        COMMIT;
    END;
  END LOOP;

  -----------------------------------------------------------------
  -- Update control table note if any column errors (unchanged)
  -----------------------------------------------------------------
  DECLARE
    V_ERR_COUNT NUMBER;
  BEGIN
    SELECT COUNT(*)
      INTO V_ERR_COUNT
      FROM POST_MIG_COLUMN_ERRORS
     WHERE TABLE_NAME = V_TABLE_NAME_A
     and error_msg not  like 'Skipped: Unsupported datatype%' ;
    IF V_ERR_COUNT > 0 THEN
      V_ERROR_MSG := 'FAILED:COL_ERRORS(' || V_ERR_COUNT || ')';
      RETURN; -- bail out if any column has errors
    END IF;
  END;

  -----------------------------------------------------------------
  -- Final message encodes C vs M (interpreted by PROCESS_TABLE)
  -----------------------------------------------------------------
  IF V_TOTAL_MISMATCH > 0 THEN
    V_ERROR_MSG := 'COMPLETED:MISMATCH';
  ELSE
    V_ERROR_MSG := 'COMPLETED:NOMISMATCH';
  END IF;

EXCEPTION
  WHEN OTHERS THEN
    V_ERROR_MSG := 'Error: ' || SQLERRM;
    ROLLBACK;
END;
/
/
