-- PACKAGE BODY PKG_HASH_TABLE_COMPARISON (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ARUNN_ADMIN"."PKG_HASH_TABLE_COMPARISON" AS
 
  -----------------------------------------------------------------
  -- Utility: busy wait copied from your existing package
  -----------------------------------------------------------------
  PROCEDURE busy_wait(seconds IN NUMBER) IS
    v_start_time TIMESTAMP := SYSTIMESTAMP;
  BEGIN
    WHILE SYSTIMESTAMP < v_start_time + NUMTODSINTERVAL(seconds, 'SECOND') LOOP
      NULL;
    END LOOP;
  END;
 
  -----------------------------------------------------------------
  -- Hash-based PK-less comparison (called inside each job)
  -----------------------------------------------------------------
  PROCEDURE PROC_COMPARE_SUBSET_TABLES(
    P_SCHEMA_A_NAME      VARCHAR2,
    P_SCHEMA_B_NAME      VARCHAR2,
    P_SCHEMA_A_TABLE     VARCHAR2,
    P_SCHEMA_B_TABLE     VARCHAR2,
    P_DB_LINK            VARCHAR2,
    P_WHERE_CONDITION    VARCHAR2,
    P_ERROR_MSG          OUT VARCHAR2
  ) IS
    V_COL_LIST VARCHAR2(4000);
    V_SQL_B VARCHAR2(4000);
    V_ROW_STRING VARCHAR2(4000);
    V_SQL_CHECK VARCHAR2(4000);
    V_MISMATCH_COUNT NUMBER := 0;
    V_CURSOR SYS_REFCURSOR;
    V_EXISTS NUMBER;
  BEGIN
    -- Mark table as working INSIDE job (like your PK-based package)
    UPDATE TABLE_COMPARISON_CONTROL
       SET RUN_STATUS = 'W'
     WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
       AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
       AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
       AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
    COMMIT;
 
    -- Build deterministic list of comparable columns
    SELECT LISTAGG('NVL(TO_CHAR(' || COLUMN_NAME || '), ''NULL'')', ' || ''|'' || ')
      INTO V_COL_LIST
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = UPPER(P_SCHEMA_B_NAME)
      AND TABLE_NAME = UPPER(P_SCHEMA_B_TABLE)
      AND DATA_TYPE NOT IN ('BLOB', 'CLOB', 'LONG', 'NCLOB');
 
    IF V_COL_LIST IS NULL THEN
      P_ERROR_MSG := 'Error: No comparable columns found.';
      UPDATE TABLE_COMPARISON_CONTROL
         SET RUN_STATUS = 'A', ERROR_CODE = P_ERROR_MSG
       WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
         AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
         AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
         AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
      COMMIT;
      RETURN;
    END IF;
 
    -- Select all rows from Schema B (no filter on B)
    V_SQL_B := 'SELECT ' || V_COL_LIST ||
               ' FROM ' || P_SCHEMA_B_NAME || '.' || P_SCHEMA_B_TABLE;
 
    OPEN V_CURSOR FOR V_SQL_B;
 
    LOOP
      FETCH V_CURSOR INTO V_ROW_STRING;
      EXIT WHEN V_CURSOR%NOTFOUND;
 
      -- Check existence in eligible rows of Schema A
      V_SQL_CHECK := 'SELECT COUNT(*) FROM ' || P_SCHEMA_A_NAME || '.' || P_SCHEMA_A_TABLE || '@' || P_DB_LINK ||
                     ' WHERE (' || V_COL_LIST || ') = ''' || REPLACE(V_ROW_STRING, '''', '''''') || '''';
 
      IF P_WHERE_CONDITION IS NOT NULL THEN
        V_SQL_CHECK := V_SQL_CHECK || ' AND ' || P_WHERE_CONDITION;
      END IF;
 
      EXECUTE IMMEDIATE V_SQL_CHECK INTO V_EXISTS;
 
      IF V_EXISTS = 0 THEN
        IF V_MISMATCH_COUNT < 5 THEN
          INSERT INTO POST_MIG_HASH_ROW_MISMATCHES(SCHEMA_B_NAME, TABLE_B, ROW_DATA_B)
          VALUES (P_SCHEMA_B_NAME, P_SCHEMA_B_TABLE, V_ROW_STRING);
        END IF;
        V_MISMATCH_COUNT := V_MISMATCH_COUNT + 1;
      END IF;
    END LOOP;
 
    CLOSE V_CURSOR;
 
    IF V_MISMATCH_COUNT > 0 THEN
      P_ERROR_MSG := 'Mismatch exists';
      UPDATE TABLE_COMPARISON_CONTROL
         SET RUN_STATUS = 'A', ERROR_CODE = P_ERROR_MSG
       WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
         AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
         AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
         AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
      COMMIT;
    ELSE
      P_ERROR_MSG := 'Completed';
      UPDATE TABLE_COMPARISON_CONTROL
         SET RUN_STATUS = 'C', ERROR_CODE = P_ERROR_MSG
       WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
         AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
         AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
         AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
      COMMIT;
    END IF;
 
  EXCEPTION
    WHEN OTHERS THEN
      P_ERROR_MSG := 'Error: ' || SQLERRM;
      UPDATE TABLE_COMPARISON_CONTROL
         SET RUN_STATUS = 'A', ERROR_CODE = P_ERROR_MSG
       WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
         AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
         AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
         AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
      COMMIT;
  END PROC_COMPARE_SUBSET_TABLES;
 
  -----------------------------------------------------------------
  -- Submission loop identical to your original package logic,
  -- concurrency limit of 2 jobs
  -----------------------------------------------------------------
  PROCEDURE SUBMIT_SUBSET_COMPARISON_JOBS IS
    TYPE TABLE_RECORD IS RECORD(
      SCHEMA_A_NAME     VARCHAR2(100),
      SCHEMA_B_NAME     VARCHAR2(100),
      SCHEMA_A_TABLE    VARCHAR2(100),
      SCHEMA_B_TABLE    VARCHAR2(100),
      DB_LINK           VARCHAR2(100),
      WHERE_CONDITION   VARCHAR2(2000)
    );
    TYPE TABLE_LIST IS TABLE OF TABLE_RECORD;
    V_TABLES       TABLE_LIST;
    V_JOB_COUNT    INTEGER;
    V_PENDING_JOBS INTEGER;
  BEGIN
    LOOP
      -- Count running jobs
      SELECT COUNT(*) INTO V_JOB_COUNT
      FROM TABLE_COMPARISON_CONTROL
      WHERE RUN_STATUS = 'W'
        AND FINAL_STAT = 'CONSIDERED'
        AND REMARKS = 'PK ISSUE';
 
      -- Count pending jobs
      SELECT COUNT(*) INTO V_PENDING_JOBS
      FROM TABLE_COMPARISON_CONTROL
      WHERE RUN_STATUS = 'N'
        AND FINAL_STAT = 'CONSIDERED'
        AND REMARKS = 'PK ISSUE';
 
      -- Exit when nothing pending and nothing running
      EXIT WHEN V_PENDING_JOBS = 0 AND V_JOB_COUNT = 0;
 
      IF V_JOB_COUNT < 2 THEN
        SELECT SCHEMA_A_NAME, SCHEMA_B_NAME, SCHEMA_A_TABLE, SCHEMA_B_TABLE, DB_LINK, WHERE_CONDITION
        BULK COLLECT INTO V_TABLES
        FROM TABLE_COMPARISON_CONTROL
        WHERE RUN_STATUS = 'N'
          AND FINAL_STAT = 'CONSIDERED'
          AND REMARKS = 'PK ISSUE'
        ORDER BY TABLE_SIZE_CATEGORY
        FOR UPDATE SKIP LOCKED;
 
        EXIT WHEN V_TABLES.COUNT = 0;
 
        FOR I IN 1 .. LEAST(2 - V_JOB_COUNT, V_TABLES.COUNT) LOOP
          DBMS_SCHEDULER.CREATE_JOB(
            JOB_NAME   => 'JOB_SUBSET_' || V_TABLES(I).SCHEMA_A_TABLE || '_' || V_TABLES(I).SCHEMA_B_TABLE,
            JOB_TYPE   => 'PLSQL_BLOCK',
            JOB_ACTION => 'DECLARE V_MSG VARCHAR2(4000); BEGIN PKG_HASH_TABLE_COMPARISON.PROC_COMPARE_SUBSET_TABLES(''' ||
                          V_TABLES(I).SCHEMA_A_NAME || ''', ''' ||
                          V_TABLES(I).SCHEMA_B_NAME || ''', ''' ||
                          V_TABLES(I).SCHEMA_A_TABLE || ''', ''' ||
                          V_TABLES(I).SCHEMA_B_TABLE || ''', ''' ||
                          V_TABLES(I).DB_LINK || ''', ''' ||
                          REPLACE(V_TABLES(I).WHERE_CONDITION, '''', '''''') || ''', V_MSG); END;',
            ENABLED    => TRUE
          );
        END LOOP;
      END IF;
 
      busy_wait(10);
    END LOOP;
  END SUBMIT_SUBSET_COMPARISON_JOBS;
 
END PKG_HASH_TABLE_COMPARISON;
/
/
