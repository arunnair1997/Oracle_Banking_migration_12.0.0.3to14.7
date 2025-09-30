-- PACKAGE PKG_TABLE_COMPARISON (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PACKAGE "ARUNN_ADMIN"."PKG_TABLE_COMPARISON" AS

    -- Procedure to log execution details
    PROCEDURE LOG_EXECUTION (
        P_JOB_NAME      VARCHAR2,
        P_SCHEMA_A_NAME VARCHAR2,
        P_SCHEMA_B_NAME VARCHAR2,
        P_SCHEMA_A_TABLE VARCHAR2,
        P_SCHEMA_B_TABLE VARCHAR2,
        P_STATUS        VARCHAR2,
        P_ERROR_MESSAGE VARCHAR2 DEFAULT NULL
    );

    -- Procedure to process individual table comparison
    PROCEDURE PROCESS_TABLE (
        P_SCHEMA_A_NAME  VARCHAR2,
        P_SCHEMA_B_NAME  VARCHAR2,
        P_SCHEMA_A_TABLE VARCHAR2,
        P_SCHEMA_B_TABLE VARCHAR2,
        P_DB_LINK VARCHAR2,--db link changes
        P_COMMON_COLUMNS VARCHAR2
    );

    -- Procedure to submit parallel jobs for table comparison
    PROCEDURE SUBMIT_COMPARISON_JOBS;

    -- Procedure to monitor job execution status
    PROCEDURE MONITOR_JOBS;

END PKG_TABLE_COMPARISON;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ARUNN_ADMIN"."PKG_TABLE_COMPARISON" AS
  -- Logging Procedure
  PROCEDURE LOG_EXECUTION(P_JOB_NAME       VARCHAR2,
                          P_SCHEMA_A_NAME  VARCHAR2,
                          P_SCHEMA_B_NAME  VARCHAR2,
                          P_SCHEMA_A_TABLE VARCHAR2,
                          P_SCHEMA_B_TABLE VARCHAR2,
                          P_STATUS         VARCHAR2,
                          P_ERROR_MESSAGE  VARCHAR2 DEFAULT NULL) AS
 
  BEGIN
    INSERT INTO TABLE_COMPARISON_LOGS
      (JOB_NAME,
       SCHEMA_A_NAME,
       SCHEMA_B_NAME,
       SCHEMA_A_TABLE,
       SCHEMA_B_TABLE,
       STATUS,
       ERROR_MESSAGE,
       LOG_TIMESTAMP)
    VALUES
      (P_JOB_NAME,
       P_SCHEMA_A_NAME,
       P_SCHEMA_B_NAME,
       P_SCHEMA_A_TABLE,
       P_SCHEMA_B_TABLE,
       P_STATUS,
       P_ERROR_MESSAGE,
       SYSTIMESTAMP);
    COMMIT;
  END LOG_EXECUTION;
  -- Table Processing Procedure
  PROCEDURE PROCESS_TABLE(P_SCHEMA_A_NAME  VARCHAR2,
                        P_SCHEMA_B_NAME  VARCHAR2,
                        P_SCHEMA_A_TABLE VARCHAR2,
                        P_SCHEMA_B_TABLE VARCHAR2,
                        P_DB_LINK        VARCHAR2,
                        P_COMMON_COLUMNS VARCHAR2) AS
  V_JOB_NAME VARCHAR2(200);
  V_MSG      VARCHAR2(300);
  V_SQLERRM      VARCHAR2(300);
  V_T_START  TIMESTAMP := SYSTIMESTAMP;
  V_T_END    TIMESTAMP;
  V_T_ID     NUMBER;
BEGIN
  V_JOB_NAME := 'COMPARE_' || P_SCHEMA_A_TABLE || '_' || P_SCHEMA_B_TABLE;

  -- Mark table as Working
  UPDATE TABLE_COMPARISON_CONTROL
     SET RUN_STATUS = 'W'
   WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
     AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
     AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
     AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
  COMMIT;

  -- Log start
  LOG_EXECUTION(V_JOB_NAME,
                P_SCHEMA_A_NAME,
                P_SCHEMA_B_NAME,
                P_SCHEMA_A_TABLE,
                P_SCHEMA_B_TABLE,
                'STARTED');

  -- Timings: insert start row
  INSERT INTO TABLE_COMPARISON_TIMINGS
    (JOB_NAME,
     SCHEMA_A_NAME,
     SCHEMA_B_NAME,
     SCHEMA_A_TABLE,
     SCHEMA_B_TABLE,
     START_TIME)
  VALUES
    (V_JOB_NAME,
     P_SCHEMA_A_NAME,
     P_SCHEMA_B_NAME,
     P_SCHEMA_A_TABLE,
     P_SCHEMA_B_TABLE,
     V_T_START)
  RETURNING ID INTO V_T_ID;
  COMMIT;

  -- Execute the comparison (now returns COMPLETED:NOMISMATCH or COMPLETED:MISMATCH)
  PROC_COMPARE_SCHEMAS_TABLES2(P_SCHEMA_A_NAME,
                               P_SCHEMA_B_NAME,
                               P_COMMON_COLUMNS,
                               P_SCHEMA_A_TABLE,
                               P_SCHEMA_B_TABLE,
                               P_DB_LINK,
                               V_MSG);

  V_T_END := SYSTIMESTAMP;

  -- Decide run status & error_code
  IF V_MSG LIKE 'COMPLETED:NOMISMATCH' THEN
    UPDATE TABLE_COMPARISON_CONTROL
       SET RUN_STATUS = 'C', ERROR_CODE = 'completed, no mismatch found'
     WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
       AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
       AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
       AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
  
    LOG_EXECUTION(V_JOB_NAME,
                  P_SCHEMA_A_NAME,
                  P_SCHEMA_B_NAME,
                  P_SCHEMA_A_TABLE,
                  P_SCHEMA_B_TABLE,
                  'COMPLETED');
  
  ELSIF V_MSG LIKE 'COMPLETED:MISMATCH' THEN
    UPDATE TABLE_COMPARISON_CONTROL
       SET RUN_STATUS = 'M', ERROR_CODE = 'completed, mismatch found'
     WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
       AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
       AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
       AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
  
    LOG_EXECUTION(V_JOB_NAME,
                  P_SCHEMA_A_NAME,
                  P_SCHEMA_B_NAME,
                  P_SCHEMA_A_TABLE,
                  P_SCHEMA_B_TABLE,
                  'COMPLETED');
  
  ELSE
    -- Treat any other text as failure
    UPDATE TABLE_COMPARISON_CONTROL
       SET RUN_STATUS = 'A', ERROR_CODE = V_MSG
     WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
       AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
       AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
       AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
  
    LOG_EXECUTION(V_JOB_NAME,
                  P_SCHEMA_A_NAME,
                  P_SCHEMA_B_NAME,
                  P_SCHEMA_A_TABLE,
                  P_SCHEMA_B_TABLE,
                  'FAILED',
                  V_MSG);
  END IF;

  -- Update timings on success/fail
  UPDATE TABLE_COMPARISON_TIMINGS
     SET END_TIME         = V_T_END,
         DURATION_SECONDS = EXTRACT(SECOND FROM(V_T_END - V_T_START)) +
                            EXTRACT(MINUTE FROM(V_T_END - V_T_START)) * 60 +
                            EXTRACT(HOUR FROM(V_T_END - V_T_START)) * 3600
   WHERE ID = V_T_ID;
  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    -- Log failure
    LOG_EXECUTION(V_JOB_NAME,
                  P_SCHEMA_A_NAME,
                  P_SCHEMA_B_NAME,
                  P_SCHEMA_A_TABLE,
                  P_SCHEMA_B_TABLE,
                  'FAILED',
                  SQLERRM);
  
    -- Mark as Aborted
    V_SQLERRM:=SQLERRM;
    
    UPDATE TABLE_COMPARISON_CONTROL
       SET RUN_STATUS = 'A', ERROR_CODE = NVL(V_MSG, V_SQLERRM)
     WHERE SCHEMA_A_NAME = P_SCHEMA_A_NAME
       AND SCHEMA_B_NAME = P_SCHEMA_B_NAME
       AND SCHEMA_A_TABLE = P_SCHEMA_A_TABLE
       AND SCHEMA_B_TABLE = P_SCHEMA_B_TABLE;
  
    -- Close timing row even on error
    UPDATE TABLE_COMPARISON_TIMINGS
       SET END_TIME         = SYSTIMESTAMP,
           DURATION_SECONDS = EXTRACT(SECOND FROM(SYSTIMESTAMP - V_T_START)) +
                              EXTRACT(MINUTE FROM(SYSTIMESTAMP - V_T_START)) * 60 +
                              EXTRACT(HOUR FROM(SYSTIMESTAMP - V_T_START)) * 3600
     WHERE ID = V_T_ID;
  
    COMMIT;
END PROCESS_TABLE;

  -- Job Submission Procedure
 
  PROCEDURE busy_wait(seconds IN NUMBER) IS
    v_start_time TIMESTAMP := SYSTIMESTAMP;
  BEGIN
    WHILE SYSTIMESTAMP < v_start_time + NUMTODSINTERVAL(seconds, 'SECOND') LOOP
      NULL; -- do nothing
    END LOOP;
  END;
 
  PROCEDURE SUBMIT_COMPARISON_JOBS IS
    TYPE TABLE_RECORD IS RECORD(
      SCHEMA_A_NAME  VARCHAR2(100),
      SCHEMA_B_NAME  VARCHAR2(100),
      SCHEMA_A_TABLE VARCHAR2(100),
      SCHEMA_B_TABLE VARCHAR2(100),
      DB_LINK      VARCHAR2(100),-- db link changes
      COMMON_COLUMNS VARCHAR2(4000));
 
    TYPE TABLE_LIST IS TABLE OF TABLE_RECORD;
    V_TABLES       TABLE_LIST;
    V_JOB_COUNT    INTEGER;
    V_PENDING_JOBS INTEGER;
    V_LOOP_FLAG    VARCHAR2(10);
  BEGIN
    LOOP
      BEGIN
        SELECT RUN_STATUS
          INTO V_LOOP_FLAG
          FROM TABLE_COMPARISON_CONTROL_FLAG
         WHERE CONTROL_ID = 1;
        EXIT WHEN V_LOOP_FLAG = 'STOP';
      END;
 
      -- Check how many jobs are currently running
      SELECT COUNT(*)
        INTO V_JOB_COUNT
        FROM TABLE_COMPARISON_CONTROL
       WHERE RUN_STATUS = 'W';
      -- Check how many jobs are pending
      BEGIN
        update table_comparison_control
        set run_status='N',error_code=NULL
        where error_code like '%TNS%';
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line ( 'Error in reseting TNS error rows ' || SQLERRM);
          END;
      SELECT COUNT(*)
        INTO V_PENDING_JOBS
        FROM TABLE_COMPARISON_CONTROL
       WHERE RUN_STATUS IN ('N', NULL) 
         AND CHECK_FLAG = 'Y'
          AND  table_size_category='BB';
      EXIT WHEN V_PENDING_JOBS = 0;
      -- If fewer than 5 jobs are running, get more to submit
      IF V_JOB_COUNT < 5 THEN
        SELECT SCHEMA_A_NAME,
               SCHEMA_B_NAME,
               SCHEMA_A_TABLE,
               SCHEMA_B_TABLE,
               DB_LINK,--db link changes
               COMMON_COLUMNS
          BULK COLLECT
          INTO V_TABLES
          FROM TABLE_COMPARISON_CONTROL
         WHERE RUN_STATUS IN ('N', NULL) --tc
           AND CHECK_FLAG = 'Y'
AND  table_size_category='BB'
         ORDER BY TABLE_SIZE_CATEGORY
           FOR UPDATE SKIP LOCKED;
        EXIT WHEN V_TABLES.COUNT = 0;
        FOR I IN 1 .. LEAST(5 - V_JOB_COUNT, V_TABLES.COUNT) LOOP
          DBMS_SCHEDULER.CREATE_JOB(JOB_NAME   => 'JOB_' || V_TABLES(I).SCHEMA_A_TABLE || '_' || V_TABLES(I).SCHEMA_B_TABLE,
                                    JOB_TYPE   => 'PLSQL_BLOCK',
                                    JOB_ACTION => 'BEGIN PKG_TABLE_COMPARISON.PROCESS_TABLE(''' || V_TABLES(I).SCHEMA_A_NAME ||
                                                  ''', ''' || V_TABLES(I).SCHEMA_B_NAME ||
                                                  ''', ''' || V_TABLES(I).SCHEMA_A_TABLE ||
                                                  ''', ''' || V_TABLES(I).SCHEMA_B_TABLE ||
                          ''', ''' || V_TABLES(I).DB_LINK ||
                                                  ''', ''' || V_TABLES(I).COMMON_COLUMNS ||
                                                  '''); END;',
                                    ENABLED    => TRUE);
        END LOOP;
      END IF;
 
      -- Wait before next check
      --DBMS_LOCK.SLEEP(10);
      busy_wait(10);
    END LOOP;
  END SUBMIT_COMPARISON_JOBS;
 
  -- Monitoring Procedure
  PROCEDURE MONITOR_JOBS AS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('Running Comparisons:');
    FOR R IN (SELECT * FROM TABLE_COMPARISON_CONTROL WHERE RUN_STATUS = 'W') LOOP
      DBMS_OUTPUT.PUT_LINE(R.SCHEMA_A_TABLE || ' -> ' || R.SCHEMA_B_TABLE ||
                           ' [Working]');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Completed Comparisons:');
    FOR C IN (SELECT * FROM TABLE_COMPARISON_CONTROL WHERE RUN_STATUS = 'C') LOOP
      DBMS_OUTPUT.PUT_LINE(C.SCHEMA_A_TABLE || ' -> ' || C.SCHEMA_B_TABLE ||
                           ' [Completed]');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Failed Comparisons:');
    FOR F IN (SELECT * FROM TABLE_COMPARISON_CONTROL WHERE RUN_STATUS = 'A') LOOP
      DBMS_OUTPUT.PUT_LINE(F.SCHEMA_A_TABLE || ' -> ' || F.SCHEMA_B_TABLE ||
                           ' [Aborted: ' || F.ERROR_CODE || ']');
    END LOOP;
  END MONITOR_JOBS;
END PKG_TABLE_COMPARISON;
/
/
