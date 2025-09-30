-- PROCEDURE SUBMIT_ONE_TABLE (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."SUBMIT_ONE_TABLE" (V_WHICH_TABLE IN VARCHAR2) IS
    TYPE TABLE_RECORD IS RECORD(
      SCHEMA_A_NAME  VARCHAR2(100),
      SCHEMA_B_NAME  VARCHAR2(100),
      SCHEMA_A_TABLE VARCHAR2(100),
      SCHEMA_B_TABLE VARCHAR2(100),
      DB_LINK      VARCHAR2(100),-- db link changes
      COMMON_COLUMNS VARCHAR2(4000));
 
    TYPE TABLE_LIST IS TABLE OF TABLE_RECORD;
    V_TABLES       TABLE_LIST;
    V_LOOP_FLAG    VARCHAR2(10);
  BEGIN
      BEGIN
        SELECT RUN_STATUS
          INTO V_LOOP_FLAG
          FROM TABLE_COMPARISON_CONTROL_FLAG
         WHERE CONTROL_ID = 1;
      END;
 

   --   IF V_JOB_COUNT < 4 THEN
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
AND  SCHEMA_A_TABLE=V_WHICH_TABLE;

   BEGIN
          DBMS_SCHEDULER.CREATE_JOB(JOB_NAME   => 'JOB_' || V_TABLES(1).SCHEMA_A_TABLE || '_' || V_TABLES(1).SCHEMA_B_TABLE,
                                    JOB_TYPE   => 'PLSQL_BLOCK',
                                    JOB_ACTION => 'BEGIN PKG_TABLE_COMPARISON.PROCESS_TABLE(''' || V_TABLES(1).SCHEMA_A_NAME ||
                                                  ''', ''' || V_TABLES(1).SCHEMA_B_NAME ||
                                                  ''', ''' || V_TABLES(1).SCHEMA_A_TABLE ||
                                                  ''', ''' || V_TABLES(1).SCHEMA_B_TABLE ||
                          ''', ''' || V_TABLES(1).DB_LINK ||
                                                  ''', ''' || V_TABLES(1).COMMON_COLUMNS ||
                                                  '''); END;',
                                    ENABLED    => TRUE);
        EXCEPTION
          WHEN OTHERS THEN 
            DBMS_OUTPUT.put_line('BOMBED DUE TO ' || SQLERRM);
        END ;
  END ;
/
/
