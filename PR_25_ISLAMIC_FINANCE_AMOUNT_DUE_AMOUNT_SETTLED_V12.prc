-- PROCEDURE PR_25_ISLAMIC_FINANCE_AMOUNT_DUE_AMOUNT_SETTLED_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_25_ISLAMIC_FINANCE_AMOUNT_DUE_AMOUNT_SETTLED_V12" (p_branch_code IN VARCHAR2,
                                                                                p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '25_ISLAMIC_FINANCE_AMOUNT_DUE_AMOUNT_SETTLED_' ||
                p_branch_code || '_V12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACCOUNT_NUMBER,BRANCH_CODE,CUSTOMER_ID,SCHEDULE_DUE_DATE,TOTAL AMOUNT DUE,TOTAL AMOUNT SETTLED,TOTAL OUTSTANDING';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT A.ACCOUNT_NUMBER,
                     A.BRANCH_CODE,
                     A.CUSTOMER_ID,
                     S.SCHEDULE_DUE_DATE,
                     SUM(S.AMOUNT_DUE) TAD,
                     SUM(S.AMOUNT_SETTLED) TAS,
                     (SUM(S.AMOUNT_DUE) - SUM(S.AMOUNT_SETTLED)) TOS
                FROM UBSPROD.CLTB_ACCOUNT_APPS_MASTER@FCUBSV12 A,
                     UBSPROD.CLTB_ACCOUNT_SCHEDULES@FCUBSV12   S
               WHERE A.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER
                 AND A.BRANCH_CODE = p_branch_code
                 and a.module_code = 'CI'
                 and a.account_status = 'A'
               GROUP BY A.ACCOUNT_NUMBER,
                        A.BRANCH_CODE,
                        A.CUSTOMER_ID,
                        S.SCHEDULE_DUE_DATE
              HAVING(SUM(S.AMOUNT_DUE) - SUM(S.AMOUNT_SETTLED)) <> 0
               ORDER BY CUSTOMER_ID) LOOP
    l_line := rec.ACCOUNT_NUMBER || ',' || rec.BRANCH_CODE || ',' ||
              rec.CUSTOMER_ID || ',' || rec.SCHEDULE_DUE_DATE || ',' ||
              rec.TAD || ',' || rec.TAS || ',' || rec.TOS;
    UTL_FILE.PUT_LINE(l_file, l_line);
  END LOOP;
  dbms_output.put_line('CHECK3');
  UTL_FILE.FCLOSE(l_file);
EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('BOMBED' || SQLERRM);
    IF UTL_FILE.IS_OPEN(l_file) THEN
      UTL_FILE.FCLOSE(l_file);
    END IF;
    RAISE;
END PR_25_ISLAMIC_FINANCE_AMOUNT_DUE_AMOUNT_SETTLED_V12;
/
/
