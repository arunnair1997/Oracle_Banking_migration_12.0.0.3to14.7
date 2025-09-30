-- PROCEDURE PR_26_ISLAMIC_FINANCE_EMI_AMOUNT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_26_ISLAMIC_FINANCE_EMI_AMOUNT_V14" (p_branch_code IN VARCHAR2,
                                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '26_ISLAMIC_FINANCE_EMI_AMOUNT_' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACCOUNT_NUMBER, BRANCH_CODE, CUSTOMER_ID, SCHEDULE_DUE_DATE, EMI_AMOUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT ACCOUNT_NUMBER,
       BRANCH_CODE,
       CUSTOMER_ID,
       SCHEDULE_DUE_DATE,
       EMI_AMOUNT
  FROM (
     SELECT
          A.ACCOUNT_NUMBER,
          A.BRANCH_CODE,
          A.CUSTOMER_ID,
          S.SCHEDULE_DUE_DATE,
          MAX(S.EMI_AMOUNT) OVER(
          PARTITION BY A.ACCOUNT_NUMBER, A.BRANCH_CODE, S.SCHEDULE_DUE_DATE
          ) AS EMI_AMOUNT,
          ROW_NUMBER() OVER(
          PARTITION BY A.ACCOUNT_NUMBER, A.BRANCH_CODE
          ORDER BY S.SCHEDULE_DUE_DATE
          ) AS RN
          FROM INTEGRATEDPP.CLZB_ACCOUNT_APPS_MASTER A
        JOIN INTEGRATEDPP.CLZB_ACCOUNT_SCHEDULES S
            ON A.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER
            AND A.BRANCH_CODE = S.BRANCH_CODE
           JOIN INTEGRATEDPP.STZM_CUSTOMER B
        ON B.CUSTOMER_NO = A.CUSTOMER_ID
           WHERE S.SCHEDULE_DUE_DATE > TO_DATE('27-07-2025', 'DD-MM-YYYY') --NEXT SCHEDULE_DUE_DATE
           AND S.EMI_AMOUNT IS NOT NULL
           AND B.RECORD_STAT = 'O'
           AND B.AUTH_STAT = 'A'
           AND A.ACCOUNT_STATUS = 'A'
           AND A.MODULE_CODE = 'CI'
           AND A.BRANCH_CODE = p_branch_code
           ) T
 WHERE RN = 1
 ORDER BY CUSTOMER_ID) LOOP
    l_line := rec.ACCOUNT_NUMBER || ',' || rec.BRANCH_CODE || ',' ||
              rec.CUSTOMER_ID || ',' || rec.SCHEDULE_DUE_DATE || ',' ||
              rec.EMI_AMOUNT;
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
END PR_26_ISLAMIC_FINANCE_EMI_AMOUNT_V14;
/
/
