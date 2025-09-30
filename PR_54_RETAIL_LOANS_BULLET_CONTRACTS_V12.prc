-- PROCEDURE PR_54_RETAIL_LOANS_BULLET_CONTRACTS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_54_RETAIL_LOANS_BULLET_CONTRACTS_V12" (p_branch_code IN VARCHAR2,
                                                                    p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '54_RETAIL_LOANS_BULLET_CONTRACTS_' || p_branch_code ||
                '_V12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACCOUNT_NUMBER, BRANCH_CODE, CUSTOMER_ID, FREQUENCY_UNIT, NO_OF_INSTALLMENTS, REMAINING_NO_OF_INSTALLMENTS';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT A.ACCOUNT_NUMBER,
                     A.BRANCH_CODE,
                     A.CUSTOMER_ID,
                     A.FREQUENCY_UNIT,
                     A.NO_OF_INSTALLMENTS,
                     (SELECT COUNT(*)
                        FROM UBSPROD.CLTB_ACCOUNT_SCHEDULES@FCUBSV12 S1
                       WHERE S1.ACCOUNT_NUMBER = A.ACCOUNT_NUMBER
                         AND S1.BRANCH_CODE = p_branch_code
                         AND S1.BRANCH_CODE = A.BRANCH_CODE
                         AND S1.COMPONENT_NAME = 'PROFIT'
                         AND NVL(S1.AMOUNT_DUE, 0) >
                             NVL(S1.AMOUNT_SETTLED, 0)) AS REMAINING_NO_OF_INSTALLMENTS
                FROM UBSPROD.CLTB_ACCOUNT_APPS_MASTER@FCUBSV12 A
                JOIN UBSPROD.STTM_CUSTOMER@FCUBSV12 B
                  ON A.CUSTOMER_ID = B.CUSTOMER_NO
               WHERE A.BRANCH_CODE = p_branch_code
                 and A.account_status = 'A'
                 and A.frequency_unit = 'B'
                 and (A.no_of_installments = '1' OR
                     A.no_of_installments IS NULL)
                 AND B.RECORD_STAT = 'O'
                 AND B.AUTH_STAT = 'A'
                 AND A.MODULE_CODE = 'CL'
               ORDER BY A.CUSTOMER_ID) LOOP
    l_line := rec.ACCOUNT_NUMBER || ',' || rec.BRANCH_CODE || ',' ||
              rec.CUSTOMER_ID || ',' || rec.FREQUENCY_UNIT || ',' ||
              rec.NO_OF_INSTALLMENTS || ',' ||
              rec.REMAINING_NO_OF_INSTALLMENTS;
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
END PR_54_RETAIL_LOANS_BULLET_CONTRACTS_V12;
/
/
