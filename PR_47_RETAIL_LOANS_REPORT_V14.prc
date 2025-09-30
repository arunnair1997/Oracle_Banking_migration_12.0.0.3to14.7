-- PROCEDURE PR_47_RETAIL_LOANS_REPORT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_47_RETAIL_LOANS_REPORT_V14" (p_branch_code IN VARCHAR2,
                                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '47_RETAIL_LOANS_REPORT_V14' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACCOUNT_NUMBER,BRANCH_CODE,CUSTOMER_ID,PRIMARY_APPLICANT_NAME,APPLICATION_NUM,MODULE_CODE,PRODUCT_CODE,PRODUCT_CATEGORY,CURRENCY,BOOK_DATE,VALUE_DATE,MATURITY_DATE,AMOUNT_FINANCED,AMOUNT_DISBURSED,USER_DEFINED_STATUS';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT A.ACCOUNT_NUMBER,
                     A.BRANCH_CODE,
                     A.CUSTOMER_ID,
                     A.PRIMARY_APPLICANT_NAME,
                     A.APPLICATION_NUM,
                     A.MODULE_CODE,
                     A.PRODUCT_CODE,
                     A.PRODUCT_CATEGORY,
                     A.CURRENCY,
                     A.BOOK_DATE,
                     A.VALUE_DATE,
                     A.MATURITY_DATE,
                     A.AMOUNT_FINANCED,
                     A.AMOUNT_DISBURSED,
                     A.USER_DEFINED_STATUS
                FROM INTEGRATEDPP.CLZB_ACCOUNT_APPS_MASTER A,
                     INTEGRATEDPP.STZM_CUSTOMER            B
               WHERE B.CUSTOMER_NO = A.CUSTOMER_ID
                 AND A.ACCOUNT_STATUS='A'
                 AND B.RECORD_STAT = 'O'
                 AND B.AUTH_STAT = 'A'
                 and a.module_code = 'CL'
                 AND A.BRANCH_CODE = p_branch_code) LOOP
    l_line := rec.ACCOUNT_NUMBER || ',' || rec.BRANCH_CODE || ',' ||
              rec.CUSTOMER_ID || ',' || rec.PRIMARY_APPLICANT_NAME || ',' ||
              rec.APPLICATION_NUM || ',' || rec.MODULE_CODE || ',' ||
              rec.PRODUCT_CODE || ',' || rec.PRODUCT_CATEGORY || ',' ||
              rec.CURRENCY || ',' || rec.BOOK_DATE || ',' || rec.VALUE_DATE || ',' ||
              rec.MATURITY_DATE || ',' || rec.AMOUNT_FINANCED ||
              rec.AMOUNT_DISBURSED || ',' || rec.USER_DEFINED_STATUS;
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
END PR_47_RETAIL_LOANS_REPORT_V14;
/
/
