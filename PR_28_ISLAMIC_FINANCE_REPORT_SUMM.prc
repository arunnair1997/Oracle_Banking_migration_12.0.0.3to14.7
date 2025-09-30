-- PROCEDURE PR_28_ISLAMIC_FINANCE_REPORT_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_28_ISLAMIC_FINANCE_REPORT_SUMM" (p_branch_code IN VARCHAR2,
                                                                  p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '28_ISLAMIC_FINANCE_PER_PRODUCT_SUMM' || p_branch_code ||
                '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT V12.PRODUCT_CODE, V12CNT, V14CNT
  from (SELECT A.PRODUCT_CODE, count(A.ACCOUNT_NUMBER) V12CNT
          FROM UBSPROD.CLTB_ACCOUNT_APPS_MASTER@FCUBSV12 A,
               UBSPROD.STTM_CUSTOMER@FCUBSV12            B
         WHERE B.CUSTOMER_NO = A.CUSTOMER_ID
           AND B.RECORD_STAT = 'O'
           AND B.AUTH_STAT = 'A'
           and a.module_code = 'CI'
           and a.account_status = 'A'
           AND A.BRANCH_CODE = p_branch_code
         group by A.PRODUCT_CODE) V12
  LEFT JOIN (SELECT A.PRODUCT_CODE, count(A.ACCOUNT_NUMBER) V14CNT
               FROM INTEGRATEDPP.CLZB_ACCOUNT_APPS_MASTER A,
                    INTEGRATEDPP.STZM_CUSTOMER            B
              WHERE B.CUSTOMER_NO = A.CUSTOMER_ID
                AND B.RECORD_STAT = 'O'
                AND B.AUTH_STAT = 'A'
                and a.module_code = 'CI'
                and a.account_status = 'A'
                AND A.BRANCH_CODE = p_branch_code
              group by A.PRODUCT_CODE) V14
    ON V14.PRODUCT_CODE = v12.PRODUCT_CODE
) LOOP
    l_line := rec.product_code || ',' || 
              rec.v12cnt || ',' || rec.v14cnt;
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
END pr_28_islamic_finance_report_SUMM;
/
/
