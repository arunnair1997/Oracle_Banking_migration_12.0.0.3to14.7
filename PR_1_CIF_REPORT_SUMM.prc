-- PROCEDURE PR_1_CIF_REPORT_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_1_CIF_REPORT_SUMM" (p_branch_code IN VARCHAR2,
                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '1_CIF_REPORT_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_TYPE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select v12.CUSTOMER_TYPE, v12.cnt v12cnt, v14.cnt v14cnt
                from (select LOCAL_BRANCH,
                             CUSTOMER_TYPE,
                             count(customer_no) cnt
                        from ubsprod.sttm_customer@fcubsv12
                       where record_stat = 'O'
                         and auth_stat = 'A'
                       group by CUSTOMER_TYPE, LOCAL_BRANCH) v12,
                     (select LOCAL_BRANCH,
                             CUSTOMER_TYPE,
                             count(customer_no) cnt
                        from integratedpp.stzm_customer
                       where record_stat = 'O'
                         and auth_stat = 'A'
                       group by CUSTOMER_TYPE, LOCAL_BRANCH) v14
               where v12.customer_type = v14.customer_type
                 and v12.LOCAL_BRANCH = v14.LOCAL_BRANCH
                 and v12.LOCAL_BRANCH = p_branch_code) LOOP
    l_line := rec.CUSTOMER_TYPE || ',' || rec.v12cnt || ',' ||
              rec.v14cnt;
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
END pr_1_CIF_REPORT_SUMM;
/
/
