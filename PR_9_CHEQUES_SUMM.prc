-- PROCEDURE PR_9_CHEQUES_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_9_CHEQUES_SUMM" (p_branch_code IN VARCHAR2,
                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '9_cheques_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER,ACCOUNT, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select V12.CUST_NO, V12.ACCOUNT, v12.v12cnt, v14.v14cnt
  from (select cac.cust_no, cab.ACCOUNT, count(*) v12cnt
          from ubsprod.CAtM_CHECK_BOOK@fcubsv12   cab,
               ubsprod.sttm_cust_account@fcubsv12 cac
         where cab.account = cac.cust_ac_no
           and cab.record_stat = 'O'
           and cab.auth_stat = 'A'
           and cab.BRANCH = p_branch_code
           and (ACCOUNT, FIRST_CHECK_NO) IN
               (SELECT ACCOUNT, CHECK_BOOK_NO
                  FROM ubsprod.CAtM_CHECK_DETAILS@fcubsv12
                 WHERE RECORD_STAT = 'O'
                   AND AUTH_STAT = 'A'
                   AND STATUS <> 'U'
                   AND ACCOUNT IN (SELECT CUST_AC_NO
                                     FROM ubsprod.STtM_CUST_ACCOUNT@fcubsv12
                                    WHERE RECORD_STAT = 'O'
                                      AND AUTH_STAT = 'A'))
         group by cac.cust_no, cab.account) V12
  LEFT JOIN (select cac.cust_no, cab.ACCOUNT, count(*) v14cnt
               from integratedpp.CAzM_CHECK_BOOK   cab,
                    integratedpp.stzm_cust_account cac
              where cab.account = cac.cust_ac_no
                and cab.record_stat = 'O'
                and cab.auth_stat = 'A'
                and cab.BRANCH = p_branch_code
                and (ACCOUNT, FIRST_CHECK_NO) IN
                    (SELECT ACCOUNT, CHECK_BOOK_NO
                       FROM integratedpp.CAzM_CHECK_DETAILS
                      WHERE RECORD_STAT = 'O'
                        AND AUTH_STAT = 'A'
                        AND STATUS <> 'U'
                        AND ACCOUNT IN (SELECT CUST_AC_NO
                                          FROM integratedpp.STzM_CUST_ACCOUNT
                                         WHERE RECORD_STAT = 'O'
                                           AND AUTH_STAT = 'A'))
              group by cac.cust_no, cab.account) V14
 ON V12.CUST_NO = V14.CUST_NO
   and V12.ACCOUNT = V14.ACCOUNT
) LOOP
    l_line := rec.CUST_NO || ',' || rec.CUST_NO  || ',' || rec.v12cnt || ',' ||
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
END pr_9_cheques_SUMM;
/
/
