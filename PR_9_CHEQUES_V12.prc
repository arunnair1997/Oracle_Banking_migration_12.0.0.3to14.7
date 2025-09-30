-- PROCEDURE PR_9_CHEQUES_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_9_CHEQUES_V12" (p_branch_code IN VARCHAR2,
                                             p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '9_cheques_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUST_NO, BRANCH, ACCOUNT, FIRST_CHECK_NO, CHECK_LEAVES, ORDER_DATE, ISSUE_DATE, ORDER_DETAILS, MAKER_ID, CHQ_TYPE, PRINT_STATUS, REQUEST_STATUS, TRN_REF_NO, SEQ_NO, MAKER_DT_STAMP, CHECKER_ID, CHECKER_DT_STAMP, MOD_NO, ONCE_AUTH';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select cac.cust_no,
                     cab.BRANCH,
                     cab.ACCOUNT,
                     cab.FIRST_CHECK_NO,
                     cab.CHECK_LEAVES,
                     cab.ORDER_DATE,
                     cab.ISSUE_DATE,
                     cab.ORDER_DETAILS,
                     cab.MAKER_ID,
                     cab.CHQ_TYPE,
                     cab.PRINT_STATUS,
                     cab.REQUEST_STATUS,
                     cab.TRN_REF_NO,
                     cab.SEQ_NO,
                     cab.MAKER_DT_STAMP,
                     cab.CHECKER_ID,
                     cab.CHECKER_DT_STAMP,
                     cab.MOD_NO,
                     cab.ONCE_AUTH
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
                                            AND AUTH_STAT = 'A'))) LOOP
    l_line := rec.CUST_NO || ',' || rec.BRANCH || ',' || rec.ACCOUNT || ',' ||
              rec.FIRST_CHECK_NO || ',' || rec.CHECK_LEAVES || ',' ||
              rec.ORDER_DATE || ',' || rec.ISSUE_DATE || ',' ||
              rec.ORDER_DETAILS || ',' || rec.MAKER_ID || ',' ||
              rec.CHQ_TYPE || ',' || rec.PRINT_STATUS || ',' ||
              rec.REQUEST_STATUS || ',' || rec.TRN_REF_NO || ',' ||
              rec.SEQ_NO || ',' || rec.MAKER_DT_STAMP || ',' ||
              rec.CHECKER_ID || ',' || rec.CHECKER_DT_STAMP || ',' ||
              rec.MOD_NO || ',' || rec.ONCE_AUTH;
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
END pr_9_cheques_v12;
/
/
