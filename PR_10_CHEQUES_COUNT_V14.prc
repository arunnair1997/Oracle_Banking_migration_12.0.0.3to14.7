-- PROCEDURE PR_10_CHEQUES_COUNT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_10_CHEQUES_COUNT_V14" (p_branch_code IN VARCHAR2,
                                                    p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '10_cheques_count' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := '   , ACCOUNT, FIRST_CHECK_NO, CHECK_LEAVES, STATUS, No of Checks';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select ccb.account,
                     ccb.first_check_no,
                     ccb.check_leaves,
                     ccd.status,
                     count(ccd.check_no) cnt
                from integratedpp.cazm_check_details ccd,
                     integratedpp.CAZM_CHECK_BOOK    ccb
               where ccb.record_stat = 'O'
                 and ccb.auth_stat = 'A'
                 and ccb.account = ccd.account
                 and ccb.first_check_no = ccd.check_book_no
                 and ccd.mod_no in
                     (select max(cd.mod_no)
                        from integratedpp.cazm_check_details cd
                       where cd.CHECK_NO = ccd.check_no
                         and ccd.account = cd.account
                         and cd.check_book_no = ccd.check_book_no)
                 and ccd.BRANCH = p_branch_code
               group by ccb.account,
                        ccb.first_check_no,
                        ccb.check_leaves,
                        ccd.status
               order by ccb.account,
                        ccb.first_check_no,
                        ccb.check_leaves,
                        ccd.status) LOOP
    l_line := rec.account || ',' || rec.first_check_no || ',' ||
              rec.check_leaves || ',' || rec.status || ',' || rec.cnt;
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
END pr_10_cheques_count_v14;
/
/
