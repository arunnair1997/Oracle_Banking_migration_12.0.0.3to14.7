-- PROCEDURE PR_10_CHEQUES_COUNT_V12_RIZ (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_10_CHEQUES_COUNT_V12_RIZ" (p_branch_code IN VARCHAR2,
                                                    p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '10_cheques_count' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := '   , ACCOUNT, FIRST_CHECK_NO, CHECK_LEAVES, STATUS, No_of_Checks';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    ccd.status,
    COUNT(ccd.check_no) No_of_Checks
FROM 
    uBSPROD.catm_check_book@Fcubsv12 ccb
JOIN 
    uBSPROD.catm_check_details@Fcubsv12 ccd 
    ON ccb.account = ccd.account 
   AND ccb.first_check_no = ccd.check_book_no
WHERE 
    ccb.record_stat = 'O'
    AND ccb.auth_stat = 'A'
    AND ccd.record_stat = 'O'
    AND ccd.auth_stat = 'A'
    AND ccd.status <> 'U'
    AND ccd.branch = p_branch_code
    AND EXISTS (
        SELECT 1 
        FROM uBSPROD.sttm_cust_account@Fcubsv12 sca
        WHERE sca.cust_ac_no = ccb.account
          AND sca.record_stat = 'O'
          AND sca.auth_stat = 'A'
    )
    AND ccd.mod_no = (
        SELECT MAX(cd.mod_no)
        FROM uBSPROD.catm_check_details@Fcubsv12 cd
        WHERE cd.account = ccd.account
          AND cd.check_no = ccd.check_no
          AND cd.check_book_no = ccd.check_book_no
    )
GROUP BY 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    ccd.status
ORDER BY 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    ccd.status) LOOP
    l_line := rec.account || ',' || rec.first_check_no || ',' ||
              rec.check_leaves || ',' || rec.status || ',' || rec.No_of_Checks;
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
END pr_10_cheques_count_v12_RIZ;
/
/
