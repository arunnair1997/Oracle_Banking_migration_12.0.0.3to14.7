-- PROCEDURE PR_10_CHEQUES_COUNT_V14_RIZ (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_10_CHEQUES_COUNT_V14_RIZ" (p_branch_code IN VARCHAR2,
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
  FOR rec IN (WITH latest_check_details AS (
    SELECT 
        ccd.*,
        ROW_NUMBER() OVER (PARTITION BY check_no, account, check_book_no ORDER BY mod_no DESC) rn
    FROM integratedpp.cazm_check_details ccd
    WHERE ccd.record_stat = 'O'
      AND ccd.auth_stat = 'A'
      AND ccd.status <> 'U'
      AND ccd.branch = p_branch_code
),
valid_accounts AS (
    SELECT cust_ac_no
    FROM integratedpp.stzm_cust_account
    WHERE record_stat = 'O'
     AND auth_stat = 'A'
),
valid_check_books AS (
    SELECT DISTINCT account, check_book_no
    FROM latest_check_details
    WHERE account IN (SELECT cust_ac_no FROM valid_accounts)
)
SELECT 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    lcd.status,
    COUNT(lcd.check_no) No_of_Checks
FROM 
    integratedpp.CAZM_CHECK_BOOK ccb
JOIN 
    latest_check_details lcd 
    ON ccb.account = lcd.account
    AND ccb.first_check_no = lcd.check_book_no
    AND lcd.rn = 1
JOIN 
    valid_check_books vcb 
    ON ccb.account = vcb.account AND ccb.first_check_no = vcb.check_book_no
WHERE 
   ccb.record_stat = 'O'
    AND ccb.auth_stat = 'A'
   AND lcd.branch = p_branch_code
GROUP BY 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    lcd.status
ORDER BY 
    ccb.account,
    ccb.first_check_no,
    ccb.check_leaves,
    lcd.status) LOOP
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
END pr_10_cheques_count_v14_RIZ;
/
/
