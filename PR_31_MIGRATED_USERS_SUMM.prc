-- PROCEDURE PR_31_MIGRATED_USERS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_31_MIGRATED_USERS_SUMM" (p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '31_Migrated_users_SUMM' || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'BRANCH,V12_ACTIVE_USER_COUNT, V14_ACTIVE_USER_COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT 
    COALESCE(v12.HOME_BRANCH, v14.HOME_BRANCH) AS BRANCH,
    NVL(v12.ACTIVE_USER_COUNT, 0) AS V12_ACTIVE_USER_COUNT,
    NVL(v14.ACTIVE_USER_COUNT, 0) AS V14_ACTIVE_USER_COUNT
FROM
    (
        SELECT 
            HOME_BRANCH,
            COUNT(*) AS ACTIVE_USER_COUNT
        FROM 
            ubsprod.smtb_user@fcubsv12
        WHERE 
            RECORD_STAT = 'O'
            AND AUTH_STAT = 'A'
        GROUP BY 
            HOME_BRANCH
    ) v12
FULL OUTER JOIN
    (
        SELECT 
            HOME_BRANCH,
            COUNT(*) AS ACTIVE_USER_COUNT
        FROM 
            integratedpp.smzb_user
        WHERE 
            RECORD_STAT = 'O'
            AND AUTH_STAT = 'A'
        GROUP BY 
            HOME_BRANCH
    ) v14
ON v12.HOME_BRANCH = v14.HOME_BRANCH
ORDER BY BRANCH) LOOP
    l_line := rec.BRANCH || ',' || rec.V12_ACTIVE_USER_COUNT || ',' ||
              rec.V14_ACTIVE_USER_COUNT;
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
END pr_31_Migrated_users_SUMM;
/
/
