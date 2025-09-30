-- PROCEDURE PR_30_MIGRATED_ROLES_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_30_MIGRATED_ROLES_V12" (p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '30_Migrated_roles_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ROLE_ID, ROLE_DESCRIPTION, BRANCHES_ALLOWED, ACCCLASS_ALLOWED, BRANCH_VLT_ROLE, BRANCH_ROLE_CAT, BRANCH_ROLE_LEVEL, BRANCH_PWD_RESET_FREQ, BRANCH_ROLE, BRANCH_AUTH_ROLE, CENTRALISATION_ROLE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select ROLE_ID,
                     ROLE_DESCRIPTION,
                     BRANCHES_ALLOWED,
                     ACCCLASS_ALLOWED,
                     BRANCH_VLT_ROLE,
                     BRANCH_ROLE_CAT,
                     BRANCH_ROLE_LEVEL,
                     BRANCH_PWD_RESET_FREQ,
                     BRANCH_ROLE,
                     BRANCH_AUTH_ROLE,
                     CENTRALISATION_ROLE
                from ubsprod.SMTB_ROLE_MASTER@fcubsv12
               where record_stat = 'O'
                 and auth_stat = 'A') LOOP
    l_line := rec.ROLE_ID || ',' || rec.ROLE_DESCRIPTION || ',' ||
              rec.BRANCHES_ALLOWED || ',' || rec.ACCCLASS_ALLOWED || ',' ||
              rec.BRANCH_VLT_ROLE || ',' || rec.BRANCH_ROLE_CAT || ',' ||
              rec.BRANCH_ROLE_LEVEL || ',' || rec.BRANCH_PWD_RESET_FREQ || ',' ||
              rec.BRANCH_ROLE || ',' || rec.BRANCH_AUTH_ROLE || ',' ||
              rec.CENTRALISATION_ROLE;
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
END pr_30_Migrated_roles_v12;
/
/
