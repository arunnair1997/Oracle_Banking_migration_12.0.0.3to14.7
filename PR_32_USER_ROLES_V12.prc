-- PROCEDURE PR_32_USER_ROLES_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_32_USER_ROLES_V12" (p_branch_code IN VARCHAR2,
                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '32_User_roles_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'USER_ID, BRANCH_CODE, ROLE_ID, Central Role';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select ur.user_id,
                     ur.branch_code,
                     ur.role_id,
                     decode(rm.CENTRALISATION_ROLE, 'Y', 'Y', 'N', 'N', 'N') centrol
                from ubsprod.smtb_user_role@fcubsv12   ur,
                     ubsprod.smtb_role_master@fcubsv12 rm
               where rm.role_id(+) = ur.role_id
                 and ur.branch_code = p_branch_code and rm.record_stat='O' and rm.auth_stat='A'
               order by ur.user_id, ur.branch_code, ur.ROLE_ID) LOOP
    l_line := rec.user_id || ',' || rec.branch_code || ',' || rec.role_id || ',' ||
              rec.centrol;
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
END pr_32_User_roles_v12;
/
/
