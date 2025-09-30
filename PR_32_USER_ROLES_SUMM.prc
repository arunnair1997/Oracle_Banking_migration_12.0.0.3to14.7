-- PROCEDURE PR_32_USER_ROLES_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_32_USER_ROLES_SUMM" (p_branch_code IN VARCHAR2,
                                                  p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '32_User_roles_SUMM' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ROLE ID, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT COALESCE(v12.role_id, v14.role_id) AS role_id,
                     NVL(v12.v12cnt, 0) AS v12cnt,
                     NVL(v14.v14cnt, 0) AS v14cnt
                from (select ur.role_id, count(ur.user_id) v12cnt
                        from ubsprod.smtb_user_role@fcubsv12   ur,
                             ubsprod.smtb_role_master@fcubsv12 rm
                       where rm.role_id(+) = ur.role_id
                         and ur.branch_code = p_branch_code
                         and rm.record_stat = 'O'
                         and rm.auth_stat = 'A'
                       group by ur.role_id) V12
                FULL OUTER JOIN (select ur.role_id, count(ur.user_id) v14cnt
                                  from integratedpp.smzb_user_role   ur,
                                       integratedpp.smzb_role_master rm
                                 where rm.role_id(+) = ur.role_id
                                   and ur.branch_code = p_branch_code
                                   and rm.record_stat = 'O'
                                   and rm.auth_stat = 'A'
                                 group by ur.role_id) V14
                  ON v12.role_id = v14.role_id) LOOP
    l_line := rec.role_id || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END pr_32_User_roles_SUMM;
/
/
