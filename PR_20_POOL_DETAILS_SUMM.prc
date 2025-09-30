-- PROCEDURE PR_20_POOL_DETAILS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_20_POOL_DETAILS_SUMM" (p_branch_code IN VARCHAR2,
                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '20_Pool_Details_SUMM' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'LIAB NO, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT COALESCE(v12.liab_no, v14.liab_no) AS liab_no,
       NVL(v12.v12cnt, 0) AS v12cnt,
       NVL(v14.v14cnt, 0) AS v14cnt
  from (select liab.liab_no, count(gp.pool_code) v12cnt
          from ubsprod.getm_pool@fcubsv12                gp,
                     ubsprod.getm_liab@fcubsv12                liab
                 --    ubsprod.GETM_OD_POOL_COLL_CUSTOM@fcubsv12 odp
               where gp.liab_id = liab.id
                 --and odp.pool_id = gp.id
                 --and gp.liab_id = odp.liab_id
                 and liab.liab_branch = p_branch_code
                 and gp.record_stat = 'O'
                 and gp.auth_stat = 'A'
         group by liab.liab_no) V12

  FULL OUTER JOIN (select liab.liab_no, count(gp.pool_code) v14cnt
                      from integratedpp.gczm_pool                gp,
                     integratedpp.gezm_liab                liab
              --       integratedpp.GEZM_OD_POOL_COLL_CUSTOM odp
               where gp.liab_id = liab.id
             --    and odp.pool_id = gp.id
            --     and gp.liab_id = odp.liab_id
                 and liab.liab_branch = p_branch_code
                 and gp.record_stat = 'O'
                 and gp.auth_stat = 'A'
                    group by liab.liab_no) V14

    ON v12.liab_no = v14.liab_no) LOOP
    l_line := rec.LIAB_NO || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END pr_20_Pool_Details_SUMM;
/
/
