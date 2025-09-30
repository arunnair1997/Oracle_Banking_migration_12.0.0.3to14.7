-- PROCEDURE PR_23_COLLATERAL_DETAILS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_23_COLLATERAL_DETAILS_SUMM" (p_branch_code IN VARCHAR2,
                                                          p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '23_Collateral_Details_SUMM' || p_branch_code || '.csv';
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
                from (select liab.liab_no, count(collateral_code) v12cnt
                        from ubsprod.getm_collat@fcubsv12              col,
                             ubsprod.getm_liab@fcubsv12                liab,
                             ubsprod.GeTM_COLLAT_CONT_CONTRIB@fcubsv12 contt
                       where col.liab_id = liab.id
                         and contt.coll_id = col.id
                         and col.record_stat = 'O'
                         and liab.liab_branch = p_branch_code
                         and col.auth_stat = 'A'
                       group by liab.liab_no) V12
                FULL OUTER JOIN (select liab.liab_no,
                                       count(collateral_code) v14cnt
                                  from integratedpp.gczm_collat col
                join integratedpp.gezm_liab liab
                  ON col.liab_id = liab.id
                join integratedpp.GCZM_COLLAT_CONT_CONTRIB contt
                  ON contt.coll_id = col.id
                left join integratedpp.gczm_collat_custom intt
                  on intt.coll_id = col.id
               where col.record_stat = 'O'
                 and liab.liab_branch = p_branch_code
                 and col.auth_stat = 'A'
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
END pr_23_Collateral_Details_SUMM;
/
/
