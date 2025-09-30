-- PROCEDURE PR_19_FACILITY_DETAILS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_19_FACILITY_DETAILS_SUMM" (p_branch_code IN VARCHAR2,
                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '19_Facility_details_SUMM_' || p_branch_code || '.csv';
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
                FROM (SELECT liab.liab_no, COUNT(fac.id) AS v12cnt
                        FROM ubsprod.getm_facility@fcubsv12 fac,
                             ubsprod.getm_liab@fcubsv12     liab
                       WHERE fac.auth_stat = 'A'
                         AND fac.record_stat = 'O'
                         AND fac.liab_id = liab.id
                         AND liab.liab_branch = p_branch_code
                       GROUP BY liab.liab_no) v12
                FULL OUTER JOIN (SELECT liab.liab_no, COUNT(fac.id) AS v14cnt
                                  FROM integratedpp.gezm_facility fac,
                                       integratedpp.gezm_liab     liab
                                 WHERE fac.auth_stat = 'A'
                                   AND fac.record_stat = 'O'
                                   AND fac.liab_id = liab.id
                                   AND liab.liab_branch = p_branch_code
                                 GROUP BY liab.liab_no) v14
                  ON v12.liab_no = v14.liab_no
               ORDER BY liab_no) LOOP
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
END pr_19_Facility_details_SUMM;
/
/
