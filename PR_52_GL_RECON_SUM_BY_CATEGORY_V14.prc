-- PROCEDURE PR_52_GL_RECON_SUM_BY_CATEGORY_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_52_GL_RECON_SUM_BY_CATEGORY_V14" (p_branch_code IN VARCHAR2,
                                                               p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '52_GL_RECON_SUM_BY_CATEGORY' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CATEGORY_NAME, PARENT_GL, TOTAL DR_BAL, TOTAL CR_BAL, TOTAL DR_BAL_LCY, TOTAL CR_BAL_LCY';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT DECODE(B.CATEGORY,
                            '1',
                            ' 1 - Asset',
                            '2',
                            '2 - Liability',
                            '3',
                            '3 - Income',
                            '4',
                            '4 - Expense',
                            '5',
                            '5 - Contingent Asset',
                            '6',
                            '6 - Contingent Liability',
                            '7',
                            '7 - Memo',
                            '8',
                            '8 - Position',
                            '9',
                            '9 - Position Equivalent') CATEGORY_NAME,
                     B.parent_gl,
                     SUM(B.dr_bal) dr_bal,
                     SUM(B.cr_bal) cr_bal,
                     SUM(B.dr_bal_lcy) dr_bal_lcy,
                     SUM(B.cr_bal_lcy) cr_bal_lcy
                FROM INTEGRATEDPP.GLZB_GL_BAL   B,
                     INTEGRATEDPP.GLZM_GLMASTER M
               WHERE B.GL_CODE = M.GL_CODE
                 AND (B.PERIOD_CODE, B.FIN_YEAR) in
                     (select current_period, current_cycle
                        from integratedpp.stzm_branch sb
                       where sb.branch_code = p_branch_code)
                 AND BRANCH_CODE = p_branch_code
                 AND M.RECORD_STAT = 'O'
                 AND M.AUTH_STAT = 'A'
                 and b.leaf = 'Y'
               GROUP BY B.CATEGORY, B.PARENT_GL
               ORDER BY B.CATEGORY) LOOP
    l_line := rec.CATEGORY_NAME || ',' || rec.parent_gl || ',' || rec.dr_bal || ',' ||
              rec.cr_bal || ',' || rec.dr_bal_lcy || ',' || rec.cr_bal_lcy;
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
END PR_52_GL_RECON_SUM_BY_CATEGORY_V14;
/
/
