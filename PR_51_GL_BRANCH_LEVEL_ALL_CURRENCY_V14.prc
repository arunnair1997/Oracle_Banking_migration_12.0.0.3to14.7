-- PROCEDURE PR_51_GL_BRANCH_LEVEL_ALL_CURRENCY_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_51_GL_BRANCH_LEVEL_ALL_CURRENCY_V14" (p_branch_code IN VARCHAR2,
                                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '51_GL_BRANCH_LEVEL_ALL_CURRENCY' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'BRANCH_CODE, PARENT_GL, GL_CODE, GL_DESC, FIN_YEAR, PERIOD_CODE, CCY_CODE, LEAF, CATEGORY, DR_BAL, CR_BAL, DR_BAL_LCY, CR_BAL_LCY, DR_MOV, CR_MOV, DR_MOV_LCY, CR_MOV_LCY';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT B.branch_code,
                     B.parent_gl,
                     B.gl_code,
                     M.gl_desc,
                     B.fin_year,
                     B.period_code,
                     B.ccy_code,
                     B.leaf,
                     DECODE(B.CATEGORY,
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
                            '9 - Position Equivalent') CATEGORY,
                     B.dr_bal,
                     B.cr_bal,
                     B.dr_bal_lcy,
                     B.cr_bal_lcy,
                     B.dr_mov,
                     B.cr_mov,
                     B.dr_mov_lcy,
                     B.cr_mov_lcy
                FROM INTEGRATEDPP.GLZB_GL_BAL   B,
                     INTEGRATEDPP.GLZM_GLMASTER M
               WHERE B.GL_CODE = M.GL_CODE
                 AND B.BRANCH_CODE = p_branch_code
                 AND (B.PERIOD_CODE, B.FIN_YEAR) in
                     (select current_period, current_cycle
                        from integratedpp.stzm_branch sb
                       where sb.branch_code = p_branch_code)
                 AND M.RECORD_STAT = 'O'
                 AND M.AUTH_STAT = 'A'
                 AND b.leaf = 'Y'
               ORDER BY B.FIN_YEAR, B.PERIOD_CODE) LOOP
    l_line := rec.BRANCH_CODE || ',' || rec.PARENT_GL || ',' || rec.GL_CODE || ',' ||
              rec.GL_DESC || ',' || rec.FIN_YEAR || ',' || rec.PERIOD_CODE || ',' ||
              rec.CCY_CODE || ',' || rec.LEAF || ',' || rec.CATEGORY || ',' ||
              rec.DR_BAL || ',' || rec.CR_BAL || ',' || rec.DR_BAL_LCY || ',' ||
              rec.CR_BAL_LCY || ',' || rec.DR_MOV || ',' || rec.CR_MOV || ',' ||
              rec.DR_MOV_LCY || ',' || rec.CR_MOV_LCY;
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
END PR_51_GL_BRANCH_LEVEL_ALL_CURRENCY_V14;
/
/
