-- PROCEDURE PR_40_BC_INVENTORY_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_40_BC_INVENTORY_V12" (p_branch_code IN VARCHAR2,
                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '40_BC_INVENTORY_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := ' ISSUING_BRANCH, INSTR_TYPE, INSTR_CCY, INSTR_NO, EVENT_SEQ_NO';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select ISSUING_BRANCH,
                     INSTR_TYPE,
                     INSTR_CCY,
                     INSTR_NO,
                     EVENT_SEQ_NO
                     from UBSPROD.ISTM_INSTR_TXN@FCUBSV12
               where AUTH_STAT='A' AND RECORD_STAT='O' AND
         ISSUING_BRANCH = P_BRANCH_CODE) LOOP
    l_line := rec.ISSUING_BRANCH || ',' || rec.INSTR_TYPE || ',' ||
              rec.INSTR_CCY || ',' || rec.INSTR_NO || ',' ||
              rec.EVENT_SEQ_NO;
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
END PR_40_BC_INVENTORY_V12;
/
/
