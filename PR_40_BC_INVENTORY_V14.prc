-- PROCEDURE PR_40_BC_INVENTORY_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_40_BC_INVENTORY_V14" (p_branch_code IN VARCHAR2,
                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '40_BC_INVENTORY_' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'HOST_CODE, BRANCH_CODE, INSTRUMENT_CODE, INSTRUMENT_TYPE, INSTRUMENT_CCY, INSTR_NO, MARK_USED, USAGE_TYPE, REMARKS, SEQ_NO';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select HOST_CODE,
                     BRANCH_CODE,
                     INSTRUMENT_CODE,
                     INSTRUMENT_TYPE,
                     INSTRUMENT_CCY,
                     INSTR_NO,
                     MARK_USED,
                     USAGE_TYPE,
                     REMARKS,
                     SEQ_NO
                from integratedpp.pizb_inst_inventory
               where BRANCH_CODE = P_BRANCH_CODE) LOOP
    l_line := rec.HOST_CODE || ',' || rec.BRANCH_CODE || ',' ||
              rec.INSTRUMENT_CODE || ',' || rec.INSTRUMENT_TYPE || ',' ||
              rec.INSTRUMENT_CCY || ',' || rec.INSTR_NO || ',' ||
              rec.MARK_USED || ',' || rec.USAGE_TYPE || ',' || rec.REMARKS || ',' ||
              rec.SEQ_NO;
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
END PR_40_BC_INVENTORY_V14;
/
/
