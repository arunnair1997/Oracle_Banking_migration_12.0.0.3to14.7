-- PROCEDURE PR_36_OUTSTANDING_DD_TT_BC_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_36_OUTSTANDING_DD_TT_BC_V12" (p_branch_code IN VARCHAR2,
                                                           p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '36_OUTSTANDING_DD_TT_BC_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT_CODE, INSTR_TYPE, INSTR_NO, ISSUING_BRANCH, PAYABLE_BRANCH, AC_NO, REL_CUSTOMER, BENEF_NAME, INSTR_AMOUNT, INSTR_CCY, INSTRUCTION_DATE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT PRODUCT_CODE,
                     INSTR_TYPE,
                     INSTR_NO,
                     ISSUING_BRANCH,
                     PAYABLE_BRANCH,
                     AC_NO,
                     REL_CUSTOMER,
                     substr(BENEF_NAME, 1, 65) BENEF_NAME,
                     INSTR_AMOUNT,
                     INSTR_CCY,
                     TO_CHAR(INSTR_DATE, 'YYYY-MM-DD') INSTRUCTION_DATE
                FROM istm_instr_txn@fcubsv12
               WHERE instr_stat IN ('INIT')
                 AND ISSUING_BRANCH = p_BRANCH_CODE) LOOP
    l_line := rec.PRODUCT_CODE || ',' || rec.INSTR_TYPE || ',' ||
              rec.INSTR_NO || ',' || rec.ISSUING_BRANCH || ',' ||
              rec.PAYABLE_BRANCH || ',' || rec.AC_NO || ',' ||
              rec.REL_CUSTOMER || ',' || rec.BENEF_NAME || ',' ||
              rec.INSTR_AMOUNT || ',' || rec.INSTR_CCY || ',' ||
              rec.INSTRUCTION_DATE;
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
END PR_36_OUTSTANDING_DD_TT_BC_V12;
/
/
