-- PROCEDURE PR_36_OUTSTANDING_DD_TT_BC_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_36_OUTSTANDING_DD_TT_BC_V14" (p_branch_code IN VARCHAR2,
                                                           p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '36_OUTSTANDING_DD_TT_BC_' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'INSTRUMENT_DESC, TXN_REF_NO, INSTR_NO, TXN_BRANCH, PAYABLE_BRN_CODE, DR_AC_NO, CUSTOMER_NO, BENEF_NAME, INSTRUMENT_AMOUNT, INSTRUMENT_CCY, INSTRUCTION_DATE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select INSTRUMENT_DESC,
                     TXN_REF_NO,
                     INSTR_NO,
                     TXN_BRANCH,
                     PAYABLE_BRN_CODE,
                     DR_AC_NO,
                     CUSTOMER_NO,
                     BENEF_NAME,
                     INSTRUMENT_AMOUNT,
                     INSTRUMENT_CCY,
                     INSTRUCTION_DATE
                from integratedpp.PIZB_INS_ISSUE
               where TXN_BRANCH = P_BRANCH_CODE
                 and source_code = 'OBTLR') LOOP
    l_line := rec.INSTRUMENT_DESC || ',' || rec.TXN_REF_NO || ',' ||
              rec.INSTR_NO || ',' || rec.TXN_BRANCH || ',' ||
              rec.PAYABLE_BRN_CODE || ',' || rec.DR_AC_NO || ',' ||
              rec.CUSTOMER_NO || ',' || rec.BENEF_NAME || ',' ||
              rec.INSTRUMENT_AMOUNT || ',' || rec.INSTRUMENT_CCY || ',' ||
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
END PR_36_OUTSTANDING_DD_TT_BC_V14;
/
/
