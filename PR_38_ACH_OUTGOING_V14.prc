-- PROCEDURE PR_38_ACH_OUTGOING_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_38_ACH_OUTGOING_V14" (p_branch_code IN VARCHAR2,
                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '38_ACH_OUTGOING_' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'TXN_REF_NO, SOURCE_REF_NO, TXN_BRANCH, SOURCE_CODE, NETWORK_CODE, CUSTOMER_NO, DR_AC_NO, TRANSFER_AMT, CR_AC_NO, CR_NAME, TXN_BOOKING_DATE, ACTIVATION_DATE, TXN_STATUS, DR_NAME, TRANSFER_CCY, INSTRUCTION_DATE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT A.TXN_REF_NO,
                     A.SOURCE_REF_NO,
                     A.TXN_BRANCH,
                     A.SOURCE_CODE,
                     A.NETWORK_CODE,
                     A.CUSTOMER_NO,
                     A.DR_AC_NO,
                     A.TRANSFER_AMT,
                     A.CR_AC_NO,
                     A.CR_NAME,
                     A.TXN_BOOKING_DATE,
                     A.ACTIVATION_DATE,
                     A.TXN_STATUS,
                     A.DR_NAME,
                     A.TRANSFER_CCY,
                     A.INSTRUCTION_DATE
                FROM integratedpp.PYZB_OUT_TXN_DRIVER A
               WHERE A.TXN_BRANCH = p_branch_code) LOOP
    l_line := rec.TXN_REF_NO || ',' || rec.SOURCE_REF_NO || ',' ||
              rec.TXN_BRANCH || ',' || rec.SOURCE_CODE || ',' ||
              rec.NETWORK_CODE || ',' || rec.CUSTOMER_NO || ',' ||
              rec.DR_AC_NO || ',' || rec.TRANSFER_AMT || ',' ||
              rec.CR_AC_NO || ',' || rec.CR_NAME || ',' ||
              rec.TXN_BOOKING_DATE || ',' || rec.ACTIVATION_DATE || ',' ||
              rec.TXN_STATUS || ',' || rec.DR_NAME || ',' ||
              rec.TRANSFER_CCY || ',' || rec.INSTRUCTION_DATE;
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
END PR_38_ACH_OUTGOING_V14;
/
/
