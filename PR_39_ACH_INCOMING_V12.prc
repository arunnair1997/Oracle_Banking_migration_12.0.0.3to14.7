-- PROCEDURE PR_39_ACH_INCOMING_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_39_ACH_INCOMING_V12" (p_branch_code IN VARCHAR2,
                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '39_ACH_INCOMING_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CONTRACT_REF_NO, BRANCH_CODE, SOURCE_CODE, NETWORK, CUST_NO, CUST_AC_NO, TXN_AMOUNT, CPTY_AC_NO, CPTY_NAME, BOOKING_DT, ACTIVATION_DT, CONTRACT_STATUS, CUST_NAME, TXN_CCY, INSTRUCTION_DATE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select CONTRACT_REF_NO,
                     BRANCH_CODE,
                     SOURCE_CODE,
                     NETWORK,
                     CUST_NO,
                     CUST_AC_NO,
                     TXN_AMOUNT,
                     CPTY_AC_NO,
                     CPTY_NAME,
                     BOOKING_DT,
                     ACTIVATION_DT,
                     CONTRACT_STATUS,
                     CUST_NAME,
                     TXN_CCY,
                     INSTRUCTION_DATE
                from pctb_contract_master@fcubsv12
               where contract_status = 'A'
                 and auth_status = 'A'
                 and product_type = 'I'
                 and contract_status = 'A'
                 and ACTIVATION_DT >
                     (select today
                        from sttm_dates@fcubsv12
                       where branch_code = '100')
                 and branch_code = p_branch_code) LOOP
    l_line := rec.CONTRACT_REF_NO || ',' || rec.BRANCH_CODE || ',' ||
              rec.SOURCE_CODE || ',' || rec.NETWORK || ',' || rec.CUST_NO || ',' ||
              rec.CUST_AC_NO || ',' || rec.TXN_AMOUNT || ',' ||
              rec.CPTY_AC_NO || ',' || rec.CPTY_NAME || ',' ||
              rec.BOOKING_DT || ',' || rec.ACTIVATION_DT || ',' ||
              rec.CONTRACT_STATUS || ',' || rec.CUST_NAME || ',' ||
              rec.TXN_CCY || ',' || rec.INSTRUCTION_DATE;
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
END PR_39_ACH_INCOMING_V12;
/
/
