-- PROCEDURE PR_23_COLLATERAL_DETAILS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_23_COLLATERAL_DETAILS_V12" (p_branch_code IN VARCHAR2,
                                                         p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '23_Collateral_Details_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'LIAB_NO, COLLATERAL_TYPE, CHARGE_TYPE, COLLATERAL_CODE, COLLATERAL_CURRENCY, COLLATERAL_VALUE, HAIRCUT, UTIL_AMT, BLOCK_AMT, AVAILABLE_AMOUNT, INT_RATE, CONTRACT_REF_NO, BLOCK_REF_NO';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select liab.liab_no,
                     col.collateral_type,
                     col.charge_type,
                     col.collateral_code,
                     col.collateral_currency,
                     col.collateral_value,
                     col.haircut,
                     col.util_amt,
                     col.BLOCK_AMT,
                     col.available_amount,
                     col.INTEREST_RATE,
                     contt.CONTRACT_REF_NO,
                     contt.BLOCK_REF_NO
                from ubsprod.getm_collat@fcubsv12              col,
                     ubsprod.getm_liab@fcubsv12                liab,
                     ubsprod.GeTM_COLLAT_CONT_CONTRIB@fcubsv12 contt
               where col.liab_id = liab.id
                 and contt.coll_id = col.id
                 and col.record_stat = 'O'
                 and liab.liab_branch = p_branch_code
                 and col.auth_stat = 'A') LOOP
    l_line := rec.LIAB_NO || ',' || rec.COLLATERAL_TYPE || ',' ||
              rec.CHARGE_TYPE || ',' || rec.COLLATERAL_CODE || ',' ||
              rec.COLLATERAL_CURRENCY || ',' || rec.COLLATERAL_VALUE || ',' ||
              rec.HAIRCUT || ',' || rec.UTIL_AMT || ',' || rec.BLOCK_AMT || ',' ||
              rec.AVAILABLE_AMOUNT || ',' || rec.INTEREST_RATE || ',' ||
              rec.CONTRACT_REF_NO || ',' || rec.BLOCK_REF_NO;
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
END pr_23_Collateral_Details_v12;
/
/
