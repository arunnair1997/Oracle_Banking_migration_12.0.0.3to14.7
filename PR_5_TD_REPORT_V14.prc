-- PROCEDURE PR_5_TD_REPORT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_5_TD_REPORT_V14" (p_branch_code IN VARCHAR2,
                                               p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '5_TD_REPORT_' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACC, BRN, CCY, PROD, ACCOUNT_CLASS, TD_AMOUNT, INT_START_DATE, INTEREST_RATE, LAST_IS_DATE, ROLLOVER_TYPE, MATURITY_DATE, MATURITY_AMOUNT, RULE, INT_CALC_METHOD, UDE_EFF_DT, UDE_ID, UDE_VALUE, RATE_CODE, UDE_VARIANCE, AMOUNT_BLOCK_NO, AMOUNT, LOCK_IN_DAYS, LOCK_IN_MONTHS, LOCK_IN_YEARS, SIGNATURE_RECORD_STATUS, CIF_SIG_ID, JOINT_HOLDER, ???? ??? ???????, CUSTOMER AGE, ???? ??????, REPRESENTATIVES, TDPAYOUT_VALIDATION';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select * from td_report where brn=p_branch_code ) LOOP
    l_line :=rec.ACC|| ','|| rec.BRN|| ','|| rec.CCY|| ','|| rec.PROD|| ','|| rec.ACCOUNT_CLASS|| ','|| rec.TD_AMOUNT|| ','|| rec.INT_START_DATE|| ','|| rec.INTEREST_RATE|| ','|| rec.LAST_IS_DATE|| ','|| rec.ROLLOVER_TYPE|| ','|| rec.MATURITY_DATE|| ','|| rec.MATURITY_AMOUNT|| ','|| rec.RULE|| ','|| rec.INT_CALC_METHOD|| ','|| rec.UDE_EFF_DT|| ','|| rec.UDE_ID|| ','|| rec.UDE_VALUE|| ','|| rec.RATE_CODE|| ','|| rec.UDE_VARIANCE|| ','|| rec.AMOUNT_BLOCK_NO|| ','|| rec.AMOUNT|| ','|| rec.LOCK_IN_DAYS|| ','|| rec.LOCK_IN_MONTHS|| ','|| rec.LOCK_IN_YEARS|| ','|| rec.RECORD_STAT|| ','|| rec.CIF_SIG_ID|| ','|| rec.JOINT_HOLDER_LIST|| ','|| rec.FIELD_VAL_1|| ','|| rec.FIELD_VAL_2|| ','|| rec.FIELD_VAL_3|| ','|| rec.FIELD_VAL_5|| ','|| rec.FIELD_VAL_6;
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
END pr_5_TD_REPORT_v14;
/
/
