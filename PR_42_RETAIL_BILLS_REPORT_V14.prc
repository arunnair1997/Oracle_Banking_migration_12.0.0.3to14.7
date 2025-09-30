-- PROCEDURE PR_42_RETAIL_BILLS_REPORT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_42_RETAIL_BILLS_REPORT_V14" (p_branch_code IN VARCHAR2,
                                                          p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '42_Retail_Bills_Report' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CONTRACT_REF_NO, BRANCH, COUNTERPARTY, COUNTERPARTY_AC_NO, CUSTOMER_NAME1, INSTRUMENT_DATE, INSTRUMENT_CODE, INSTRUMENT_DESCR, PRODUCT_CODE, PRODUCT_TYPE, CONTRACT_STATUS, MATURITY_DATE, GRACE_DAYS, DISPATCH_LEAD_DAYS, RECORD_STAT, AUTH_STAT, TRACK_LIMIT, FINANCING, COLLATERAL, MARGIN_PERCENTAGE, EVENT_SEQ_NO, EVENT_CODE, REVERSAL_INCLUDES_CHARGES, BULK_INPUT_REF_NO, LIQ_ON_BOOK, OTHER_PARTY_ID, RISK_START, RISK_END, OTHER_PARTY_LIMIT, CUST_ID, OTHER_PARTY_NAME, ADDRESS_LINE_1, COLLECTING_BANK_NAME, COLLECTING_BANK_CODE, AVAIL_AMT, LIMIT_AMT, COL_CAP_AMT, ISSUER_BANK, ISSUER_BRANCH, CLEARING_BANK, CLEARING_BRANCH, BOOKING_DATE, ACTIVATION_DATE, CONTRACT_CCY, CONTRACT_AMT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT M.CONTRACT_REF_NO,
                     C.BRANCH,
                     M.COUNTERPARTY,
                     M.COUNTERPARTY_AC_NO,
                     CP.PARTY_NAME               CUSTOMER_NAME1,
                     M.INSTRUMENT_DATE,
                     I.INSTRUMENT_CODE,
                     I.INSTRUMENT_DESCR,
                     M.PRODUCT_CODE,
                     I.PRODUCT_TYPE,
                     C.CONTRACT_STATUS,
                     M.MATURITY_DATE,
                     M.GRACE_DAYS,
                     I.DISPATCH_LEAD_DAYS,
                     I.RECORD_STAT,
                     I.AUTH_STAT,
                     M.TRACK_LIMIT,
                     M.FINANCING,
                     M.COLLATERAL,
                     M.MARGIN_PERCENTAGE,
                     M.EVENT_SEQ_NO,
                     M.EVENT_CODE,
                     M.REVERSAL_INCLUDES_CHARGES,
                     M.BULK_INPUT_REF_NO,
                     M.LIQ_ON_BOOK,
                     M.OTHER_PARTY_ID,
                     PC.RISK_START,
                     PC.RISK_END,
                     M.OTHER_PARTY_LIMIT,
                     P.CUST_ID,
                     P.PTY_NAME                  OTHER_PARTY_NAME,
                     P.ADDRESS_LINE_1,
                     D.clg_bank_code             COLLECTING_BANK_NAME,
                     E.branch_desc               COLLECTING_BANK_CODE,
                     P.AVAIL_AMT,
                     P.LIMIT_AMT,
                     P.COL_CAP_AMT,
                     M.ISSUER_BANK,
                     M.ISSUER_BRANCH,
                     M.CLEARING_BANK,
                     M.CLEARING_BRANCH,
                     M.BOOKING_DATE,
                     M.ACTIVATION_DATE,
                     M.CONTRACT_CCY,
                     M.CONTRACT_AMT
                FROM INTEGRATEDPP.RBzB_CONTRACT_MASTER M
                LEFT OUTER JOIN INTEGRATEDPP.rbzb_contract_parties CP
                  ON (M.CONTRACT_REF_NO = CP.CONTRACT_REF_NO AND
                     CP.PARTY_TYPE = 'DRAWER' AND
                     M.EVENT_SEQ_NO = CP.EVENT_SEQ_NO)
                LEFT OUTER JOIN INTEGRATEDPP.RBzM_INSTRUMENTS I
                  ON (M.INSTRUMENT_CODE = I.INSTRUMENT_CODE)
                LEFT OUTER JOIN INTEGRATEDPP.RBzM_OTHER_PARTY P
                  ON (M.OTHER_PARTY_ID = P.OTHR_PTY_ID)
                LEFT OUTER JOIN INTEGRATEDPP.CSzB_CONTRACT C
                  ON (M.CONTRACT_REF_NO = C.CONTRACT_REF_NO AND
                     M.version_no = C.latest_version_no and
                     m.event_seq_no = c.latest_event_seq_no)
                LEFT OUTER JOIN INTEGRATEDPP.RBzM_OTHER_PARTY_CUSTOM PC
                  ON (M.OTHER_PARTY_ID = PC.OTHR_PTY_ID)
                LEFT OUTER JOIN INTEGRATEDPP.dezm_clg_bank_code D
                  ON (D.clg_bank_code = M.coll_rem_bank_code)
                LEFT OUTER JOIN INTEGRATEDPP.dezm_clg_brn_code E
                  ON (E.branch_code = M.coll_rem_branch_code and
                     E.bank_code = M.coll_rem_bank_code)
               WHERE C.CONTRACT_STATUS = 'A'
                 AND C.AUTH_STATUS = 'A'
                 AND C.BRANCH = p_branch_code) LOOP
    l_line := rec.CONTRACT_REF_NO || ',' || rec.BRANCH || ',' ||
              rec.COUNTERPARTY || ',' || rec.COUNTERPARTY_AC_NO || ',' ||
              rec.CUSTOMER_NAME1 || ',' || rec.INSTRUMENT_DATE || ',' ||
              rec.INSTRUMENT_CODE || ',' || rec.INSTRUMENT_DESCR || ',' ||
              rec.PRODUCT_CODE || ',' || rec.PRODUCT_TYPE || ',' ||
              rec.CONTRACT_STATUS || ',' || rec.MATURITY_DATE || ',' ||
              rec.GRACE_DAYS || ',' || rec.DISPATCH_LEAD_DAYS || ',' ||
              rec.RECORD_STAT || ',' || rec.AUTH_STAT || ',' ||
              rec.TRACK_LIMIT || ',' || rec.FINANCING || ',' ||
              rec.COLLATERAL || ',' || rec.MARGIN_PERCENTAGE || ',' ||
              rec.EVENT_SEQ_NO || ',' || rec.EVENT_CODE || ',' ||
              rec.REVERSAL_INCLUDES_CHARGES || ',' || rec.BULK_INPUT_REF_NO || ',' ||
              rec.LIQ_ON_BOOK || ',' || rec.OTHER_PARTY_ID || ',' ||
              rec.RISK_START || ',' || rec.RISK_END || ',' ||
              rec.OTHER_PARTY_LIMIT || ',' || rec.CUST_ID || ',' ||
              rec.OTHER_PARTY_NAME || ',' || rec.ADDRESS_LINE_1 || ',' ||
              rec.COLLECTING_BANK_NAME || ',' || rec.COLLECTING_BANK_CODE || ',' ||
              rec.AVAIL_AMT || ',' || rec.LIMIT_AMT || ',' ||
              rec.COL_CAP_AMT || ',' || rec.ISSUER_BANK || ',' ||
              rec.ISSUER_BRANCH || ',' || rec.CLEARING_BANK || ',' ||
              rec.CLEARING_BRANCH || ',' || rec.BOOKING_DATE || ',' ||
              rec.ACTIVATION_DATE || ',' || rec.CONTRACT_CCY || ',' ||
              rec.CONTRACT_AMT;
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
END pr_42_Retail_Bills_Report_v14;
/
/
