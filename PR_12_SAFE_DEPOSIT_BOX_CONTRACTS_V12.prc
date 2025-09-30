-- PROCEDURE PR_12_SAFE_DEPOSIT_BOX_CONTRACTS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_12_SAFE_DEPOSIT_BOX_CONTRACTS_V12" (p_branch_code IN VARCHAR2,
                                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename     VARCHAR2(200);
  v_record_count NUMBER := 0;
  v_file_index   NUMBER := 1;
  v_max_records constant number := 200000;

  procedure open_new_file is
  BEGIN
    -- Construct filename
    l_filename := '12_SAFE_DEPOSIT_BOX_CONTRACTS_' || p_branch_code ||
                  '_v12.csv';
    l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
    dbms_output.put_line('CHECK1');
    -- Write header line
    l_line := 'CONTRACT_REF_NO, PRODUCT_CODE, PRODUCT_DESCRIPTION, PRODUCT_TYPE, BOOK_DATE, ISSUE_DATE, EFFECTIVE_DATE, EXPIRY_DATE, BREAK_OPEN_DT, VERSION_NO, EVENT_SEQ_NO, EVENT_CODE, BOX_TYPE, BOX_DIMENSION, BOX_DESCRIPTION, VAULT_CODE, CONTRACT_CCY, RENTAL_MODE, FIRST_RENTDUEDT, NEXT_RENTDUEDT, DEPOSIT_AMT, CASA_ACCOUNT, CIF_ID, CUST_TYPE, CUST_NAME, SETTLEMENT_ACCOUNT, SETTLEMENT_BRANCH, SETTLEMENT_CCY, AUTO_LIQD, AC_STAT_DORMANT, AC_STAT_FROZEN, DL_LANG_CODE, VISITED_DATE, ENTRY_TIME, EXIT_TIME, TENOR, AUTO_ROLLOVER';
    UTL_FILE.PUT_LINE(l_file, l_line);
  END;

BEGIN
  open_new_file;
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT M.CONTRACT_REF_NO,
                     M.PRODUCT_CODE,
                     CP.PRODUCT_DESCRIPTION,
                     C.PRODUCT_TYPE,
                     C.BOOK_DATE,
                     M.ISSUE_DATE,
                     M.EFFECTIVE_DATE,
                     M.EXPIRY_DATE,
                     M.BREAK_OPEN_DT,
                     M.VERSION_NO,
                     M.EVENT_SEQ_NO,
                     M.EVENT_CODE,
                     M.BOX_TYPE,
                     M.BOX_DIMENSION,
                     M.BOX_DESCRIPTION,
                     M.VAULT_CODE,
                     M.CONTRACT_CCY,
                     M.RENTAL_MODE,
                     M.FIRST_RENTDUEDT,
                     M.NEXT_RENTDUEDT,
                     M.DEPOSIT_AMT,
                     M.CASA_ACCOUNT,
                     M.CIF_ID,
                     M.CUST_TYPE,
                     M.CUST_NAME,
                     M.SETTLEMENT_ACCOUNT,
                     M.SETTLEMENT_BRANCH,
                     M.SETTLEMENT_CCY,
                     M.AUTO_LIQD,
                     M.AC_STAT_DORMANT,
                     M.AC_STAT_FROZEN,
                     M.DL_LANG_CODE,
                     V.VISITED_DATE,
                     V.ENTRY_TIME,
                     V.EXIT_TIME,
                     M.TENOR,
                     M.AUTO_ROLLOVER
                FROM UBSPROD.DLTB_CONTRACT_MASTER@fcubsv12 M
                JOIN UBSPROD.CSTB_CONTRACT@fcubsv12 C
                  ON M.CONTRACT_REF_NO = C.CONTRACT_REF_NO
                 AND M.VERSION_NO = C.LATEST_VERSION_NO
                 AND M.EVENT_SEQ_NO = C.LATEST_EVENT_SEQ_NO
                LEFT JOIN UBSPROD.DLTB_VISITOR_INFO@fcubsv12 V
                  ON V.CONTRACT_REF_NO = M.CONTRACT_REF_NO
                 AND V.EVENT_SEQ_NO = M.EVENT_SEQ_NO
                LEFT JOIN UBSPROD.CSTM_PRODUCT@fcubsv12 CP
                  ON M.PRODUCT_CODE = CP.PRODUCT_CODE
                 AND CP.MODULE = 'DL'
                 AND CP.RECORD_STAT = 'O'
                 AND CP.ONCE_AUTH = 'Y'
               WHERE C.CONTRACT_STATUS = 'A'
                 AND C.AUTH_STATUS = 'A'
                 AND M.SETTLEMENT_BRANCH = p_branch_code) LOOP
    v_record_count := v_record_count + 1;
    if v_record_count > v_max_records then
      utl_file.fclose(l_file);
      v_file_index   := v_file_index + 1;
      v_record_count := 1;
      open_new_file;
    end if;
    l_line := rec.CONTRACT_REF_NO || ',' || rec.PRODUCT_CODE || ',' ||
              rec.PRODUCT_DESCRIPTION || ',' || rec.PRODUCT_TYPE || ',' ||
              rec.BOOK_DATE || ',' || rec.ISSUE_DATE || ',' ||
              rec.EFFECTIVE_DATE || ',' || rec.EXPIRY_DATE || ',' ||
              rec.BREAK_OPEN_DT || ',' || rec.VERSION_NO || ',' ||
              rec.EVENT_SEQ_NO || ',' || rec.EVENT_CODE || ',' ||
              rec.BOX_TYPE || ',' || rec.BOX_DIMENSION || ',' ||
              rec.BOX_DESCRIPTION || ',' || rec.VAULT_CODE || ',' ||
              rec.CONTRACT_CCY || ',' || rec.RENTAL_MODE || ',' ||
              rec.FIRST_RENTDUEDT || ',' || rec.NEXT_RENTDUEDT || ',' ||
              rec.DEPOSIT_AMT || ',' || rec.CASA_ACCOUNT || ',' ||
              rec.CIF_ID || ',' || rec.CUST_TYPE || ',' || rec.CUST_NAME || ',' ||
              rec.SETTLEMENT_ACCOUNT || ',' || rec.SETTLEMENT_BRANCH || ',' ||
              rec.SETTLEMENT_CCY || ',' || rec.AUTO_LIQD || ',' ||
              rec.AC_STAT_DORMANT || ',' || rec.AC_STAT_FROZEN || ',' ||
              rec.DL_LANG_CODE || ',' || rec.VISITED_DATE || ',' ||
              rec.ENTRY_TIME || ',' || rec.EXIT_TIME || ',' || rec.TENOR || ',' ||
              rec.AUTO_ROLLOVER;
    UTL_FILE.PUT_LINE(l_file, l_line);
  END LOOP;
  dbms_output.put_line('CHECK3');
  if utl_file.is_open(l_file) then
    UTL_FILE.FCLOSE(l_file);
  end if;

EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('BOMBED' || SQLERRM);
    IF UTL_FILE.IS_OPEN(l_file) THEN
      UTL_FILE.FCLOSE(l_file);
    END IF;
    RAISE;
END pr_12_SAFE_DEPOSIT_BOX_CONTRACTS_v12;
/
/
