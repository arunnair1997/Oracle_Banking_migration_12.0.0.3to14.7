-- PROCEDURE PR_6_IA_CASA_REPORT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_6_IA_CASA_REPORT_V14" (p_branch_code IN VARCHAR2,
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
    l_filename := '6_ISLAMIC_CASA_REPORT_' || p_branch_code || '_v14.csv';
    l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
    dbms_output.put_line('CHECK1');
    -- Write header line
    l_line := 'CUST_AC_NO, BRANCH_CODE, AC_DESC, CUST_NO, CCY, ACCOUNT_CLASS, AC_STAT_NO_DR, AC_STAT_NO_CR, AC_STAT_BLOCK, AC_STAT_STOP_PAY, AC_STAT_DORMANT, JOINT_AC_INDICATOR, AC_OPEN_DATE, AC_STMT_DAY, AC_STMT_CYCLE, ALT_AC_NO, CHEQUE_BOOK_FACILITY, ATM_FACILITY, AC_STMT_TYPE, UNCOLL_FUNDS_LIMIT, AC_STAT_FROZEN, DR_GL, CR_GL, RECORD_STAT, AUTH_STAT, MOD_NO, MAKER_ID, MAKER_DT_STAMP, CHECKER_ID, CHECKER_DT_STAMP, ONCE_AUTH, CAS_ACCOUNT, ACY_OPENING_BAL, LCY_OPENING_BAL, ACY_TANK_CR, ACY_TANK_DR, LCY_TANK_CR, LCY_TANK_DR, ACY_TANK_UNCOLLECTED, ACY_CURR_BALANCE, LCY_CURR_BALANCE, ACY_BLOCKED_AMOUNT, ACY_AVL_BAL, ACY_UNAUTH_DR, ACY_UNAUTH_TANK_DR, ACY_UNAUTH_CR, ACY_UNAUTH_TANK_CR, ACY_UNAUTH_UNCOLLECTED, ACY_UNAUTH_TANK_UNCOLLECTED, DORMANCY_START_DT, STATUS_CHANGE_DATE, SEQ_NO, AC_STAT_DE_POST, DORMANT, CHARGE_ON_HOLD_MAIL, SIGNATURE_RECORD_STATUS, CIF_SIG_ID, JOINT_HOLDER, ??? ??????, ??? ??????, ????? ???????, ??????? 111, ?????? ???? ?? ????, ????? ????, ????? ??????, PENSION_LINK_1, PENSION_LINK_2, PENSION_LINK_3, PENSION_LINK_4, ???? ?????, ????? ????? ????????? ??????????? C, ??? ???? ??????, YOUTH AGE, REPRESENTATIVES, ????? ????? ????? ???????, ????? ???????? ?? ????? ???????, NI CAPITAL ????? ???????, WOMEN_BANKING, ACCOUNT_IBAN_NUMBER, TRANSFER_FREQUENCY, SWIFT_CODE, TRANSFER_EXPIRY_DT, BENEFICIARY NAME, PURPOSE-????? ???????';
    UTL_FILE.PUT_LINE(l_file, l_line);
  END;

BEGIN
  open_new_file;
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select *
                from ISLAMIC_CASA_REPORT
               where branch_code = P_branch_code) LOOP
    v_record_count := v_record_count + 1;
    if v_record_count > v_max_records then
      utl_file.fclose(l_file);
      v_file_index   := v_file_index + 1;
      v_record_count := 1;
      open_new_file;
    end if;
    l_line := rec.CUST_AC_NO || ',' || rec.BRANCH_CODE || ',' ||
              rec.AC_DESC || ',' || rec.CUST_NO || ',' || rec.CCY || ',' ||
              rec.ACCOUNT_CLASS || ',' || rec.AC_STAT_NO_DR || ',' ||
              rec.AC_STAT_NO_CR || ',' || rec.AC_STAT_BLOCK || ',' ||
              rec.AC_STAT_STOP_PAY || ',' || rec.AC_STAT_DORMANT || ',' ||
              rec.JOINT_AC_INDICATOR || ',' || rec.AC_OPEN_DATE || ',' ||
              rec.AC_STMT_DAY || ',' || rec.AC_STMT_CYCLE || ',' ||
              rec.ALT_AC_NO || ',' || rec.CHEQUE_BOOK_FACILITY || ',' ||
              rec.ATM_FACILITY || ',' || rec.AC_STMT_TYPE || ',' ||
              rec.UNCOLL_FUNDS_LIMIT || ',' || rec.AC_STAT_FROZEN || ',' ||
              rec.DR_GL || ',' || rec.CR_GL || ',' || rec.RECORD_STAT || ',' ||
              rec.AUTH_STAT || ',' || rec.MOD_NO || ',' || rec.MAKER_ID || ',' ||
              rec.MAKER_DT_STAMP || ',' || rec.CHECKER_ID || ',' ||
              rec.CHECKER_DT_STAMP || ',' || rec.ONCE_AUTH || ',' ||
              rec.CAS_ACCOUNT || ',' || rec.ACY_OPENING_BAL || ',' ||
              rec.LCY_OPENING_BAL || ',' || rec.ACY_TANK_CR || ',' ||
              rec.ACY_TANK_DR || ',' || rec.LCY_TANK_CR || ',' ||
              rec.LCY_TANK_DR || ',' || rec.ACY_TANK_UNCOLLECTED || ',' ||
              rec.ACY_CURR_BALANCE || ',' || rec.LCY_CURR_BALANCE || ',' ||
              rec.ACY_BLOCKED_AMOUNT || ',' || rec.ACY_AVL_BAL || ',' ||
              rec.ACY_UNAUTH_DR || ',' || rec.ACY_UNAUTH_TANK_DR || ',' ||
              rec.ACY_UNAUTH_CR || ',' || rec.ACY_UNAUTH_TANK_CR || ',' ||
              rec.ACY_UNAUTH_UNCOLLECTED || ',' ||
              rec.ACY_UNAUTH_TANK_UNCOLLECTED || ',' ||
              rec.DORMANCY_START_DT || ',' || rec.STATUS_CHANGE_DATE || ',' ||
              rec.SEQ_NO || ',' || rec.AC_STAT_DE_POST || ',' ||
              rec.DORMANT || ',' || rec.CHARGE_ON_HOLD_MAIL || ',' ||
              rec.SIGNATURE_RECORD_STATUS || ',' || rec.CIF_SIG_ID || ',' ||
              rec.JOINT_HOLDER || ',' || rec.FIELD_VAL_1 || ',' ||
              rec.FIELD_VAL_2 || ',' || rec.FIELD_VAL_3 || ',' ||
              rec.FIELD_VAL_4 || ',' || rec.FIELD_VAL_5 || ',' ||
              rec.FIELD_VAL_6 || ',' || rec.FIELD_VAL_7 || ',' ||
              rec.FIELD_VAL_8 || ',' || rec.FIELD_VAL_9 || ',' ||
              rec.FIELD_VAL_10 || ',' || rec.FIELD_VAL_11 || ',' ||
              rec.FIELD_VAL_12 || ',' || rec.FIELD_VAL_13 || ',' ||
              rec.FIELD_VAL_14 || ',' || rec.FIELD_VAL_15 || ',' ||
              rec.FIELD_VAL_16 || ',' || rec.FIELD_VAL_17 || ',' ||
              rec.FIELD_VAL_18 || ',' || rec.FIELD_VAL_19 || ',' ||
              rec.FIELD_VAL_20 || ',' || rec.FIELD_VAL_21 || ',' ||
              rec.FIELD_VAL_22 || ',' || rec.FIELD_VAL_23 || ',' ||
              rec.FIELD_VAL_24 || ',' || rec.FIELD_VAL_25 || ',' ||
              rec.FIELD_VAL_26;
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
END pr_6_IA_CASA_REPORT_v14;
/
/
