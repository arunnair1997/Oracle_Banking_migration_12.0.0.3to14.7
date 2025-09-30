-- PROCEDURE PR_4_CASA_REPORT_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_4_CASA_REPORT_V12" (p_branch_code IN VARCHAR2,
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
    l_filename := '4_CASA_REPORT_' || p_branch_code || '_v12.csv';
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
  FOR rec IN (

    WITH Latest_AFC AS (
    SELECT *
    FROM (
        SELECT AFC.ACCOUNT,
               AFC.BRANCH,
               AFC.TRNREFNO,
               AFC.TRNCODE,
               AFC.TRN_DT,
               AFC.AMOUNT,
               ROW_NUMBER() OVER (PARTITION BY AFC.ACCOUNT, AFC.BRANCH ORDER BY AFC.TRN_DT DESC) AS rn
        FROM UBSPROD.ACTB_FUNCOL@FCUBSV12 AFC
        WHERE AFC.auth_stat = 'A'
          AND AFC.record_stat = 'O'
    ) AFC_INNER
    WHERE AFC_INNER.rn = 1
),
Latest_ACS AS (
    SELECT *
    FROM (
        SELECT ACS.CUST_AC_NO,
               ACS.BRANCH_CODE,
               ACS.STATUS_CHANGE_DATE,
               ACS.SEQ_NO,
               ACS.AC_STAT_DE_POST,
               ACS.DORMANT,
               ROW_NUMBER() OVER (PARTITION BY ACS.CUST_AC_NO, ACS.BRANCH_CODE ORDER BY ACS.STATUS_CHANGE_DATE DESC) AS rn
        FROM UBSPROD.STTM_AC_STAT_CHANGE@FCUBSV12 ACS
        WHERE ACS.CUST_AC_NO IN (
            SELECT CUST_AC_NO
            FROM UBSPROD.STTM_CUST_ACCOUNT@FCUBSV12
            WHERE record_stat = 'O'
              AND AUTH_STAT = 'A'
              AND CUST_NO IN (
                  SELECT CUSTOMER_NO
                  FROM UBSPROD.sttm_customer@FCUBSV12
                  WHERE record_stat = 'O'
                    AND AUTH_STAT = 'A'
              )
        )
    ) ACS_INNER
    WHERE ACS_INNER.rn = 1
),
Latest_CAD AS (
    SELECT *
    FROM (
        SELECT CAD.CUST_AC_NO,
               CAD.BRANCH_CODE,
               CAD.DORMANCY_START_DT,
               ROW_NUMBER() OVER (PARTITION BY CAD.CUST_AC_NO, CAD.BRANCH_CODE ORDER BY CAD.DORMANCY_START_DT DESC) AS rn
        FROM UBSPROD.STTM_CUST_ACCOUNT_DORMANCY@FCUBSV12 CAD
        WHERE CAD.CUST_AC_NO IN (
            SELECT CUST_AC_NO
            FROM UBSPROD.STTM_CUST_ACCOUNT@FCUBSV12
            WHERE record_stat = 'O'
              AND AUTH_STAT = 'A'
              AND CUST_NO IN (
                  SELECT CUSTOMER_NO
                  FROM UBSPROD.sttm_customer@FCUBSV12
                  WHERE record_stat = 'O'
                    AND AUTH_STAT = 'A'
              )
        )
    ) CAD_INNER
    WHERE CAD_INNER.rn = 1
),
Latest_ASIG AS (
    SELECT *
    FROM (
        SELECT ASIG.ACC_NO,
               ASIG.BRANCH,
               ASIG.RECORD_STAT,
               ASIG.CIF_SIG_ID,
               ROW_NUMBER() OVER (PARTITION BY ASIG.ACC_NO, ASIG.BRANCH ORDER BY ASIG.CIF_SIG_ID DESC) AS rn
        FROM UBSPROD.SVTM_ACC_SIG_DET@FCUBSV12 ASIG
        WHERE ASIG.ACC_NO IN (
            SELECT CUST_AC_NO
            FROM UBSPROD.STTM_CUST_ACCOUNT@FCUBSV12
            WHERE RECORD_STAT = 'O'
              AND AUTH_STAT = 'A'
        )
          AND ASIG.RECORD_STAT <> 'D'
    ) ASIG_INNER
    WHERE ASIG_INNER.rn = 1
),
Latest_AJH AS (
    SELECT *
    FROM (
        SELECT AJH.CUST_AC_NO,
               AJH.BRANCH_CODE,
               AJH.JOINT_HOLDER,
               ROW_NUMBER() OVER (PARTITION BY AJH.CUST_AC_NO, AJH.BRANCH_CODE ORDER BY AJH.JOINT_HOLDER DESC) AS rn
        FROM UBSPROD.STTM_ACC_JOINT_HOLDER@FCUBSV12 AJH
        WHERE AJH.CUST_AC_NO IN (
            SELECT CUST_AC_NO
            FROM UBSPROD.STTM_CUST_ACCOUNT@FCUBSV12
            WHERE record_stat = 'O'
              AND AUTH_STAT = 'A'
              AND CUST_NO IN (
                  SELECT CUSTOMER_NO
                  FROM UBSPROD.sttm_customer@FCUBSV12
                  WHERE record_stat = 'O'
                    AND AUTH_STAT = 'A'
              )
        )
    ) AJH_INNER
    WHERE AJH_INNER.rn = 1
)
SELECT 
    CAC.CUST_AC_NO,
    CAC.BRANCH_CODE,
    CAC.AC_DESC,
    CAC.CUST_NO,
    CAC.CCY,
    CAC.ACCOUNT_CLASS,
    CAC.AC_STAT_NO_DR,
    CAC.AC_STAT_NO_CR,
    CAC.AC_STAT_BLOCK,
    CAC.AC_STAT_STOP_PAY,
    CAC.AC_STAT_DORMANT,
    CAC.JOINT_AC_INDICATOR,
    CAC.AC_OPEN_DATE,
    CAC.AC_STMT_DAY,
    CAC.AC_STMT_CYCLE,
    CAC.ALT_AC_NO,
    CAC.CHEQUE_BOOK_FACILITY,
    CAC.ATM_FACILITY,
    CAC.PASSBOOK_FACILITY,
    CAC.AC_STMT_TYPE,
    CAC.DR_HO_LINE,
    CAC.CR_HO_LINE,
    CAC.CR_CB_LINE,
    CAC.DR_CB_LINE,
    CAC.SUBLIMIT,
    CAC.UNCOLL_FUNDS_LIMIT,
    CAC.AC_STAT_FROZEN,
    CAC.PREVIOUS_STATEMENT_DATE,
    CAC.PREVIOUS_STATEMENT_BALANCE,
    CAC.PREVIOUS_STATEMENT_NO,
    CAC.TOD_LIMIT_START_DATE,
    CAC.TOD_LIMIT_END_DATE,
    CAC.TOD_LIMIT,
    CAC.NOMINEE1,
    CAC.NOMINEE2,
    CAC.DR_GL,
    CAC.CR_GL,
    CAC.RECORD_STAT,
    CAC.AUTH_STAT,
    CAC.MOD_NO,
    CAC.MAKER_ID,
    CAC.MAKER_DT_STAMP,
    CAC.CHECKER_ID,
    CAC.CHECKER_DT_STAMP,
    CAC.ONCE_AUTH,
    CAC.LIMIT_CCY,
    CAC.LINE_ID,
    CAC.OFFLINE_LIMIT,
    CAC.CAS_ACCOUNT,
    CAC.ACY_OPENING_BAL,
    CAC.LCY_OPENING_BAL,
    CAC.ACY_TODAY_TOVER_DR,
    CAC.LCY_TODAY_TOVER_DR,
    CAC.ACY_TODAY_TOVER_CR,
    CAC.LCY_TODAY_TOVER_CR,
    CAC.ACY_TANK_CR,
    CAC.ACY_TANK_DR,
    CAC.LCY_TANK_CR,
    CAC.LCY_TANK_DR,
    CAC.ACY_TOVER_CR,
    CAC.LCY_TOVER_CR,
    CAC.ACY_TANK_UNCOLLECTED,
    CAC.ACY_CURR_BALANCE,
    CAC.LCY_CURR_BALANCE,
    CAC.ACY_BLOCKED_AMOUNT,
    CAC.ACY_AVL_BAL,
    CAC.ACY_UNAUTH_DR,
    CAC.ACY_UNAUTH_TANK_DR,
    CAC.ACY_UNAUTH_CR,
    CAC.ACY_UNAUTH_TANK_CR,
    CAC.ACY_UNAUTH_UNCOLLECTED,
    CAC.ACY_UNAUTH_TANK_UNCOLLECTED,
    Latest_CAD.DORMANCY_START_DT,
    Latest_AFC.TRNREFNO,
    Latest_AFC.TRNCODE,
    Latest_AFC.TRN_DT,
    Latest_AFC.AMOUNT,
    FUF.FUNCTION_ID,
    FUF.REC_KEY,
    Latest_ACS.STATUS_CHANGE_DATE,
    Latest_ACS.SEQ_NO,
    Latest_ACS.AC_STAT_DE_POST,
    Latest_ACS.DORMANT,
    Latest_ASIG.RECORD_STAT,
    Latest_ASIG.CIF_SIG_ID,
    Latest_AJH.JOINT_HOLDER,
    FUF.FIELD_VAL_1 ,
    FUF.FIELD_VAL_2 ,
    FUF.FIELD_VAL_3 ,
    FUF.FIELD_VAL_4 ,
    FUF.FIELD_VAL_5 ,
    FUF.FIELD_VAL_6 ,
    FUF.FIELD_VAL_7 ,
    FUF.FIELD_VAL_8 ,
    FUF.FIELD_VAL_9 ,
    FUF.FIELD_VAL_10,
    FUF.FIELD_VAL_11,
    FUF.FIELD_VAL_12,
    FUF.FIELD_VAL_13,
    FUF.FIELD_VAL_14,
    FUF.FIELD_VAL_15,
    FUF.FIELD_VAL_16,
    FUF.FIELD_VAL_17,
    FUF.FIELD_VAL_18,
    FUF.FIELD_VAL_19,
    FUF.FIELD_VAL_20,
    FUF.FIELD_VAL_21,
    FUF.FIELD_VAL_22,
    FUF.FIELD_VAL_23,
    FUF.FIELD_VAL_24,
    FUF.FIELD_VAL_25,
    FUF.FIELD_VAL_26
FROM UBSPROD.STTM_CUST_ACCOUNT@FCUBSV12 CAC
LEFT JOIN Latest_CAD
  ON CAC.CUST_AC_NO = Latest_CAD.CUST_AC_NO
 AND CAC.BRANCH_CODE = Latest_CAD.BRANCH_CODE
LEFT JOIN Latest_AFC
  ON CAC.CUST_AC_NO = Latest_AFC.ACCOUNT
 AND CAC.BRANCH_CODE = Latest_AFC.BRANCH
LEFT JOIN UBSPROD.CSTM_FUNCTION_USERDEF_FIELDS@FCUBSV12 FUF
  ON FUF.FUNCTION_ID = 'STDCUSAC'
  and fuf.rec_key= CAC.BRANCH_CODE || '~' || CAC.CUST_AC_NO || '~'    
 /*AND CAC.BRANCH_CODE = SUBSTR(FUF.REC_KEY, 1, 3)
 AND CAC.CUST_AC_NO = SUBSTR(
       FUF.REC_KEY,
       INSTR(FUF.REC_KEY, '~', 1, 1) + 1,
       ( (INSTR(FUF.REC_KEY, '~', 1, 2) - 1) - INSTR(FUF.REC_KEY, '~', 1, 1))
     )*/
LEFT JOIN Latest_ACS
  ON CAC.CUST_AC_NO = Latest_ACS.CUST_AC_NO
 AND CAC.BRANCH_CODE = Latest_ACS.BRANCH_CODE
LEFT JOIN UBSPROD.STTM_CUST_ACCOUNT_CUSTOM@FCUBSV12 ACOM
  ON CAC.CUST_AC_NO = ACOM.CUST_AC_NO
 AND CAC.BRANCH_CODE = ACOM.BRANCH_CODE
LEFT JOIN Latest_AJH
  ON CAC.CUST_AC_NO = Latest_AJH.CUST_AC_NO
 AND CAC.BRANCH_CODE = Latest_AJH.BRANCH_CODE
 AND CAC.CUST_NO = Latest_AJH.JOINT_HOLDER
LEFT JOIN Latest_ASIG
  ON CAC.CUST_AC_NO = Latest_ASIG.ACC_NO
 AND CAC.BRANCH_CODE = Latest_ASIG.BRANCH
WHERE CAC.ACCOUNT_TYPE <> 'Y'
  AND CAC.MUDARABAH_ACCOUNTS <> 'Y'
  AND CAC.BRANCH_CODE = '101'
  AND CAC.record_stat = 'O'
  AND CAC.AUTH_STAT = 'A'
  AND CAC.CUST_NO IN (
      SELECT CUSTOMER_NO
      FROM UBSPROD.sttm_customer@FCUBSV12
      WHERE record_stat = 'O'
        AND AUTH_STAT = 'A'
  )
ORDER BY CAC.CUST_AC_NO) LOOP
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
              rec.DR_HO_LINE || ',' || rec.CR_HO_LINE || ',' ||
              rec.CR_CB_LINE || ',' || rec.DR_CB_LINE || ',' || rec.SUBLIMIT || ',' ||
              rec.UNCOLL_FUNDS_LIMIT || ',' || rec.AC_STAT_FROZEN || ',' || rec.PREVIOUS_STATEMENT_DATE || ',' ||
              rec.PREVIOUS_STATEMENT_BALANCE || ',' || rec.PREVIOUS_STATEMENT_NO || ',' ||
              rec.TOD_LIMIT_START_DATE || ',' || rec.TOD_LIMIT_END_DATE || ',' ||
              rec.TOD_LIMIT || ',' || rec.NOMINEE1 || ',' ||
              rec.NOMINEE2 || ',' || rec.DR_GL || ',' ||
              rec.CR_GL || ',' || rec.RECORD_STAT || ',' ||
              rec.AUTH_STAT || ',' || rec.MOD_NO || ',' || rec.MAKER_ID || ',' ||
              rec.MAKER_DT_STAMP || ',' || rec.CHECKER_ID || ',' ||
              rec.CHECKER_DT_STAMP || ',' || rec.ONCE_AUTH || ',' ||
              rec.LIMIT_LCY || ',' || rec.LINE_ID || ',' ||
              rec.OFFLINE_LIMIT || ',' || rec.CAS_ACCOUNT || ',' ||
              rec.ACY_OPENING_BAL || ',' || rec.LCY_OPENING_BAL || ',' ||
              rec.ACY_TODAY_TOVER_DR || ',' ||  rec.LCY_TODAY_TOVER_DR  || ',' ||
              rec.ACY_TODAY_TOVER_CR || ',' ||  rec.LCY_TODAY_TOVER_CR  || ',' ||
              rec.ACY_TANK_CR || ',' || rec.ACY_TANK_DR || ',' ||
              rec.LCY_TANK_CR || ',' || rec.LCY_TANK_DR || ',' ||
              rec.ACY_TOVER_CR || ',' || rec.LCY_TOVER_CR || ',' ||
              rec.ACY_TANK_UNCOLLECTED || ',' ||
              rec.ACY_CURR_BALANCE || ',' || rec.LCY_CURR_BALANCE || ',' ||
              rec.ACY_BLOCKED_AMOUNT || ',' || rec.ACY_AVL_BAL || ',' ||
              rec.ACY_UNAUTH_DR || ',' || rec.ACY_UNAUTH_TANK_DR || ',' ||
              rec.ACY_UNAUTH_CR || ',' || rec.ACY_UNAUTH_TANK_CR || ',' ||
              rec.ACY_UNAUTH_UNCOLLECTED || ',' ||
              rec.ACY_UNAUTH_TANK_UNCOLLECTED || ',' ||
              rec.DORMANCY_START_DT || ',' || rec.TRNREFNO ||',' ||
              rec.TRNCODE || ',' || rec.TRN_DT ||',' ||
              rec.AMOUNT || ',' || rec.FUNCTION_ID ||',' ||
              rec.REC_KEY ||',' || rec.STATUS_CHANGE_DATE || ',' ||
              rec.SEQ_NO || ',' || rec.AC_STAT_DE_POST || ',' ||
              rec.DORMANT || ',' || rec.SIGNATURE_RECORD_STATUS || ',' || rec.CIF_SIG_ID || ',' ||
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
END pr_4_CASA_REPORT_v12;
/
/
