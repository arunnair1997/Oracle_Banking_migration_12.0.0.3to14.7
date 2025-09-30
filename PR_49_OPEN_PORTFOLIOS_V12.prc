-- PROCEDURE PR_49_OPEN_PORTFOLIOS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_49_OPEN_PORTFOLIOS_V12" (
                                                      p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '49_Open_Portfolios_V12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PORTFOLIO_PRODUCT_CODE, PORTFOLIO_REF_NO, PORTFOLIO_ID, PORTFOLIO_DESCRIPTION, PORTFOLIO_CUSTOMER_ID, CUSTOMER_ACCOUNT_BRANCH, CUSTOMER_ACCOUNT, PORTFOLIO_LOCAL_BRANCH, PORTFOLIO_CCY_CODE, PORTFOLIO_TYPE, PORTFOLIO_COSTING_METHOD, ACCRUE_PREMIUM_FLAG, ACCRUE_DISCOUNT_FLAG, DPR_ACCRUAL_FREQUENCY, DPR_ACCRUAL_METHOD, DPR_ACCRUAL_START_DAY, DPR_ACCRUAL_START_MONTH, BOOK_DP_METHOD, ACCRUE_REDMPREM_FLAG, RPR_ACCRUAL_FREQUENCY, RPR_ACCRUAL_METHOD, RPR_ACCRUAL_START_DAY, RPR_ACCRUAL_START_MONTH, FWDPNL_ACCRUAL_FLAG, FWDPNL_ACCRUAL_FREQUENCY, FWDPNL_ACCRUAL_METHOD, FWDPNL_ACCRUAL_START_DAY, FWDPNL_ACCRUAL_START_MONTH, INTEREST_ACCRUAL_FLAG, INTEREST_ACCRUAL_FREQUENCY, INTEREST_ACCRUAL_START_DAY, INTEREST_ACCRUAL_START_MONTH, REVALUATION_FLAG, REVALUATION_METHOD, REVALUATION_FREQUENCY, REVALUATION_START_DAY, REVALUATION_START_MONTH, UNSETTLED_DEALREVAL_FLAG, SHORT_POSITION_ALLOWED_FLAG, CORPUS_ACCOUNT_FLAG, ACCRUE_WITHHOLDING_TAX_FLAG, STATEMENT_HOLDINGS_REQDFLAG, STATEMENT_HOLDINGS_FREQUENCY, STATEMENT_HOLDINGS_START_DAY, STATEMENT_HOLDINGS_START_MONTH, STATEMENT_TXNS_REQDFLAG, STATEMENT_TXNS_FREQUENCY, STATEMENT_TXNS_START_DAY, STATEMENT_TXNS_START_MONTH, PREVIOUS_STMNTHOLDING_NUMBER, PREVIOUS_STMNTHOLDING_DATE, NEXT_STMNTHOLDING_DATE, PREVIOUS_STMNTTXNS_NUMBER, PREVIOUS_STMNTTXNS_DATE, NEXT_STMNTTXNS_DATE, CORPACTION_NOTICE_FLAG, CORPACTION_NOTICE_DAYS, CORPACTION_NOTICE_DAYSTYPE, CORPACTION_AUTOLIQ_FLAG, SECURITIES_ALLOWED_FLAG, BOOK_RIGHTSIV_FLAG, BOOK_WARRANTSIV_FLAG, TBD_ACCRUAL_FLAG, TBD_ACCRUAL_FREQUENCY, TBD_ACCRUAL_START_DAY, TBD_ACCRUAL_START_MONTH, LIMITS_TRACKING_FOR_BONDS, LIMITS_TRACKING_FOR_EQUITIES, LIMITS_TRACKING_FOR_RIGHTS, LIMITS_TRACKING_FOR_WARRANTS, LIMITS_TRACKING_FOR_TBILLS, MAKER_ID, MAKER_DT_STAMP, CHECKER_ID, CHECKER_DT_STAMP, CONTRACT_STATUS, AUTH_STAT, RECORD_STAT, ONCE_AUTH, MOD_NO, TEMPLATE_STATUS, PREVIOUS_DISCPREM_ACCRUAL_DATE, PREVIOUS_REDMPREM_ACCRUAL_DATE, PREVIOUS_ZCBDISC_ACCRUAL_DATE, CORPORATE_ACTION_REQD, TRACK_ACCRUED_INTEREST, REALIZED_REVAL_FLAG, REALIZED_REVAL_FREQUENCY, REALIZED_REVAL_METHOD, REALIZED_REVAL_START_DAY, REALIZED_REVAL_START_MONTH, ACCRUE_TBD_WHT_FLAG, ACCRUE_PREMIUM_METHOD, ACCRUE_DISCOUNT_METHOD, REVAL_LEVEL, LOCOM_BASIS, EXCH_RATE_TYPE, BANKERS_ACCEPTANCE, CONTRA_HOLDING_TYPE, CONTRA_HOLDING_ALLOWED, SUBSYSTEMSTAT, FUND_ID, REPO_TODAYS_PROJECTED_HOLDING';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select PORTFOLIO_PRODUCT_CODE,
                     PORTFOLIO_REF_NO,
                     PORTFOLIO_ID,
                     PORTFOLIO_DESCRIPTION,
                     PORTFOLIO_CUSTOMER_ID,
                     CUSTOMER_ACCOUNT_BRANCH,
                     CUSTOMER_ACCOUNT,
                     PORTFOLIO_LOCAL_BRANCH,
                     PORTFOLIO_CCY_CODE,
                     PORTFOLIO_TYPE,
                     PORTFOLIO_COSTING_METHOD,
                     ACCRUE_PREMIUM_FLAG,
                     ACCRUE_DISCOUNT_FLAG,
                     DPR_ACCRUAL_FREQUENCY,
                     DPR_ACCRUAL_METHOD,
                     DPR_ACCRUAL_START_DAY,
                     DPR_ACCRUAL_START_MONTH,
                     BOOK_DP_METHOD,
                     ACCRUE_REDMPREM_FLAG,
                     RPR_ACCRUAL_FREQUENCY,
                     RPR_ACCRUAL_METHOD,
                     RPR_ACCRUAL_START_DAY,
                     RPR_ACCRUAL_START_MONTH,
                     FWDPNL_ACCRUAL_FLAG,
                     FWDPNL_ACCRUAL_FREQUENCY,
                     FWDPNL_ACCRUAL_METHOD,
                     FWDPNL_ACCRUAL_START_DAY,
                     FWDPNL_ACCRUAL_START_MONTH,
                     INTEREST_ACCRUAL_FLAG,
                     INTEREST_ACCRUAL_FREQUENCY,
                     INTEREST_ACCRUAL_START_DAY,
                     INTEREST_ACCRUAL_START_MONTH,
                     REVALUATION_FLAG,
                     REVALUATION_METHOD,
                     REVALUATION_FREQUENCY,
                     REVALUATION_START_DAY,
                     REVALUATION_START_MONTH,
                     UNSETTLED_DEALREVAL_FLAG,
                     SHORT_POSITION_ALLOWED_FLAG,
                     CORPUS_ACCOUNT_FLAG,
                     ACCRUE_WITHHOLDING_TAX_FLAG,
                     STATEMENT_HOLDINGS_REQDFLAG,
                     STATEMENT_HOLDINGS_FREQUENCY,
                     STATEMENT_HOLDINGS_START_DAY,
                     STATEMENT_HOLDINGS_START_MONTH,
                     STATEMENT_TXNS_REQDFLAG,
                     STATEMENT_TXNS_FREQUENCY,
                     STATEMENT_TXNS_START_DAY,
                     STATEMENT_TXNS_START_MONTH,
                     PREVIOUS_STMNTHOLDING_NUMBER,
                     PREVIOUS_STMNTHOLDING_DATE,
                     NEXT_STMNTHOLDING_DATE,
                     PREVIOUS_STMNTTXNS_NUMBER,
                     PREVIOUS_STMNTTXNS_DATE,
                     NEXT_STMNTTXNS_DATE,
                     CORPACTION_NOTICE_FLAG,
                     CORPACTION_NOTICE_DAYS,
                     CORPACTION_NOTICE_DAYSTYPE,
                     CORPACTION_AUTOLIQ_FLAG,
                     SECURITIES_ALLOWED_FLAG,
                     BOOK_RIGHTSIV_FLAG,
                     BOOK_WARRANTSIV_FLAG,
                     TBD_ACCRUAL_FLAG,
                     TBD_ACCRUAL_FREQUENCY,
                     TBD_ACCRUAL_START_DAY,
                     TBD_ACCRUAL_START_MONTH,
                     LIMITS_TRACKING_FOR_BONDS,
                     LIMITS_TRACKING_FOR_EQUITIES,
                     LIMITS_TRACKING_FOR_RIGHTS,
                     LIMITS_TRACKING_FOR_WARRANTS,
                     LIMITS_TRACKING_FOR_TBILLS,
                     MAKER_ID,
                     MAKER_DT_STAMP,
                     CHECKER_ID,
                     CHECKER_DT_STAMP,
                     CONTRACT_STATUS,
                     AUTH_STAT,
                     RECORD_STAT,
                     ONCE_AUTH,
                     MOD_NO,
                     TEMPLATE_STATUS,
                     PREVIOUS_DISCPREM_ACCRUAL_DATE,
                     PREVIOUS_REDMPREM_ACCRUAL_DATE,
                     PREVIOUS_ZCBDISC_ACCRUAL_DATE,
                     CORPORATE_ACTION_REQD,
                     TRACK_ACCRUED_INTEREST,
                     REALIZED_REVAL_FLAG,
                     REALIZED_REVAL_FREQUENCY,
                     REALIZED_REVAL_METHOD,
                     REALIZED_REVAL_START_DAY,
                     REALIZED_REVAL_START_MONTH,
                     ACCRUE_TBD_WHT_FLAG,
                     ACCRUE_PREMIUM_METHOD,
                     ACCRUE_DISCOUNT_METHOD,
                     REVAL_LEVEL,
                     LOCOM_BASIS,
                     EXCH_RATE_TYPE,
                     BANKERS_ACCEPTANCE,
                     CONTRA_HOLDING_TYPE,
                     CONTRA_HOLDING_ALLOWED,
                     SUBSYSTEMSTAT,
                     FUND_ID,
                     REPO_TODAYS_PROJECTED_HOLDING
                from ubsprod.SETM_PORTFOLIO_MASTER@fcubsv12
               where record_stat = 'O'
                 and auth_stat = 'A') LOOP
    l_line := rec.PORTFOLIO_PRODUCT_CODE || ',' || rec.PORTFOLIO_REF_NO || ',' ||
              rec.PORTFOLIO_ID || ',' || rec.PORTFOLIO_DESCRIPTION || ',' ||
              rec.PORTFOLIO_CUSTOMER_ID || ',' ||
              rec.CUSTOMER_ACCOUNT_BRANCH || ',' || rec.CUSTOMER_ACCOUNT || ',' ||
              rec.PORTFOLIO_LOCAL_BRANCH || ',' || rec.PORTFOLIO_CCY_CODE || ',' ||
              rec.PORTFOLIO_TYPE || ',' || rec.PORTFOLIO_COSTING_METHOD || ',' ||
              rec.ACCRUE_PREMIUM_FLAG || ',' || rec.ACCRUE_DISCOUNT_FLAG || ',' ||
              rec.DPR_ACCRUAL_FREQUENCY || ',' || rec.DPR_ACCRUAL_METHOD || ',' ||
              rec.DPR_ACCRUAL_START_DAY || ',' ||
              rec.DPR_ACCRUAL_START_MONTH || ',' || rec.BOOK_DP_METHOD || ',' ||
              rec.ACCRUE_REDMPREM_FLAG || ',' || rec.RPR_ACCRUAL_FREQUENCY || ',' ||
              rec.RPR_ACCRUAL_METHOD || ',' || rec.RPR_ACCRUAL_START_DAY || ',' ||
              rec.RPR_ACCRUAL_START_MONTH || ',' || rec.FWDPNL_ACCRUAL_FLAG || ',' ||
              rec.FWDPNL_ACCRUAL_FREQUENCY || ',' ||
              rec.FWDPNL_ACCRUAL_METHOD || ',' ||
              rec.FWDPNL_ACCRUAL_START_DAY || ',' ||
              rec.FWDPNL_ACCRUAL_START_MONTH || ',' ||
              rec.INTEREST_ACCRUAL_FLAG || ',' ||
              rec.INTEREST_ACCRUAL_FREQUENCY || ',' ||
              rec.INTEREST_ACCRUAL_START_DAY || ',' ||
              rec.INTEREST_ACCRUAL_START_MONTH || ',' ||
              rec.REVALUATION_FLAG || ',' || rec.REVALUATION_METHOD || ',' ||
              rec.REVALUATION_FREQUENCY || ',' || rec.REVALUATION_START_DAY || ',' ||
              rec.REVALUATION_START_MONTH || ',' ||
              rec.UNSETTLED_DEALREVAL_FLAG || ',' ||
              rec.SHORT_POSITION_ALLOWED_FLAG || ',' ||
              rec.CORPUS_ACCOUNT_FLAG || ',' ||
              rec.ACCRUE_WITHHOLDING_TAX_FLAG || ',' ||
              rec.STATEMENT_HOLDINGS_REQDFLAG || ',' ||
              rec.STATEMENT_HOLDINGS_FREQUENCY || ',' ||
              rec.STATEMENT_HOLDINGS_START_DAY || ',' ||
              rec.STATEMENT_HOLDINGS_START_MONTH || ',' ||
              rec.STATEMENT_TXNS_REQDFLAG || ',' ||
              rec.STATEMENT_TXNS_FREQUENCY || ',' ||
              rec.STATEMENT_TXNS_START_DAY || ',' ||
              rec.STATEMENT_TXNS_START_MONTH || ',' ||
              rec.PREVIOUS_STMNTHOLDING_NUMBER || ',' ||
              rec.PREVIOUS_STMNTHOLDING_DATE || ',' ||
              rec.NEXT_STMNTHOLDING_DATE || ',' ||
              rec.PREVIOUS_STMNTTXNS_NUMBER || ',' ||
              rec.PREVIOUS_STMNTTXNS_DATE || ',' || rec.NEXT_STMNTTXNS_DATE || ',' ||
              rec.CORPACTION_NOTICE_FLAG || ',' ||
              rec.CORPACTION_NOTICE_DAYS || ',' ||
              rec.CORPACTION_NOTICE_DAYSTYPE || ',' ||
              rec.CORPACTION_AUTOLIQ_FLAG || ',' ||
              rec.SECURITIES_ALLOWED_FLAG || ',' || rec.BOOK_RIGHTSIV_FLAG || ',' ||
              rec.BOOK_WARRANTSIV_FLAG || ',' || rec.TBD_ACCRUAL_FLAG || ',' ||
              rec.TBD_ACCRUAL_FREQUENCY || ',' || rec.TBD_ACCRUAL_START_DAY || ',' ||
              rec.TBD_ACCRUAL_START_MONTH || ',' ||
              rec.LIMITS_TRACKING_FOR_BONDS || ',' ||
              rec.LIMITS_TRACKING_FOR_EQUITIES || ',' ||
              rec.LIMITS_TRACKING_FOR_RIGHTS || ',' ||
              rec.LIMITS_TRACKING_FOR_WARRANTS || ',' ||
              rec.LIMITS_TRACKING_FOR_TBILLS || ',' || rec.MAKER_ID || ',' ||
              rec.MAKER_DT_STAMP || ',' || rec.CHECKER_ID || ',' ||
              rec.CHECKER_DT_STAMP || ',' || rec.CONTRACT_STATUS || ',' ||
              rec.AUTH_STAT || ',' || rec.RECORD_STAT || ',' ||
              rec.ONCE_AUTH || ',' || rec.MOD_NO || ',' ||
              rec.TEMPLATE_STATUS || ',' ||
              rec.PREVIOUS_DISCPREM_ACCRUAL_DATE || ',' ||
              rec.PREVIOUS_REDMPREM_ACCRUAL_DATE || ',' ||
              rec.PREVIOUS_ZCBDISC_ACCRUAL_DATE || ',' ||
              rec.CORPORATE_ACTION_REQD || ',' ||
              rec.TRACK_ACCRUED_INTEREST || ',' || rec.REALIZED_REVAL_FLAG || ',' ||
              rec.REALIZED_REVAL_FREQUENCY || ',' ||
              rec.REALIZED_REVAL_METHOD || ',' ||
              rec.REALIZED_REVAL_START_DAY || ',' ||
              rec.REALIZED_REVAL_START_MONTH || ',' ||
              rec.ACCRUE_TBD_WHT_FLAG || ',' || rec.ACCRUE_PREMIUM_METHOD || ',' ||
              rec.ACCRUE_DISCOUNT_METHOD || ',' || rec.REVAL_LEVEL || ',' ||
              rec.LOCOM_BASIS || ',' || rec.EXCH_RATE_TYPE || ',' ||
              rec.BANKERS_ACCEPTANCE || ',' || rec.CONTRA_HOLDING_TYPE || ',' ||
              rec.CONTRA_HOLDING_ALLOWED || ',' || rec.SUBSYSTEMSTAT || ',' ||
              rec.FUND_ID || ',' || rec.REPO_TODAYS_PROJECTED_HOLDING;
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
END PR_49_Open_Portfolios_V12;
/
/
