-- PROCEDURE PR_48_OPEN_INSTRUMENTS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_48_OPEN_INSTRUMENTS_V12" (p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '48_open_instruments_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'INTERNAL_SEC_ID, ISIN_IDENTIFIER, SECURITY_DESCRIPTION, PRODUCT, SECURITY_TYPE, ORDINARY_OR_PREFERRED, INTEREST_RATE_TYPE, SECURITY_REF_NO, ISSUER_ID, LOCAL_MARKET_OF_ISSUE, REGISTERED_OR_BEARER, FORM_TYPE, QUANTITY_QUOTATION_METHOD, PRICE_QUOTATION_METHOD, INTEREST_QUOTATION_METHOD, FRACTIONAL_QUANTITY_ALLOWED, UNIT_DECIMALS, MINIMUM_TRADABLE_QUANTITY, RENOUNCABLE, REDEEMABLE, CALL_OPTION, PUT_OPTION, REDEMPTION_TYPE, SECURITY_CCY, ISSUER_PAYMENT_CCY, ISSUE_OR_TEAROFF_DATE, START_OF_TRADING_DATE, START_OF_INTEREST_DATE, REDEMPTION_OR_EXPIRY_DATE, INITIAL_FACE_VALUE, ISSUE_PRICE, REDEMPTION_PRICE, ISSUER_SECURITY_ID, MARKET_SECURITY_ID, CENTRAL_BANK_SECURITY_ID, IDENTIFICATION_NARRATIVE_01, IDENTIFICATION_NARRATIVE_02, IDENTIFICATION_NARRATIVE_03, IDENTIFICATION_NARRATIVE_04, RESTRICTED, VOTING_RIGHTS, COVERED, CONVERTIBLE, ISO_CFI_CODE, ISITC_CODE, ATTRIBUTE_DESCRIPTION, ATTRIBUTE_NARRATIVE_01, ATTRIBUTE_NARRATIVE_02, ATTRIBUTE_NARRATIVE_03, ATTRIBUTE_NARRATIVE_04, ATTRIBUTE_NARRATIVE_05, ATTRIBUTE_NARRATIVE_06, ATTRIBUTE_NARRATIVE_07, ATTRIBUTE_NARRATIVE_08, ATTRIBUTE_NARRATIVE_09, ATTRIBUTE_NARRATIVE_10, COLLATERAL_TYPE, DIRECT_EXPOSURE_LINE, REVALUATION_PRICE_CODE, COUPON_BASIS, DIVIDEND_BASIS, WARRANT_BASIS, RIGHTS_BASIS, AUTO_INIT_OF_CORPORATE_ACTION, BULLET_REDEMPTION_ACTION_ID, EXPIRY_ACTION_ID, LOT_SIZE, ORIGINAL_ISSUE_SIZE, OUTSTANDING_ISSUE_SIZE, QUALITY_RATING, EQUATED_REDEM, INDEX_FLAG, CAPITALIZATION, TBD_WHTACCR_FLAG, TBD_WHTACCR_RATECODE, REINV_PERIOD, ANNUALIZING_METHOD, BOND_DAY_COUNT_NR, BOND_DAY_COUNT_DR, TBILL_DAY_COUNT_NR, TBILL_DAY_COUNT_DR, CPN_DT_PLOT_HOL_TREATMENT, CPN_ACCT_VAL_DT_HOL_TREATMENT, OLD_REDEMPTION_DATE, USE_BOND_FORMULA, TBILL_REINV_PERIOD, TBILL_ANNUALIZING_METHOD, MARKET_FOR_REVAL, BANKERS_ACCEPTANCE, PREM_DISC_CURR_PERIOD, DAYS, DAY_IND, MINIMUM_RATE, MAXIMUM_RATE, SPREAD, FOREIGN_SECURITY, SOURCE_CODE, SUBSYSTEMSTAT, REDEMPTION_QUOTATION, CONFIRM_CORP_ACTION';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select internal_sec_id,
                     isin_identifier,
                     security_description,
                     product,
                     security_type,
                     ordinary_or_preferred,
                     interest_rate_type,
                     security_ref_no,
                     issuer_id,
                     local_market_of_issue,
                     registered_or_bearer,
                     form_type,
                     quantity_quotation_method,
                     price_quotation_method,
                     interest_quotation_method,
                     fractional_quantity_allowed,
                     unit_decimals,
                     minimum_tradable_quantity,
                     renouncable,
                     redeemable,
                     call_option,
                     put_option,
                     redemption_type,
                     security_ccy,
                     issuer_payment_ccy,
                     issue_or_tearoff_date,
                     start_of_trading_date,
                     start_of_interest_date,
                     redemption_or_expiry_date,
                     initial_face_value,
                     issue_price,
                     redemption_price,
                     issuer_security_id,
                     market_security_id,
                     central_bank_security_id,
                     identification_narrative_01,
                     identification_narrative_02,
                     identification_narrative_03,
                     identification_narrative_04,
                     restricted,
                     voting_rights,
                     covered,
                     convertible,
                     iso_cfi_code,
                     isitc_code,
                     attribute_description,
                     attribute_narrative_01,
                     attribute_narrative_02,
                     attribute_narrative_03,
                     attribute_narrative_04,
                     attribute_narrative_05,
                     attribute_narrative_06,
                     attribute_narrative_07,
                     attribute_narrative_08,
                     attribute_narrative_09,
                     attribute_narrative_10,
                     collateral_type,
                     direct_exposure_line,
                     revaluation_price_code,
                     coupon_basis,
                     dividend_basis,
                     warrant_basis,
                     rights_basis,
                     auto_init_of_corporate_action,
                     bullet_redemption_action_id,
                     expiry_action_id,
                     lot_size,
                     original_issue_size,
                     outstanding_issue_size,
                     quality_rating,
                     equated_redem,
                     index_flag,
                     capitalization,
                     tbd_whtaccr_flag,
                     tbd_whtaccr_ratecode,
                     reinv_period,
                     annualizing_method,
                     bond_day_count_nr,
                     bond_day_count_dr,
                     tbill_day_count_nr,
                     tbill_day_count_dr,
                     cpn_dt_plot_hol_treatment,
                     cpn_acct_val_dt_hol_treatment,
                     old_redemption_date,
                     use_bond_formula,
                     tbill_reinv_period,
                     tbill_annualizing_method,
                     market_for_reval,
                     bankers_acceptance,
                     prem_disc_curr_period,
                     days,
                     day_ind,
                     minimum_rate,
                     maximum_rate,
                     spread,
                     foreign_security,
                     source_code,
                     subsystemstat,
                     redemption_quotation,
                     confirm_corp_action
                from ubsprod.SETM_SECURITY_MASTER@fcubsv12
               where record_stat = 'O'
                 and Auth_stat = 'A') LOOP
    l_line := rec.INTERNAL_SEC_ID || ',' || rec.ISIN_IDENTIFIER || ',' ||
              rec.SECURITY_DESCRIPTION || ',' || rec.PRODUCT || ',' ||
              rec.SECURITY_TYPE || ',' || rec.ORDINARY_OR_PREFERRED || ',' ||
              rec.INTEREST_RATE_TYPE || ',' || rec.SECURITY_REF_NO || ',' ||
              rec.ISSUER_ID || ',' || rec.LOCAL_MARKET_OF_ISSUE || ',' ||
              rec.REGISTERED_OR_BEARER || ',' || rec.FORM_TYPE || ',' ||
              rec.QUANTITY_QUOTATION_METHOD || ',' ||
              rec.PRICE_QUOTATION_METHOD || ',' ||
              rec.INTEREST_QUOTATION_METHOD || ',' ||
              rec.FRACTIONAL_QUANTITY_ALLOWED || ',' || rec.UNIT_DECIMALS || ',' ||
              rec.MINIMUM_TRADABLE_QUANTITY || ',' || rec.RENOUNCABLE || ',' ||
              rec.REDEEMABLE || ',' || rec.CALL_OPTION || ',' ||
              rec.PUT_OPTION || ',' || rec.REDEMPTION_TYPE || ',' ||
              rec.SECURITY_CCY || ',' || rec.ISSUER_PAYMENT_CCY || ',' ||
              rec.ISSUE_OR_TEAROFF_DATE || ',' || rec.START_OF_TRADING_DATE || ',' ||
              rec.START_OF_INTEREST_DATE || ',' ||
              rec.REDEMPTION_OR_EXPIRY_DATE || ',' ||
              rec.INITIAL_FACE_VALUE || ',' || rec.ISSUE_PRICE || ',' ||
              rec.REDEMPTION_PRICE || ',' || rec.ISSUER_SECURITY_ID || ',' ||
              rec.MARKET_SECURITY_ID || ',' || rec.CENTRAL_BANK_SECURITY_ID || ',' ||
              rec.IDENTIFICATION_NARRATIVE_01 || ',' ||
              rec.IDENTIFICATION_NARRATIVE_02 || ',' ||
              rec.IDENTIFICATION_NARRATIVE_03 || ',' ||
              rec.IDENTIFICATION_NARRATIVE_04 || ',' || rec.RESTRICTED || ',' ||
              rec.VOTING_RIGHTS || ',' || rec.COVERED || ',' ||
              rec.CONVERTIBLE || ',' || rec.ISO_CFI_CODE || ',' ||
              rec.ISITC_CODE || ',' || rec.ATTRIBUTE_DESCRIPTION || ',' ||
              rec.ATTRIBUTE_NARRATIVE_01 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_02 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_03 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_04 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_05 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_06 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_07 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_08 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_09 || ',' ||
              rec.ATTRIBUTE_NARRATIVE_10 || ',' || rec.COLLATERAL_TYPE || ',' ||
              rec.DIRECT_EXPOSURE_LINE || ',' || rec.REVALUATION_PRICE_CODE || ',' ||
              rec.COUPON_BASIS || ',' || rec.DIVIDEND_BASIS || ',' ||
              rec.WARRANT_BASIS || ',' || rec.RIGHTS_BASIS || ',' ||
              rec.AUTO_INIT_OF_CORPORATE_ACTION || ',' ||
              rec.BULLET_REDEMPTION_ACTION_ID || ',' ||
              rec.EXPIRY_ACTION_ID || ',' || rec.LOT_SIZE || ',' ||
              rec.ORIGINAL_ISSUE_SIZE || ',' || rec.OUTSTANDING_ISSUE_SIZE || ',' ||
              rec.QUALITY_RATING || ',' || rec.EQUATED_REDEM || ',' ||
              rec.INDEX_FLAG || ',' || rec.CAPITALIZATION || ',' ||
              rec.TBD_WHTACCR_FLAG || ',' || rec.TBD_WHTACCR_RATECODE || ',' ||
              rec.REINV_PERIOD || ',' || rec.ANNUALIZING_METHOD || ',' ||
              rec.BOND_DAY_COUNT_NR || ',' || rec.BOND_DAY_COUNT_DR || ',' ||
              rec.TBILL_DAY_COUNT_NR || ',' || rec.TBILL_DAY_COUNT_DR || ',' ||
              rec.CPN_DT_PLOT_HOL_TREATMENT || ',' ||
              rec.CPN_ACCT_VAL_DT_HOL_TREATMENT || ',' ||
              rec.OLD_REDEMPTION_DATE || ',' || rec.USE_BOND_FORMULA || ',' ||
              rec.TBILL_REINV_PERIOD || ',' || rec.TBILL_ANNUALIZING_METHOD || ',' ||
              rec.MARKET_FOR_REVAL || ',' || rec.BANKERS_ACCEPTANCE || ',' ||
              rec.PREM_DISC_CURR_PERIOD || ',' || rec.DAYS || ',' ||
              rec.DAY_IND || ',' || rec.MINIMUM_RATE || ',' ||
              rec.MAXIMUM_RATE || ',' || rec.SPREAD || ',' ||
              rec.FOREIGN_SECURITY || ',' || rec.SOURCE_CODE || ',' ||
              rec.SUBSYSTEMSTAT || ',' || rec.REDEMPTION_QUOTATION || ',' ||
              rec.CONFIRM_CORP_ACTION;
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
END pr_48_open_instruments_v12;
/
/
