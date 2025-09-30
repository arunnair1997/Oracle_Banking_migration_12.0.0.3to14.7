-- PROCEDURE PR_50_SECURITIES_BALANCES_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_50_SECURITIES_BALANCES_V14" (p_dir IN VARCHAR2) IS

  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '50_Securities_balances_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'BRANCH_CODE, PORTFOLIO_ID, PORTFOLIO_DESCRIPTION, POSITION_REF_NO, PORTFOLIO_CUSTOMER_ID, CUSTOMER_NAME1, SECURITY_ID, SECURITY_DESCRIPTION, SCY, SK_LOCATION_ID, SK_LOCATION_DESC, SK_LOCATION_ACCOUNT, SECURITY_FORM_CODE, CURRENT_POSITION, CURRENT_HOLDING, OPENING_POSITION, OPENING_HOLDING, PORTFOLIO_TYPE, CURRENT_BALANCE_BLOCKED, AVAIL_HOLDING';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT DISTINCT a.BRANCH_CODE,
                              a.PORTFOLIO_ID,
                              b.PORTFOLIO_DESCRIPTION,
                              a.POSITION_REF_NO,
                              b.PORTFOLIO_CUSTOMER_ID,
                              d.CUSTOMER_NAME1,
                              a.SECURITY_ID,
                              c.SECURITY_DESCRIPTION,
                              g.SCY,
                              a.SK_LOCATION_ID,
                              f.SK_LOCATION_DESC,
                              a.SK_LOCATION_ACCOUNT,
                              a.SECURITY_FORM_CODE,
                              a.CURRENT_POSITION,
                              a.CURRENT_HOLDING,
                              a.OPENING_POSITION,
                              a.OPENING_HOLDING,
                              b.PORTFOLIO_TYPE,
                              nvl(a.CURRENT_BALANCE_BLOCKED, 0) "CURRENT_BALANCE_BLOCKED",
                              g.avail_holding
                FROM integratedpp.SEZB_PFOLIO_SKACBALANCES        a,
                     integratedpp.SEZM_PORTFOLIO_MASTER           b,
                     integratedpp.SEZM_SECURITY_MASTER            c,
                     arunn_admin.TRVW_CUSTOMER_v14                d,
                     integratedpp.SEZM_SK_LOCATION                f,
                     ARUNN_ADMIN.SEVW_PFOLIO_BALANCES_SUMMARY_v14 g
               WHERE b.PORTFOLIO_ID = a.PORTFOLIO_ID
                 AND g.PORTFOLIO_ID = a.PORTFOLIO_ID
                 AND g.SECURITY_ID = a.SECURITY_ID
                 AND c.INTERNAL_SEC_ID = a.SECURITY_ID
                 AND d.CUSTOMER_NO(+) = b.PORTFOLIO_CUSTOMER_ID
                 AND f.SK_LOCATION_ID = a.SK_LOCATION_ID
                 AND g.SKL = a.SK_LOCATION_ID
                 AnD b.record_stat = 'O'
                 and b.auth_stat = 'A') LOOP
    l_line := rec.BRANCH_CODE || ',' || rec.PORTFOLIO_ID || ',' ||
              rec.PORTFOLIO_DESCRIPTION || ',' || rec.POSITION_REF_NO || ',' ||
              rec.PORTFOLIO_CUSTOMER_ID || ',' || rec.CUSTOMER_NAME1 || ',' ||
              rec.SECURITY_ID || ',' || rec.SECURITY_DESCRIPTION || ',' ||
              rec.SCY || ',' || rec.SK_LOCATION_ID || ',' ||
              rec.SK_LOCATION_DESC || ',' || rec.SK_LOCATION_ACCOUNT || ',' ||
              rec.SECURITY_FORM_CODE || ',' || rec.CURRENT_POSITION || ',' ||
              rec.CURRENT_HOLDING || ',' || rec.OPENING_POSITION || ',' ||
              rec.OPENING_HOLDING || ',' || rec.PORTFOLIO_TYPE || ',' ||
              rec.CURRENT_BALANCE_BLOCKED || ',' || rec.AVAIL_HOLDING;
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
END pr_50_Securities_balances_v14;
/
/
