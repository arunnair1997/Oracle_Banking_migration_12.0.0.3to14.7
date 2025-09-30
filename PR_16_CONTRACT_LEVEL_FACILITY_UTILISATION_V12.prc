-- PROCEDURE PR_16_CONTRACT_LEVEL_FACILITY_UTILISATION_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_16_CONTRACT_LEVEL_FACILITY_UTILISATION_V12" (p_branch_code IN VARCHAR2,
                                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '16_contract_level_facility_utilisation' || p_branch_code ||
                '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO, MODULE, USER_REFNO, UTIL_CCY, LINE_CODE_SERIAL, TOTAL UTIL';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select util.customer_no,
                     util.module,
                     util.user_refno,
                     util.util_ccy,
                     fac.line_code,
                     fac.line_serial,
                     sum(util_amt) Util_Amt
                from getb_utils@fcubsv12    util,
                     getm_facility@fcubsv12 fac
               where util.util_stat = 'A'
                 and util.auth_stat = 'A'
                 and util.limit_type = 'F'
                 and util.limit_id = fac.id
                 and util.liab_branch = p_branch_code
               group by util.customer_no,
                        util.module,
                        util.user_refno,
                        fac.line_code,
                        fac.line_serial,
                        util.util_ccy
               order by util.customer_no,
                        util.module,
                        util.user_refno,
                        fac.line_code ||
                        fac.line_serial,
                        util.util_ccy) LOOP
    l_line := rec.customer_no || ',' || rec.module || ',' || rec.user_refno || ',' ||
              rec.util_ccy || ',' || rec.line_code || rec.line_serial || ','|| rec.Util_Amt;
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
END pr_16_contract_level_facility_utilisation_v12;
/
/
