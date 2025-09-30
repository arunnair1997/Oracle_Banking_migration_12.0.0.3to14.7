-- PROCEDURE PR_13_LIAB_LEVEL_ACTIVE_UTILISATION_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_13_LIAB_LEVEL_ACTIVE_UTILISATION_V14" (p_branch_code IN VARCHAR2,p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '13_Liab_level_active_utilisation_' || p_branch_code ||
                '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO, LIAB_NO, UTIL_CCY, UTILISATION, OVERALL_LIMIT, LIAB UTIL';
  UTL_FILE.PUT_LINE(l_file, l_line);
dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select util.customer_no,
                     liab.liab_no,
                     util.util_ccy,
                     sum(util.util_amt) as sumutil,
                     liab.OVERALL_LIMIT,
                     liab.UTIL_AMT
                from integratedpp.gezb_utils     util,
                     integratedpp.gezm_liab      liab,
                     integratedpp.gezm_liab_cust liabcust
               where util.util_stat = 'A'
                 and util.auth_stat = 'A'
                 and liabcust.CUSTOMER_NO = util.customer_no
                 and util.liab_id = liabcust.liab_id
                 and liab.id = liabcust.liab_id
                 and liab.id = util.liab_id
                 and util.LIAB_BRANCH = p_branch_code
                 and liab.record_stat='O' 
                 and liab.auth_stat='A'
               group by util.customer_no,
                        liab.liab_no,
                        util.util_ccy,
                        liab.OVERALL_LIMIT,
                        liab.UTIL_AMT
               order by util.customer_no,
                        liab.liab_no,
                        util.util_ccy,
                        liab.OVERALL_LIMIT,
                        liab.UTIL_AMT) LOOP
    l_line := rec.customer_no|| ',' ||rec.liab_no|| ',' || rec.util_ccy|| ',' || rec.sumutil|| ',' ||rec.OVERALL_LIMIT|| ',' ||rec.UTIL_AMT ;
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
END pr_13_liab_level_active_utilisation_v14;
/
/
