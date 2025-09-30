-- PROCEDURE PR_15_CONTRACT_LEVEL_ACTIVE_UTILISATION_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_15_CONTRACT_LEVEL_ACTIVE_UTILISATION_V14" (p_branch_code IN VARCHAR2,
                                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '15_Contract_level_active_utilisation' || p_branch_code ||
                '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO,UTIL TYPE, MODULE, USER_REFNO, UTIL CCY, TOTAL UTIL ';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select customer_no,
                     decode(limit_type,
                            'L',
                            'Liab',
                            'F',
                            'Facility',
                            'C',
                            'Collateral',
                            'P',
                            'Pool') utiltype,
                     module,
                     user_refno,
                     util_ccy,
                     sum(util_amt) as sumutil
                from integratedpp.gezb_utils
               where util_stat = 'A'
                 and auth_stat = 'A'
                 and liab_branch = p_branch_code
               group by customer_no,
                        limit_type,
                        module,
                        user_refno,
                        util_ccy
               order by customer_no,
                        limit_type,
                        module,
                        user_refno,
                        util_ccy) LOOP
    l_line := rec.customer_no || ',' || rec.utiltype || ',' || rec.module || ',' ||
              rec.USER_REFNO || ',' || rec.util_ccy || ',' ||
              rec.sumutil;
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
END pr_15_Contract_level_active_utilisation_v14;
/
/
