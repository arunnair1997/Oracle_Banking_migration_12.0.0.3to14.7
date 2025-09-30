-- PROCEDURE PR_18_CONTRACT_LEVEL_POOL_UTILISATION_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_18_CONTRACT_LEVEL_POOL_UTILISATION_V14" (p_branch_code IN VARCHAR2,
                                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '18_contract_level_pool_utilisation' || p_branch_code ||
                '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO, MODULE, USER_REFNO, UTIL_CCY,  POOL_CODE, TOTAL UTIL ';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select util.customer_no,
       util.module,
       util.user_refno,
       util.util_ccy,
       gp.pool_code,
       sum(util_amt) sumutil
  from integratedpp.gezb_utils util, integratedpp.gczm_pool gp
 where util.util_stat = 'A'
   and util.auth_stat = 'A'
   and util.limit_type = 'P'
   and util.limit_id = gp.id
   and util.liab_branch = p_branch_code
 group by util.customer_no,
          util.module,
          util.user_refno,
          util.util_ccy,
          gp.pool_code
 order by util.customer_no,
          util.module,
          util.user_refno,
          util.util_ccy,
          gp.pool_code
) LOOP
    l_line := rec.customer_no || ',' || rec.module || ',' || rec.user_refno || ',' ||
              rec.util_ccy || ',' || rec.pool_code || rec.sumutil;
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
END pr_18_contract_level_pool_utilisation_v14;
/
/
