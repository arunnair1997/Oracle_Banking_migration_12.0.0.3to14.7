-- PROCEDURE PR_20_POOL_DETAILS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_20_POOL_DETAILS_V12" (p_branch_code IN VARCHAR2,
                                                   p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '20_Pool_Details' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'LIAB_NO, POOL_CODE, POOL_CCY, POOL_AMOUNT,  POOL_UTIL, BLOCK_AMT ,AVAILABLE_AMOUNT';--,AVAILABLE_INTEREST_RATE,INTEREST_SPREAD,RATE_OF_INTEREST ';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select liab.liab_no,
                     gp.pool_code,
                     gp.pool_ccy,
                     gp.pool_amount,
                     gp.pool_util,
                     gp.block_amt,
                     gp.available_amount
                 --    odp.AVAILABLE_INTEREST_RATE,
                --     odp.INTEREST_SPREAD,
                --     odp.RATE_OF_INTEREST
                from ubsprod.getm_pool@fcubsv12                gp,
                     ubsprod.getm_liab@fcubsv12                liab
                 --    ubsprod.GETM_OD_POOL_COLL_CUSTOM@fcubsv12 odp
               where gp.liab_id = liab.id
                 --and odp.pool_id = gp.id
                 --and gp.liab_id = odp.liab_id
                 and liab.liab_branch = p_branch_code
                 and gp.record_stat = 'O'
                 and gp.auth_stat = 'A') LOOP
    l_line := rec.liab_no || ',' || rec.pool_code || ',' || rec.pool_ccy || ',' ||
              rec.pool_amount || ',' || rec.pool_util || ',' ||
              rec.BLOCK_AMT || ',' || rec.available_amount;/* || ',' ||
              rec.AVAILABLE_INTEREST_RATE || ',' || rec.INTEREST_SPREAD || ',' ||
              rec.RATE_OF_INTEREST;*/
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
END pr_20_Pool_Details_v12;
/
/
