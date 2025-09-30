-- PROCEDURE PR_22_POOLVSFACILITY_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_22_POOLVSFACILITY_V12" (p_branch_code IN VARCHAR2,
                                                     p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '22_PoolvsFacility_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ID, BRN, LIAB NO, LIAB NAME,  LINE CCY, LINE CODE SERIAL ,MAIN LINE ,AVAILABLE,LINE START DATE,LINE EXPIRY DATE,POOL CODE,POOL CCY,POOL AMT,FACILITY AMT POOL CCY,BLOCK AMOUNT, POOL PERCENTAGE ';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT fac.id,
                     fac.brn,
                     liab.liab_no,
                     liab.liab_name,
                     fac.line_currency,
                     fac.line_code || fac.line_serial AS facility,
                     mainline.line_code || mainline.line_serial AS mainline,
                     fac.availability_flag,
                     fac.line_start_date,
                     fac.line_expiry_date,
                     pool.pool_code,
                     pool.pool_ccy,
                     pool.pool_amount,
                     gpl.facility_amount,
                     gpl.facility_amount_pool_ccy,
                     pool.block_amt,
                     gpl.percentage_of_pool
                FROM getm_facility@fcubsv12 fac
                JOIN getm_liab@fcubsv12 liab
                  ON liab.id = fac.liab_id
                JOIN getm_pool@fcubsv12 pool
                  ON pool.liab_id = fac.liab_id
                JOIN getb_pool_link@fcubsv12 gpl
                  ON gpl.facility_id = fac.id
                 AND gpl.liab_id = fac.liab_id
                 AND gpl.pool_id = pool.id
                LEFT JOIN getm_facility@fcubsv12 mainline
                  ON fac.main_line_id = mainline.id
               WHERE liab.liab_branch = p_branch_code
               ORDER BY fac.id, pool.id) LOOP
    l_line := rec.id || ',' || rec.brn || ',' || rec.liab_no || ',' ||
              rec.liab_name || ',' || rec.line_currency || ',' ||
              rec.facility || ',' || rec.mainline || ',' ||
              rec.availability_flag || ',' || rec.line_start_date || ',' ||
              rec.line_expiry_date || ',' || rec.pool_code || ',' ||
              rec.pool_ccy || ',' || rec.pool_amount || ',' ||
              rec.facility_amount || ',' || rec.facility_amount_pool_ccy || ',' ||
              rec.block_amt || ',' || rec.percentage_of_pool;
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
END pr_22_PoolvsFacility_v12;
/
/
