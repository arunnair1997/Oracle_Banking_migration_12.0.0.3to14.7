-- PROCEDURE PR_48_OPEN_INSTRUMENTS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_48_OPEN_INSTRUMENTS_SUMM" (p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '48_OPEN_INSTRUMENTS_SUMM.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT_CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select V12.product, v12cnt, v14cnt
                from (select product, count(*) v12cnt
                        from ubsprod.SETM_SECURITY_MASTER@fcubsv12
                       where record_stat = 'O'
                         and Auth_stat = 'A'
                       group by product) V12
                LEFT JOIN (select product, count(*) v14cnt
                            from integratedpp.SEZM_SECURITY_MASTER
                           where record_stat = 'O'
                             and Auth_stat = 'A'
                           group by product) V14
                  ON V12.product = v14.product) LOOP
    l_line := rec.product || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END PR_48_OPEN_INSTRUMENTS_SUMM;
/
/
