-- PROCEDURE PR_49_OPEN_PORTFOLIOS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_49_OPEN_PORTFOLIOS_SUMM" (p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '49_OPEN_PORTFOLIOS.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PORTFOLIO_PRODUCT_CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select V12.PORTFOLIO_PRODUCT_CODE, V12cnt, V14cnt
  from (select PORTFOLIO_PRODUCT_CODE, count(PORTFOLIO_REF_NO) v12cnt
          from ubsprod.SETM_PORTFOLIO_MASTER@fcubsv12
         where record_stat = 'O'
           and auth_stat = 'A'
         group by PORTFOLIO_PRODUCT_CODE) V12
  LEFT JOIN (select PORTFOLIO_PRODUCT_CODE, count(PORTFOLIO_REF_NO) v14cnt
               from integratedpp.SEZM_PORTFOLIO_MASTER
              where record_stat = 'O'
                and auth_stat = 'A'
              group by PORTFOLIO_PRODUCT_CODE) V14
    ON V14.PORTFOLIO_PRODUCT_CODE = v12.PORTFOLIO_PRODUCT_CODE

) LOOP
    l_line := rec.PORTFOLIO_PRODUCT_CODE || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END PR_49_OPEN_PORTFOLIOS_SUMM;
/
/
