-- PROCEDURE PR_14_LIABVSUTIL_V12_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_14_LIABVSUTIL_V12_V14" (p_branch_code IN VARCHAR2,
                                                     p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '14_liabvsutil_' || p_branch_code || '_v12_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO, LIAB_NO, V12_UTILISATION, V12_LIAB_CCY, V12_LIAB_UTIL, V14_UTILISATION, V14_LIAB_CCY, V14_LIAB_UTIL';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT COALESCE(v12.customer_no, v14.customer_no) customer_no,
                     COALESCE(v12.liab_no, v14.liab_no) liab_no,
                     v12.utils v12_utilisation,
                     v12.liab_ccy v12_liab_ccy,
                     v12.util_amt v12_liab_util,
                     v14.utils v14_utilisation,
                     v14.liab_ccy v14_liab_ccy,
                     v14.util_amt v14_liab_util
                FROM (SELECT a.customer_no,
                             b.liab_no,
                             a.liab_ccy AS liab_ccy, -- alias explicitly
                             SUM(a.liab_util) AS utils,
                             b.util_amt AS util_amt
                        FROM ubsprod.getb_utils@fcubsv12 a,
                             ubsprod.getm_liab@fcubsv12  b
                       WHERE a.liab_id = b.id
                         AND a.util_stat = 'A'
                         AND a.liab_branch = p_branch_code
                       GROUP BY a.customer_no,
                                a.liab_ccy,
                                b.liab_no,
                                b.util_amt) v12
                FULL OUTER JOIN (SELECT a.customer_no,
                                       b.liab_no,
                                       a.liab_ccy AS liab_ccy, -- alias explicitly
                                       SUM(a.liab_util) AS utils,
                                       b.util_amt AS util_amt
                                  FROM integratedpp.gezb_utils a,
                                       integratedpp.gezm_liab  b
                                 WHERE a.liab_id = b.id
                                   AND a.util_stat = 'A'
                                   AND a.liab_branch = p_branch_code
                                 GROUP BY a.customer_no,
                                          a.liab_ccy,
                                          b.liab_no,
                                          b.util_amt) v14
                  ON (v12.liab_no = v14.liab_no AND
                     v12.customer_no = v14.customer_no)) LOOP
    l_line := rec.CUSTOMER_NO || ',' || rec.LIAB_NO || ',' ||
              rec.V12_UTILISATION || ',' || rec.V12_LIAB_CCY || ',' ||
              rec.V12_LIAB_UTIL || ',' || rec.V14_UTILISATION || ',' ||
              rec.V14_LIAB_CCY || ',' || rec.V14_LIAB_UTIL;
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
END pr_14_liabvsutil_v12_v14;
/
/
