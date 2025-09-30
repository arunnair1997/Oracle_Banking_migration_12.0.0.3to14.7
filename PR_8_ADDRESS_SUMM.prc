-- PROCEDURE PR_8_ADDRESS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_8_ADDRESS_SUMM" (p_branch_code IN VARCHAR2,
                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '8_ADDRESS_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'LOCATION,MEDIA, V12_COUNT_CUST_AC_NO, V14_COUNT_CUST_AC_NO';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT 
   NVL(v12.LOCATION, v14.LOCATION) AS LOCATION,
   NVL(v12.MEDIA, v14.MEDIA) AS MEDIA,
   NVL(v12.v12_count_cust_ac_no, 0) AS v12_count_cust_ac_no,
   NVL(v14.v14_count_cust_ac_no, 0) AS v14_count_cust_ac_no
FROM (
   SELECT LOCATION, MEDIA, COUNT(CUST_AC_NO) AS v12_count_cust_ac_no
   FROM ubsprod.MSTM_CUST_ACC_ADDRESS@fcubsv12
   WHERE cust_ac_no IN (
             SELECT cust_ac_no
             FROM ubsprod.sttm_cust_account@fcubsv12
             WHERE record_stat = 'O' 
               AND auth_stat = 'A')
     AND CUST_AC_BRN = p_branch_code
   GROUP BY LOCATION, MEDIA
) v12
FULL OUTER JOIN (
    SELECT LOCATION, MEDIA, COUNT(CUST_AC_NO) AS v14_count_cust_ac_no
    FROM integratedpp.MSZM_CUST_ACC_ADDRESS
    WHERE cust_ac_no IN (
              SELECT cust_ac_no
              FROM integratedpp.stzm_cust_account
              WHERE record_stat = 'O' 
                AND auth_stat = 'A')
      AND CUST_AC_BRN = p_branch_code
    GROUP BY LOCATION, MEDIA
) v14
ON v12.LOCATION = v14.LOCATION
AND v12.MEDIA = v14.MEDIA
) LOOP
    l_line := rec.LOCATION || ',' || rec.MEDIA  || ',' || rec.v12_count_cust_ac_no || ',' ||
              rec.v14_count_cust_ac_no;
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
END pr_8_ADDRESS_SUMM;
/
/
