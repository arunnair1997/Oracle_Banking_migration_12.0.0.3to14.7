-- PROCEDURE PR_8_ADDRESS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_8_ADDRESS_V12" (p_branch_code IN VARCHAR2,
                                             p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := 'pr_8_address_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUST_AC_NO, LOCATION, MEDIA, ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4, LANGUAGE, TEST_KEYWORD, COUNTRY, NAME, ANSWERBACK, TEST_REQUIRED, CHECKER_DT_STAMP, MOD_NO, RECORD_STAT, AUTH_STAT, ONCE_AUTH, MAKER_DT_STAMP, MAKER_ID, CHECKER_ID, TOBE_EMAILED, DELIVERY_BY, HOLD_MAIL, CUST_AC_BRN, DEFAULT_ADDRESS, ADDR_REC_NUM, PINCODE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select CUST_AC_NO,
                     LOCATION,
                     MEDIA,
                     ADDRESS1,
                     ADDRESS2,
                     ADDRESS3,
                     ADDRESS4,
                     LANGUAGE,
                     TEST_KEYWORD,
                     COUNTRY,
                     NAME,
                     ANSWERBACK,
                     TEST_REQUIRED,
                     CHECKER_DT_STAMP,
                     MOD_NO,
                     RECORD_STAT,
                     AUTH_STAT,
                     ONCE_AUTH,
                     MAKER_DT_STAMP,
                     MAKER_ID,
                     CHECKER_ID,
                     TOBE_EMAILED,
                     DELIVERY_BY,
                     HOLD_MAIL,
                     CUST_AC_BRN,
                     DEFAULT_ADDRESS,
                     ADDR_REC_NUM,
                     PINCODE
                from ubsprod.MSTM_CUST_ACC_ADDRESS@fcubsv12
               where cust_ac_no in(select cust_ac_no from ubsprod.sttm_cust_account@fcubsv12 where record_stat = 'O' and auth_stat = 'A')
               and CUST_AC_BRN = p_branch_code
               order by cust_ac_no, media) LOOP
    l_line := rec.CUST_AC_NO || ',' || rec.LOCATION || ',' || rec.MEDIA || ',' ||
              rec.ADDRESS1 || ',' || rec.ADDRESS2 || ',' || rec.ADDRESS3 || ',' ||
              rec.ADDRESS4 || ',' || rec.LANGUAGE || ',' ||
              rec.TEST_KEYWORD || ',' || rec.COUNTRY || ',' || rec.NAME || ',' ||
              rec.ANSWERBACK || ',' || rec.TEST_REQUIRED || ',' ||
              rec.CHECKER_DT_STAMP || ',' || rec.MOD_NO || ',' ||
              rec.RECORD_STAT || ',' || rec.AUTH_STAT || ',' ||
              rec.ONCE_AUTH || ',' || rec.MAKER_DT_STAMP || ',' ||
              rec.MAKER_ID || ',' || rec.CHECKER_ID || ',' ||
              rec.TOBE_EMAILED || ',' || rec.DELIVERY_BY || ',' ||
              rec.HOLD_MAIL || ',' || rec.CUST_AC_BRN || ',' ||
              rec.DEFAULT_ADDRESS || ',' || rec.ADDR_REC_NUM || ',' ||
              rec.PINCODE;
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
END pr_8_address_v12;
/
/
