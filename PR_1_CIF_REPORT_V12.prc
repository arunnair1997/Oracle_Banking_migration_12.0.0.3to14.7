-- PROCEDURE PR_1_CIF_REPORT_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_1_CIF_REPORT_V12" (p_branch_code IN VARCHAR2,
                                                p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename     VARCHAR2(200);
  v_record_count NUMBER := 0;
  v_file_index   NUMBER := 1;
  v_max_records constant number := 200000;

  procedure open_new_file is
  BEGIN
    -- Construct filename
    l_filename := '1_CIF_REPORT_' || p_branch_code || '_v12.csv';
    l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
    dbms_output.put_line('CHECK1');
    -- Write header line
    l_line := 'CUSTOMER_NO, CUSTOMER_TYPE, CUSTOMER_NAME1, ADDRESS_LINE1, ADDRESS_LINE3, ADDRESS_LINE2, ADDRESS_LINE4, COUNTRY, SHORT_NAME, NATIONALITY, LANGUAGE, LOCAL_BRANCH, UNIQUE_ID_NAME, UNIQUE_ID_VALUE, FROZEN, DECEASED, WHEREABOUTS_UNKNOWN, CUSTOMER_CATEGORY, RECORD_STAT, AUTH_STAT, MOD_NO, MAKER_ID, MAKER_DT_STAMP, CHECKER_ID, CHECKER_DT_STAMP, ONCE_AUTH, DEFAULT_MEDIA, SSN, LOC_CODE, FULL_NAME, UDF_1, UDF_2, UDF_3, UDF_4, UDF_5, AML_REQUIRED, AML_CUSTOMER_GRP, MAILERS_REQUIRED, CHARGE_GROUP, RISK_CATEGORY, CIF_CREATION_DATE, GENERATE_MT920, KYC_DETAILS, STAFF, KYC_REF_NO, TELEPHONE, E_MAIL,????? ??????? ???????,????? ????? ???????,????? ????? ???????,???? ??????? ??????? ????? ??????,???? ??????? ??????? 3 ????? ?????,????? ?????? ??????? ???????,????? ????? ??????,???? ?????,??? ????? ??????,???? ??????,?????? ??????? APEX,?????? ????,???? ????,??? ?????? ?????,????? ??? ??????? ????? ???,????? ????? ?????,????? ?????? ??????,????? ?????? ?????,????? ?? ??? ??????,????? ?????? ??????????,PUBLIC FIGURE,???? ??? ??asdf??????????';
    UTL_FILE.PUT_LINE(l_file, l_line);
  END;

BEGIN
  open_new_file;
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT CIF.CUSTOMER_NO,
                     CIF.CUSTOMER_TYPE,
                     CIF.CUSTOMER_NAME1,
                     CIF.ADDRESS_LINE1,
                     CIF.ADDRESS_LINE3,
                     CIF.ADDRESS_LINE2,
                     CIF.ADDRESS_LINE4,
                     CIF.COUNTRY,
                     CIF.SHORT_NAME,
                     CIF.NATIONALITY,
                     CIF.LANGUAGE,
                     CIF.LOCAL_BRANCH,
                     CIF.UNIQUE_ID_NAME,
                     CIF.UNIQUE_ID_VALUE,
                     CIF.FROZEN,
                     CIF.DECEASED,
                     CIF.WHEREABOUTS_UNKNOWN,
                     CIF.CUSTOMER_CATEGORY,
                     CIF.RECORD_STAT,
                     CIF.AUTH_STAT,
                     CIF.MOD_NO,
                     CIF.MAKER_ID,
                     CIF.MAKER_DT_STAMP,
                     CIF.CHECKER_ID,
                     CIF.CHECKER_DT_STAMP,
                     CIF.ONCE_AUTH,
                     CIF.DEFAULT_MEDIA,
                     CIF.SSN,
                     CIF.LOC_CODE,
                     CIF.FULL_NAME,
                     CIF.UDF_1,
                     CIF.UDF_2,
                     CIF.UDF_3,
                     CIF.UDF_4,
                     CIF.UDF_5,
                     CIF.AML_REQUIRED,
                     CIF.AML_CUSTOMER_GRP,
                     CIF.MAILERS_REQUIRED,
                     CIF.CHARGE_GROUP,
                     CIF.RISK_CATEGORY,
                     CIF.CIF_CREATION_DATE,
                     CIF.GENERATE_MT920,
                     CIF.KYC_DETAILS,
                     CIF.STAFF,
                     CIF.KYC_REF_NO,
                     CIP.TELEPHONE,
                     CIP.E_MAIL,
                     CIU.FIELD_VAL_1,
                     CIU.FIELD_VAL_2,
                     CIU.FIELD_VAL_3,
                     CIU.FIELD_VAL_4,
                     CIU.FIELD_VAL_5,
                     CIU.FIELD_VAL_6,
                     CIU.FIELD_VAL_7,
                     CIU.FIELD_VAL_8,
                     CIU.FIELD_VAL_9,
                     CIU.FIELD_VAL_10,
                     CIU.FIELD_VAL_11,
                     CIU.FIELD_VAL_12,
                     CIU.FIELD_VAL_13,
                     CIU.FIELD_VAL_14,
                     CIU.FIELD_VAL_15,
                     CIU.FIELD_VAL_16,
                     CIU.FIELD_VAL_17,
                     CIU.FIELD_VAL_18,
                     CIU.FIELD_VAL_19,
                     CIU.FIELD_VAL_20,
                     CIU.FIELD_VAL_21,
                     CIU.FIELD_VAL_22
                FROM STTM_CUSTOMER@FCUBSV12 CIF
                LEFT JOIN STTM_CUST_PERSONAL@fcubsv12 CIP
                  ON CIF.CUSTOMER_NO = CIP.CUSTOMER_NO
                LEFT JOIN CSTM_FUNCTION_USERDEF_FIELDS@fcubsv12 CIU
                  ON CIF.CUSTOMER_NO || '~' = CIU.REC_KEY
                 AND CIU.function_id = 'STDCIF'
               WHERE CIF.RECORD_STAT = 'O'
                 AND CIF.AUTH_STAT = 'A'
                 AND CIF.LOCAL_BRANCH = p_branch_code
               ORDER BY CIF.CUSTOMER_NO) LOOP
    v_record_count := v_record_count + 1;
    if v_record_count > v_max_records then
      utl_file.fclose(l_file);
      v_file_index   := v_file_index + 1;
      v_record_count := 1;
      open_new_file;
    end if;
    l_line := rec.CUSTOMER_NO || ',' || rec.CUSTOMER_TYPE || ',' ||
              rec.CUSTOMER_NAME1 || ',' || rec.ADDRESS_LINE1 || ',' ||
              rec.ADDRESS_LINE3 || ',' || rec.ADDRESS_LINE2 || ',' ||
              rec.ADDRESS_LINE4 || ',' || rec.COUNTRY || ',' ||
              rec.SHORT_NAME || ',' || rec.NATIONALITY || ',' ||
              rec.LANGUAGE || ',' || rec.LOCAL_BRANCH || ',' ||
              rec.UNIQUE_ID_NAME || ',' || rec.UNIQUE_ID_VALUE || ',' ||
              rec.FROZEN || ',' || rec.DECEASED || ',' ||
              rec.WHEREABOUTS_UNKNOWN || ',' || rec.CUSTOMER_CATEGORY || ',' ||
              rec.RECORD_STAT || ',' || rec.AUTH_STAT || ',' || rec.MOD_NO || ',' ||
              rec.MAKER_ID || ',' || rec.MAKER_DT_STAMP || ',' ||
              rec.CHECKER_ID || ',' || rec.CHECKER_DT_STAMP || ',' ||
              rec.ONCE_AUTH || ',' || rec.DEFAULT_MEDIA || ',' || rec.SSN || ',' ||
              rec.LOC_CODE || ',' || rec.FULL_NAME || ',' || rec.UDF_1 || ',' ||
              rec.UDF_2 || ',' || rec.UDF_3 || ',' || rec.UDF_4 || ',' ||
              rec.UDF_5 || ',' || rec.AML_REQUIRED || ',' ||
              rec.AML_CUSTOMER_GRP || ',' || rec.MAILERS_REQUIRED || ',' ||
              rec.CHARGE_GROUP || ',' || rec.RISK_CATEGORY || ',' ||
              rec.CIF_CREATION_DATE || ',' || rec.GENERATE_MT920 || ',' ||
              rec.KYC_DETAILS || ',' || rec.STAFF || ',' || rec.KYC_REF_NO || ',' ||
              rec.TELEPHONE || ',' || rec.E_MAIL || ',' || rec.FIELD_VAL_1 || ',' ||
              rec.FIELD_VAL_2 || ',' || rec.FIELD_VAL_3 || ',' ||
              rec.FIELD_VAL_4 || ',' || rec.FIELD_VAL_5 || ',' ||
              rec.FIELD_VAL_6 || ',' || rec.FIELD_VAL_7 || ',' ||
              rec.FIELD_VAL_8 || ',' || rec.FIELD_VAL_9 || ',' ||
              rec.FIELD_VAL_10 || ',' || rec.FIELD_VAL_11 || ',' ||
              rec.FIELD_VAL_12 || ',' || rec.FIELD_VAL_13 || ',' ||
              rec.FIELD_VAL_14 || ',' || rec.FIELD_VAL_15 || ',' ||
              rec.FIELD_VAL_16 || ',' || rec.FIELD_VAL_17 || ',' ||
              rec.FIELD_VAL_18 || ',' || rec.FIELD_VAL_19 || ',' ||
              rec.FIELD_VAL_20 || ',' || rec.FIELD_VAL_21 || ',' ||
              rec.FIELD_VAL_22;
    UTL_FILE.PUT_LINE(l_file, l_line);
  END LOOP;
  dbms_output.put_line('CHECK3');
  if utl_file.is_open(l_file) then
    UTL_FILE.FCLOSE(l_file);
  end if;

EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('BOMBED' || SQLERRM);
    IF UTL_FILE.IS_OPEN(l_file) THEN
      UTL_FILE.FCLOSE(l_file);
    END IF;
    RAISE;
END pr_1_CIF_REPORT_v12;
/
/
