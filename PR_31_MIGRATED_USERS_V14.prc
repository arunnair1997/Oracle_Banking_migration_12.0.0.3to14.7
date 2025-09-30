-- PROCEDURE PR_31_MIGRATED_USERS_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_31_MIGRATED_USERS_V14" (p_branch_code IN VARCHAR2,
                                                     p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '31_Migrated_users_' || p_branch_code || '_v14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'USER_ID, START_DATE, USER_NAME, STATUS_CHANGED_ON, TILL_ALLOWED, ACCLASS_ALLOWED, PRODUCTS_ALLOWED, BRANCHES_ALLOWED, MAX_OVERRIDE_AMT, TIME_LEVEL, USER_CATEGORY, USER_STATUS, END_DATE, PWD_CHANGED_ON, MAX_TXN_AMT, MAX_AUTH_AMT, FORCE_PASSWD_CHANGE, USER_LANGUAGE, HOME_BRANCH, GL_ALLOWED, AUTH_STAT, CHECKER_DT_STAMP, CHECKER_ID, MAKER_DT_STAMP, MAKER_ID, MOD_NO, ONCE_AUTH, RECORD_STAT, USER_TXN_LIMIT, LIMITS_CCY, AUTO_AUTH, CUSTOMER_NO, PRODUCTS_ACCESS_ALLOWED, DFLT_MODULE, USER_EMAIL, TELEPHONE_NUMBER, USER_MANAGER, HOME_PHONE, USER_MOBILE, TAX_IDENTIFIER, STAFF_AC_RESTR, AMOUNT_FORMAT, DATE_FORMAT, DEPT_CODE, GROUP_CODE_ALLOWED, MULTIBRANCH_ACCESS, OTHER_RM_CUST_RESTRICT, ACCESS_CONTROL';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select USER_ID,
                     START_DATE,
                     USER_NAME,
                     STATUS_CHANGED_ON,
                     TILL_ALLOWED,
                     ACCLASS_ALLOWED,
                     PRODUCTS_ALLOWED,
                     BRANCHES_ALLOWED,
                     MAX_OVERRIDE_AMT,
                     TIME_LEVEL,
                     USER_CATEGORY,
                     USER_STATUS,
                     END_DATE,
                     PWD_CHANGED_ON,
                     MAX_TXN_AMT,
                     MAX_AUTH_AMT,
                     FORCE_PASSWD_CHANGE,
                     USER_LANGUAGE,
                     HOME_BRANCH,
                     GL_ALLOWED,
                     AUTH_STAT,
                     CHECKER_DT_STAMP,
                     CHECKER_ID,
                     MAKER_DT_STAMP,
                     MAKER_ID,
                     MOD_NO,
                     ONCE_AUTH,
                     RECORD_STAT,
                     USER_TXN_LIMIT,
                     LIMITS_CCY,
                     AUTO_AUTH,
                     CUSTOMER_NO,
                     PRODUCTS_ACCESS_ALLOWED,
                     DFLT_MODULE,
                     USER_EMAIL,
                     TELEPHONE_NUMBER,
                     USER_MANAGER,
                     HOME_PHONE,
                     USER_MOBILE,
                     TAX_IDENTIFIER,
                     STAFF_AC_RESTR,
                     AMOUNT_FORMAT,
                     DATE_FORMAT,
                     DEPT_CODE,
                     GROUP_CODE_ALLOWED,
                     MULTIBRANCH_ACCESS,
                     OTHER_RM_CUST_RESTRICT,
                     ACCESS_CONTROL
                from integratedpp.smzb_user
               where record_stat = 'O'
                 and auth_stat = 'A'
                 and HOME_BRANCH = p_branch_code) LOOP
    l_line := rec.USER_ID || ',' || rec.START_DATE || ',' || rec.USER_NAME || ',' ||
              rec.STATUS_CHANGED_ON || ',' || rec.TILL_ALLOWED || ',' ||
              rec.ACCLASS_ALLOWED || ',' || rec.PRODUCTS_ALLOWED || ',' ||
              rec.BRANCHES_ALLOWED || ',' || rec.MAX_OVERRIDE_AMT || ',' ||
              rec.TIME_LEVEL || ',' || rec.USER_CATEGORY || ',' ||
              rec.USER_STATUS || ',' || rec.END_DATE || ',' ||
              rec.PWD_CHANGED_ON || ',' || rec.MAX_TXN_AMT || ',' ||
              rec.MAX_AUTH_AMT || ',' || rec.FORCE_PASSWD_CHANGE || ',' ||
              rec.USER_LANGUAGE || ',' || rec.HOME_BRANCH || ',' ||
              rec.GL_ALLOWED || ',' || rec.AUTH_STAT || ',' ||
              rec.CHECKER_DT_STAMP || ',' || rec.CHECKER_ID || ',' ||
              rec.MAKER_DT_STAMP || ',' || rec.MAKER_ID || ',' ||
              rec.MOD_NO || ',' || rec.ONCE_AUTH || ',' || rec.RECORD_STAT || ',' ||
              rec.USER_TXN_LIMIT || ',' || rec.LIMITS_CCY || ',' ||
              rec.AUTO_AUTH || ',' || rec.CUSTOMER_NO || ',' ||
              rec.PRODUCTS_ACCESS_ALLOWED || ',' || rec.DFLT_MODULE || ',' ||
              rec.USER_EMAIL || ',' || rec.TELEPHONE_NUMBER || ',' ||
              rec.USER_MANAGER || ',' || rec.HOME_PHONE || ',' ||
              rec.USER_MOBILE || ',' || rec.TAX_IDENTIFIER || ',' ||
              rec.STAFF_AC_RESTR || ',' || rec.AMOUNT_FORMAT || ',' ||
              rec.DATE_FORMAT || ',' || rec.DEPT_CODE || ',' ||
              rec.GROUP_CODE_ALLOWED || ',' || rec.MULTIBRANCH_ACCESS || ',' ||
              rec.OTHER_RM_CUST_RESTRICT || ',' || rec.ACCESS_CONTROL;
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
END pr_31_Migrated_users_v14;
/
/
