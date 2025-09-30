-- PROCEDURE PR_7_ISLAMIC_TD_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_7_ISLAMIC_TD_V14" (
    p_branch_code IN VARCHAR2,
    p_dir         IN VARCHAR2
) IS
    l_file      UTL_FILE.FILE_TYPE;
    l_line      VARCHAR2(32767);
    l_filename  VARCHAR2(200);
  
    -- Cursor with explicit aliases (no duplicates)
    CURSOR c_data IS
    WITH Latest_ICTD AS (
 
    SELECT *
 
    FROM (
 
        SELECT ICTD.ACC,
 
               ICTD.BRN,
 
               ICTD.CCY,
 
               ICTD.TD_AMOUNT,
 
               ROW_NUMBER() OVER (PARTITION BY ICTD.ACC, ICTD.BRN ORDER BY ICTD.TD_AMOUNT DESC) AS rn
 
        FROM INTEGRATEDPP.ICZM_TD_DETAILS ICTD
 
        WHERE ICTD.ACC IN (
 
            SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
            WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
        )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_ICU AS (
 
    SELECT *
 
    FROM (
 
        SELECT ICU.ACC,
 
               ICU.BRN,
 
               ICU.PROD,
 
               ICU.UDE_EFF_DT,
 
               ICU.UDE_ID,
 
               ICU.UDE_VALUE,
 
               ICU.RATE_CODE,
 
               ICU.UDE_VARIANCE,
 
               ROW_NUMBER() OVER (PARTITION BY ICU.ACC, ICU.BRN ORDER BY ICU.UDE_EFF_DT DESC) AS rn
 
        FROM INTEGRATEDPP.ICZM_ACC_UDEVALS ICU
 
        WHERE ICU.ACC IN (
 
            SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
            WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
        )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_AB AS (
 
    SELECT *
 
    FROM (
 
        SELECT AB.ACCOUNT,
 
               AB.BRANCH,
 
               AB.AMOUNT_BLOCK_NO,
 
               AB.AMOUNT,
 
               ROW_NUMBER() OVER (PARTITION BY AB.ACCOUNT, AB.BRANCH ORDER BY AB.AMOUNT_BLOCK_NO DESC) AS rn
 
        FROM INTEGRATEDPP.CAZM_AMOUNT_BLOCKS AB
 
        WHERE AB.RECORD_STAT = 'O'
 
          AND AB.AUTH_STAT = 'A'
 
    ) X
 
    WHERE rn = 1
 
),

Latest_ICLK AS (
 
    SELECT *
 
    FROM (
 
        SELECT ICLK.CUST_AC_NO,
 
               ICLK.BRANCH_CODE,
 
               ICLK.LOCK_IN_DAYS,
 
               ICLK.LOCK_IN_MONTHS,
 
               ICLK.LOCK_IN_YEARS,
 
               ROW_NUMBER() OVER (PARTITION BY ICLK.CUST_AC_NO, ICLK.BRANCH_CODE ORDER BY ICLK.LOCK_IN_DAYS DESC) AS rn
 
        FROM INTEGRATEDPP.ICZM_ACC_BRKN_PR_CUSTOM ICLK
 
        WHERE ICLK.CUST_AC_NO IN (
 
            SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
            WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
        )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_ASIG AS (
 
    SELECT *
 
    FROM (
 
        SELECT ASIG.ACC_NO,
 
               ASIG.BRANCH,
 
               ASIG.RECORD_STAT,
 
               ASIG.CIF_SIG_ID,
 
               ROW_NUMBER() OVER (PARTITION BY ASIG.ACC_NO, ASIG.BRANCH ORDER BY ASIG.CIF_SIG_ID DESC) AS rn
 
        FROM INTEGRATEDPP.SVZM_ACC_SIG_DET ASIG
 
        WHERE ASIG.RECORD_STAT <> 'D'
 
          AND ASIG.ACC_NO IN (
 
              SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
              WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
          )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_AJH AS (
 
    SELECT *
 
    FROM (
 
        SELECT AJH.CUST_AC_NO,
 
               AJH.BRANCH_CODE,
 
               AJH.JOINT_HOLDER,
 
               ROW_NUMBER() OVER (PARTITION BY AJH.CUST_AC_NO, AJH.BRANCH_CODE ORDER BY AJH.JOINT_HOLDER DESC) AS rn
 
        FROM INTEGRATEDPP.STZM_ACC_JOINT_HOLDER AJH
 
        WHERE AJH.CUST_AC_NO IN (
 
            SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
            WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
              AND CUST_NO IN (
 
                  SELECT CUSTOMER_NO FROM INTEGRATEDPP.STZM_CUSTOMER
 
                  WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
              )
 
        )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_FUF AS (
 
    SELECT *
 
    FROM (
 
        SELECT FUF.*,
 
               ROW_NUMBER() OVER (
 
                   PARTITION BY SUBSTR(FUF.REC_KEY, 1, 3),
 
                                SUBSTR(FUF.REC_KEY, INSTR(FUF.REC_KEY,'~',1,1)+1,
 
                                      (INSTR(FUF.REC_KEY,'~',1,2)-1) - INSTR(FUF.REC_KEY,'~',1,1))
 
                   ORDER BY FUF.REC_KEY DESC
 
               ) AS rn
 
        FROM INTEGRATEDPP.CSZM_FUNCTION_USERDEF_FIELDS FUF
 
        WHERE FUF.FUNCTION_ID = 'STDCUSTD'
 
    ) X
 
    WHERE rn = 1
 
),

Latest_INT AS (
 
    SELECT *
 
    FROM (
 
        SELECT U.ACC,
 
               U.BRN,
 
               U.PROD,
 
               P.RULE,
 
               E.RESULT AS INT_CALC_METHOD,
 
               ROW_NUMBER() OVER (PARTITION BY U.ACC, U.BRN ORDER BY U.UDE_EFF_DT DESC) AS rn
 
        FROM INTEGRATEDPP.ICZM_ACC_UDEVALS U
 
        JOIN INTEGRATEDPP.ICZM_PR_INT P ON U.PROD = P.PRODUCT_CODE
 
        JOIN INTEGRATEDPP.ICZM_EXPR E ON P.RULE = E.RULE_ID
 
        WHERE P.MUDARABAH_PRODUCT IS NULL
 
          AND E.FRM_NO = 1
 
          AND E.EXPR_LINE = 1
 
          AND U.ACC IN (
 
              SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
              WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
          )
 
    ) X
 
    WHERE rn = 1
 
),

Latest_TOT_INT AS (
 
    SELECT *
 
    FROM (
 
        SELECT IC.ACC,
 
               IC.BRN,
 
               (IC.MATURITY_AMOUNT - ICTD.TD_AMOUNT) AS TOTAL_INTEREST,
 
               ROW_NUMBER() OVER (PARTITION BY IC.ACC, IC.BRN ORDER BY IC.MATURITY_DATE DESC) AS rn
 
        FROM INTEGRATEDPP.ICZM_TD_DETAILS ICTD
 
        JOIN INTEGRATEDPP.ICZM_ACC IC ON ICTD.ACC = IC.ACC AND ICTD.BRN = IC.BRN
 
        WHERE ICTD.ACC IN (
 
            SELECT CUST_AC_NO FROM INTEGRATEDPP.STZM_CUST_ACCOUNT
 
            WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
        )
 
    ) X
 
    WHERE rn = 1
 
)

SELECT
 
    Latest_ICU.ACC,
 
    Latest_ICU.BRN,
 
    Latest_ICTD.CCY,
 
    Latest_ICU.PROD,
 
    CUSAC.ACCOUNT_CLASS,
 
    Latest_ICTD.TD_AMOUNT,
 
    IC.INT_START_DATE,
 
    IC.INTEREST_RATE,
 
    IC.LAST_IS_DATE,
 
    IC.ROLLOVER_TYPE,
 
    IC.MATURITY_DATE,
 
    IC.MATURITY_AMOUNT,
 
    Latest_INT.RULE,
 
    Latest_INT.INT_CALC_METHOD,
 
    Latest_ICU.UDE_EFF_DT,
 
    Latest_ICU.UDE_ID,
 
    Latest_ICU.UDE_VALUE,
 
    Latest_ICU.RATE_CODE,
 
    Latest_ICU.UDE_VARIANCE,
 
    Latest_AB.AMOUNT_BLOCK_NO,
 
    Latest_AB.AMOUNT,
 
    Latest_ICLK.LOCK_IN_DAYS,
 
    Latest_ICLK.LOCK_IN_MONTHS,
 
    Latest_ICLK.LOCK_IN_YEARS,
 
    Latest_FUF.FUNCTION_ID,
 
    Latest_FUF.REC_KEY,
 
    Latest_ASIG.RECORD_STAT AS SIG_RECORD_STAT,
 
    Latest_ASIG.CIF_SIG_ID,
 
    Latest_AJH.JOINT_HOLDER,
 
    Latest_FUF.FIELD_VAL_1,
 
    Latest_FUF.FIELD_VAL_2,
 
    Latest_FUF.FIELD_VAL_3,
 
    Latest_FUF.FIELD_VAL_4,
 
    Latest_FUF.FIELD_VAL_5,
 
    Latest_FUF.FIELD_VAL_6,
 
    Latest_TOT_INT.TOTAL_INTEREST
 
FROM INTEGRATEDPP.ICZM_ACC IC
 
LEFT JOIN Latest_ICTD ON IC.ACC = Latest_ICTD.ACC AND IC.BRN = Latest_ICTD.BRN
 
LEFT JOIN Latest_ICU  ON IC.ACC = Latest_ICU.ACC AND IC.BRN = Latest_ICU.BRN
 
LEFT JOIN INTEGRATEDPP.STZM_CUST_ACCOUNT CUSAC
 
       ON IC.ACC = CUSAC.CUST_AC_NO AND IC.BRN = CUSAC.BRANCH_CODE
 
      AND CUSAC.RECORD_STAT = 'O' AND CUSAC.AUTH_STAT = 'A'
 
      AND CUSAC.CUST_NO IN (
 
          SELECT CUSTOMER_NO FROM INTEGRATEDPP.STZM_CUSTOMER
 
          WHERE RECORD_STAT = 'O' AND AUTH_STAT = 'A'
 
      )
 
LEFT JOIN Latest_AB       ON IC.ACC = Latest_AB.ACCOUNT AND IC.BRN = Latest_AB.BRANCH
 
LEFT JOIN Latest_ICLK     ON IC.ACC = Latest_ICLK.CUST_AC_NO AND IC.BRN = Latest_ICLK.BRANCH_CODE
 
LEFT JOIN Latest_FUF      ON IC.BRN = SUBSTR(Latest_FUF.REC_KEY, 1, 3)
 
                          AND IC.ACC = SUBSTR(Latest_FUF.REC_KEY, INSTR(Latest_FUF.REC_KEY, '~', 1, 1) + 1,
 
                                              ((INSTR(Latest_FUF.REC_KEY, '~', 1, 2) - 1) - INSTR(Latest_FUF.REC_KEY, '~', 1, 1)))
 
LEFT JOIN Latest_AJH      ON IC.ACC = Latest_AJH.CUST_AC_NO AND IC.BRN = Latest_AJH.BRANCH_CODE
 
LEFT JOIN Latest_ASIG     ON IC.ACC = Latest_ASIG.ACC_NO AND IC.BRN = Latest_ASIG.BRANCH
 
LEFT JOIN Latest_INT      ON IC.ACC = Latest_INT.ACC AND IC.BRN = Latest_INT.BRN
 
LEFT JOIN Latest_TOT_INT  ON IC.ACC = Latest_TOT_INT.ACC AND IC.BRN = Latest_TOT_INT.BRN
 
WHERE CUSAC.ACCOUNT_TYPE = 'Y'
 
  AND CUSAC.MUDARABAH_ACCOUNTS = 'Y'
 
  AND IC.BRN = p_branch_code;
  
BEGIN
    -- open file
    l_filename := 'PR_7_ISLAMIC_TD_' || p_branch_code || '_V14.csv';
    l_file := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);

    -- header row
    l_line := 'ACC,BRN,CCY,PROD,ACCOUNT_CLASS,TD_AMOUNT,INT_START_DATE,INTEREST_RATE,LAST_IS_DATE,ROLLOVER_TYPE,MATURITY_DATE,MATURITY_AMOUNT,RULE,INT_CALC_METHOD,UDE_EFF_DT,UDE_ID,UDE_VALUE,RATE_CODE,UDE_VARIANCE,AMOUNT_BLOCK_NO,AMOUNT,LOCK_IN_DAYS,LOCK_IN_MONTHS,LOCK_IN_YEARS,FUNCTION_ID,REC_KEY,SIGNATURE_RECORD_STATUS,CIF_SIG_ID,JOINT_HOLDER,مؤشر حفظ الشهادة,CUSTOMER AGE,حساب العميل,INSTALLMENT,REPRESENTATIVES,TDPAYOUT_VALIDATION';
    UTL_FILE.PUT_LINE(l_file, l_line);

    -- loop rows
    FOR rec IN c_data LOOP
        l_line :=
            rec.ACC || ',' ||
            rec.BRN || ',' ||
            rec.CCY || ',' ||
            rec.PROD || ',' ||
            rec.CCY || ',' ||
            rec.ACCOUNT_CLASS || ',' ||
            rec.TD_AMOUNT || ',' ||
            rec.INT_START_DATE || ',' ||
            rec.INTEREST_RATE || ',' ||
            rec.LAST_IS_DATE || ',' ||
            rec.ROLLOVER_TYPE || ',' ||
            rec.MATURITY_DATE || ',' ||
            rec.MATURITY_AMOUNT || ',' ||
            rec.RULE || ',' ||
            rec.INT_CALC_METHOD || ',' ||
            rec.UDE_EFF_DT || ',' ||
            rec.UDE_ID || ',' ||
            rec.UDE_VALUE || ',' ||
            rec.RATE_CODE || ',' ||
            rec.UDE_VARIANCE || ',' ||
            rec.AMOUNT_BLOCK_NO || ',' ||
            rec.AMOUNT || ',' ||
            rec.LOCK_IN_DAYS || ',' ||
            rec.LOCK_IN_MONTHS || ',' ||
            rec.LOCK_IN_YEARS || ',' ||
            rec.FUNCTION_ID || ',' ||
            rec.REC_KEY || ',' ||
            rec.SIG_RECORD_STAT || ',' ||
            rec.CIF_SIG_ID || ',' ||
            rec.JOINT_HOLDER|| ',' ||
            rec.FIELD_VAL_1 || ',' ||
            rec.FIELD_VAL_2 || ',' ||
            rec.FIELD_VAL_3 || ',' ||
            rec.FIELD_VAL_4 || ',' ||
            rec.FIELD_VAL_5 || ',' ||
            rec.FIELD_VAL_6;
        UTL_FILE.PUT_LINE(l_file, l_line);
    END LOOP;

    UTL_FILE.FCLOSE(l_file);

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        IF UTL_FILE.IS_OPEN(l_file) THEN
            UTL_FILE.FCLOSE(l_file);
        END IF;
        RAISE;
END PR_7_ISLAMIC_TD_V14;
/
/
