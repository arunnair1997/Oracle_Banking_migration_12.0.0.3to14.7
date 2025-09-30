-- PROCEDURE PR_11_STANDING_INSTRUCTIONS_CONTRACTS_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_11_STANDING_INSTRUCTIONS_CONTRACTS_V14" (p_branch_code IN VARCHAR2,
                                                                      p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '11_STANDING_INSTRUCTIONS_CONTRACTS' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'BRANCH,PRODUCT_CODE,INSTRUCTION_NO,INST_VERSION_NO,LATEST_VERSION_NO,SI_TYPE,INSTRUCTION_STATUS,INSTRUCTION_AUTH_STATUS,FIRST_EXEC_DATE,NEXT_EXEC_DATE,CAL_HOL_EXCP,COUNTERPARTY,LATEST_CYCLE_NO,LATEST_CYCLE_DATE,CONTRACT_REF_NO,SI_EXPIRY_DATE,DR_ACCOUNT,DR_ACC_CCY,SI_AMT,SI_AMT_CCY,CR_ACCOUNT,CR_ACC_CCY,ACCOUNT,ACCOUNT_TYPE,CONTRACT_STATUS,BOOK_DATE';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (
WITH instr AS (
    SELECT I.BRANCH,
           I.PRODUCT_CODE,
          I.INSTRUCTION_NO,
           I.INST_VERSION_NO,
           I.LATEST_VERSION_NO,
           I.SI_TYPE,
           I.INST_STATUS,
           I.AUTH_STATUS,
           I.FIRST_EXEC_DATE,
           I.NEXT_EXEC_DATE,
           I.CAL_HOL_EXCP,
           I.COUNTERPARTY,
           I.LATEST_CYCLE_NO,
           I.LATEST_CYCLE_DATE,
           ROW_NUMBER() OVER (PARTITION BY I.INSTRUCTION_NO ORDER BY I.INST_VERSION_NO DESC) rn
      FROM INTEGRATEDPP.SIZB_INSTRUCTION I
      JOIN INTEGRATEDPP.SIZB_CONTRACT_MASTER A
        ON I.INSTRUCTION_NO = A.INSTRUCTION_NO
      JOIN INTEGRATEDPP.CSZB_CONTRACT B
        ON A.CONTRACT_REF_NO = B.CONTRACT_REF_NO
     WHERE I.INST_VERSION_NO >= I.LATEST_VERSION_NO
       AND B.MODULE_CODE = 'SI'
       AND B.CONTRACT_STATUS = 'A'
       AND B.AUTH_STATUS = 'A'
 )
SELECT i.BRANCH,
       i.PRODUCT_CODE,
       i.INSTRUCTION_NO,
       i.INST_VERSION_NO,
       i.LATEST_VERSION_NO,
       DECODE(i.SI_TYPE,'1','ONE TO ONE','2','ONE TO MANY','3','MANY TO ONE','4','MANY TO MANY') AS SI_TYPE,
       DECODE(i.INST_STATUS,'A','ACTIVE','H','HOLD','S','CLOSE') AS INSTRUCTION_STATUS,
       i.AUTH_STATUS AS INSTRUCTION_AUTH_STATUS,
       i.FIRST_EXEC_DATE,
       i.NEXT_EXEC_DATE,
       i.CAL_HOL_EXCP,
       i.COUNTERPARTY,
       i.LATEST_CYCLE_NO,
       i.LATEST_CYCLE_DATE,
       m.CONTRACT_REF_NO,
       m.SI_EXPIRY_DATE,
       m.DR_ACCOUNT,
       m.DR_ACC_CCY,
       m.SI_AMT,
       m.SI_AMT_CCY,
       m.CR_ACCOUNT,
       m.CR_ACC_CCY,
       COALESCE(agl.AC_GL_NO, aca.AC_GL_NO) AS ACCOUNT,
       DECODE(COALESCE(agl.AC_OR_GL, aca.AC_OR_GL),'A','ACCOUNT','G','GL',NULL) AS ACCOUNT_TYPE,
       DECODE(c.CONTRACT_STATUS,'K','CANCEL','V','REVERSED','S','CLOSED','A','ACTIVE','H','HOLD') AS CONTRACT_STATUS,
       c.BOOK_DATE
  FROM instr i
  JOIN INTEGRATEDPP.SIZB_CONTRACT_MASTER m
    ON i.INSTRUCTION_NO = m.INSTRUCTION_NO
  JOIN INTEGRATEDPP.CSZB_CONTRACT c
    ON m.CONTRACT_REF_NO = c.CONTRACT_REF_NO
   AND c.MODULE_CODE = 'SI'
   AND c.CONTRACT_STATUS = 'A'
   AND c.AUTH_STATUS = 'A'
  LEFT JOIN INTEGRATEDPP.STZB_ACCOUNT_GL agl
    ON agl.AC_GL_NO IN (m.DR_ACCOUNT, m.CR_ACCOUNT)
  LEFT JOIN INTEGRATEDPP.STZB_ACCOUNT_CA aca
    ON aca.AC_GL_NO IN (m.DR_ACCOUNT, m.CR_ACCOUNT)
WHERE i.rn = 1
  AND i.BRANCH = p_branch_code
  AND M.SI_EXPIRY_DATE >  (SELECT today
                            FROM INTEGRATEDPP.stZm_dates
                           WHERE branch_code = '100')                         
ORDER BY i.INSTRUCTION_NO) LOOP
    l_line := rec.BRANCH || ',' || rec.PRODUCT_CODE || ',' ||
              rec.INSTRUCTION_NO || ',' || rec.INST_VERSION_NO || ',' ||
              rec.LATEST_VERSION_NO || ',' || rec.SI_TYPE || ',' ||
              rec.INSTRUCTION_STATUS || ',' || rec.INSTRUCTION_AUTH_STATUS || ',' ||
              rec.FIRST_EXEC_DATE || ',' || rec.NEXT_EXEC_DATE || ',' ||
              rec.CAL_HOL_EXCP || ',' || rec.COUNTERPARTY || ',' ||
              rec.LATEST_CYCLE_NO || rec.LATEST_CYCLE_DATE || ',' ||
              rec.CONTRACT_REF_NO || ',' || rec.SI_EXPIRY_DATE || ',' ||
              rec.DR_ACCOUNT || ',' || rec.DR_ACC_CCY || ',' || rec.SI_AMT || ',' ||
              rec.SI_AMT_CCY || ',' || rec.CR_ACCOUNT || ',' ||
              rec.CR_ACC_CCY || ',' || rec.ACCOUNT || ',' ||
              rec.ACCOUNT_TYPE || ',' || rec.CONTRACT_STATUS || ',' ||
              rec.BOOK_DATE;
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
END PR_11_STANDING_INSTRUCTIONS_CONTRACTS_V14;
/
/
