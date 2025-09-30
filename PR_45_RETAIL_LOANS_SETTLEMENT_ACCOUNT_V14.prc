-- PROCEDURE PR_45_RETAIL_LOANS_SETTLEMENT_ACCOUNT_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_45_RETAIL_LOANS_SETTLEMENT_ACCOUNT_V14" (p_branch_code IN VARCHAR2,
                                                                      p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '45_RETAIL_LOANS_SETTLEMENT_ACCOUNT_' || p_branch_code ||
                '_V14.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'ACCOUNT_NUMBER, BRANCH_CODE, COMPONENT_NAME, DR_PROD_AC, CR_PROD_AC';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT A.ACCOUNT_NUMBER,
                     A.BRANCH_CODE,
                     A.COMPONENT_NAME,
                     A.DR_PROD_AC,
                     A.CR_PROD_AC
                FROM INTEGRATEDPP.CLZB_ACCOUNT_COMPONENTS  A,
                     INTEGRATEDPP.CLZB_ACCOUNT_APPS_MASTER B
               WHERE A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                 AND A.BRANCH_CODE = p_branch_code
                 AND A.BRANCH_CODE = B.BRANCH_CODE
                 AND B.ACCOUNT_STATUS = 'A'
                 AND B.MODULE_CODE = 'CL'
               GROUP BY A.ACCOUNT_NUMBER,
                        A.BRANCH_CODE,
                        A.COMPONENT_NAME,
                        A.DR_PROD_AC,
                        A.CR_PROD_AC
               ORDER BY A.ACCOUNT_NUMBER) LOOP
    l_line := rec.ACCOUNT_NUMBER || ',' || rec.BRANCH_CODE || ',' ||
              rec.COMPONENT_NAME || ',' || rec.DR_PROD_AC || ',' ||
              rec.CR_PROD_AC;
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
END PR_45_RETAIL_LOANS_SETTLEMENT_ACCOUNT_V14;
/
/
