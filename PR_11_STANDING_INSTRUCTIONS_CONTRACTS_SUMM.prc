-- PROCEDURE PR_11_STANDING_INSTRUCTIONS_CONTRACTS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_11_STANDING_INSTRUCTIONS_CONTRACTS_SUMM" (p_branch_code IN VARCHAR2,
                                                 p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '11_STANDING_INSTRUCTIONS_CONTRACTS_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (WITH instr_v12 AS
 (
  
  SELECT i.instruction_no,
          
          i.product_code,
          
          i.branch,
          
          ROW_NUMBER() OVER(PARTITION BY i.instruction_no ORDER BY i.inst_version_no DESC) rn
  
    FROM UBSPROD.SITB_INSTRUCTION@FCUBSV12 i
  
  ),

instr_v14 AS
 (
  
  SELECT i.instruction_no,
         
         i.product_code,
         
         i.branch,
         
         ROW_NUMBER() OVER(PARTITION BY i.instruction_no ORDER BY i.inst_version_no DESC) rn
  
    FROM INTEGRATEDPP.SIZB_INSTRUCTION i
  
  ),

accounts AS
 (
  
  SELECT AC_GL_NO
    FROM INTEGRATEDPP.STZB_ACCOUNT_GL
  
  UNION ALL
  
  SELECT AC_GL_NO
    FROM INTEGRATEDPP.STZB_ACCOUNT_CA
  
  )

SELECT v12.prd,
       
       v12.v12cnt,
       
       v14.v14cnt

  FROM (
        
        SELECT i.product_code prd, COUNT(M.CONTRACT_REF_NO) v12cnt
        
          FROM instr_v12 i
        
          JOIN UBSPROD.SITB_CONTRACT_MASTER@FCUBSV12 M
        
            ON i.instruction_no = M.instruction_no
        
          JOIN UBSPROD.CSTB_CONTRACT@FCUBSV12 C
        
            ON M.CONTRACT_REF_NO = C.CONTRACT_REF_NO
        
         WHERE i.rn = 1
              
           AND C.CONTRACT_STATUS = 'A'
              
           AND C.AUTH_STATUS = 'A'
              
           AND C.MODULE_CODE = 'SI'
              
           AND i.branch = p_branch_code
        
         GROUP BY i.product_code
        
        ) v12

  FULL JOIN (
             
             SELECT i.product_code prd, COUNT(M.CONTRACT_REF_NO) v14cnt
             
               FROM instr_v14 i
             
               JOIN INTEGRATEDPP.SIZB_CONTRACT_MASTER M
             
                 ON i.instruction_no = M.instruction_no
             
               JOIN INTEGRATEDPP.CSZB_CONTRACT C
             
                 ON M.CONTRACT_REF_NO = C.CONTRACT_REF_NO
             
              WHERE i.rn = 1
                   
                AND C.CONTRACT_STATUS = 'A'
                   
                AND C.AUTH_STATUS = 'A'
                   
                AND C.MODULE_CODE = 'SI'
                   
                AND i.branch = p_branch_code
             
              GROUP BY i.product_code
             
             ) v14

    ON v12.prd = v14.prd
)loop
    l_line := rec.prd || ',' || rec.v12cnt || ',' ||
              rec.v14cnt;
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
END pr_11_STANDING_INSTRUCTIONS_CONTRACTS_SUMM;
/
/
