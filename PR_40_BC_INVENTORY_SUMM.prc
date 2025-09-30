-- PROCEDURE PR_40_BC_INVENTORY_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_40_BC_INVENTORY_SUMM" (p_branch_code IN VARCHAR2,

                                                           p_dir         IN VARCHAR2) IS

  l_file UTL_FILE.FILE_TYPE;

  l_line VARCHAR2(32767);

  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object

  l_filename VARCHAR2(200);

BEGIN

  -- Construct filename

  l_filename := 'PR_40_BC_INVENTORY_SUMM_' || p_branch_code || '_SUMM.csv';

  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);

  dbms_output.put_line('CHECK1');

  -- Write header line

  l_line := 'V12 INSTR TYPE CODE, V14 INSTR TYPE CODE, V12_Count, V14_Count';

  UTL_FILE.PUT_LINE(l_file, l_line);

  dbms_output.put_line('CHECK2');

  -- Loop through query result and write lines

  FOR rec IN (WITH v12 AS (
    SELECT CASE 
             WHEN SUBSTR(instr_type,1,2) = 'TT' THEN 'RO'
             WHEN SUBSTR(instr_type,1,2) = 'DD' THEN 'DD'
             WHEN SUBSTR(instr_type,1,2) = 'BC' THEN 'MC'
           END AS V14_INSTR_TYPE_CODE,
           SUBSTR(instr_type,1,2) AS V12_INSTR_TYPE_CODE,
           COUNT(*) AS V12_Count
      FROM ubsprod.istm_instr_txn@fcubsv12
    WHERE ISSUING_BRANCH = p_branch_code and auth_stat='A' and record_stat='O'
     GROUP BY SUBSTR(instr_type,1,2)
),
v14 AS (
    SELECT SUBSTR(instrument_type,1,2) AS V14_INSTR_TYPE_CODE,
           COUNT(*) AS V14_Count
      FROM integratedpp.PIZB_INS_ISSUE
    WHERE TXN_BRANCH = p_branch_code
     GROUP BY SUBSTR(instrument_type,1,2)
)
SELECT v12.V12_INSTR_TYPE_CODE   AS "V12_INSTR_TYPE_CODE",
       v12.V14_INSTR_TYPE_CODE   AS "V14_INSTR_TYPE_CODE",
       v12.v12_count  AS V12_COUNT,
       NVL(v14.v14_count,0) AS V14_COUNT
  FROM v12
  LEFT JOIN v14 ON v12.V14_INSTR_TYPE_CODE = v14.V14_INSTR_TYPE_CODE) LOOP
    l_line := rec.V12_INSTR_TYPE_CODE || ',' || rec.V14_INSTR_TYPE_CODE || ',' ||
              rec.V12_Count || ',' || rec.V14_Count;
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
END PR_40_BC_INVENTORY_SUMM;
/
/
