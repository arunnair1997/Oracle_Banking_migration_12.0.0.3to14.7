-- PROCEDURE PR_RUN_REPORT_FOR_ALL_BRANCHES (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_RUN_REPORT_FOR_ALL_BRANCHES" (
    p_proc_name IN VARCHAR2
) IS
p_dir varchar2(50) :='RECON';
BEGIN
  FOR branch_rec IN (
    SELECT branch_code FROM recon_branch_master WHERE branch_code IS NOT NULL
  ) LOOP
    BEGIN
      -- Build and execute the dynamic report procedure call
      EXECUTE IMMEDIATE 'BEGIN ' || p_proc_name || '(:1, :2); END;'
        USING branch_rec.branch_code, p_dir;
 
      DBMS_OUTPUT.PUT_LINE('Completed for ' || branch_rec.branch_code);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('BOMBED IN LOOP for ' || branch_rec.branch_code || ': ' || SQLERRM);
    END;
  END LOOP;
 
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('STUB failed: ' || SQLERRM);
END;
/
/
