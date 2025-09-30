-- PACKAGE BODY PKG_ROW_COUNT_CHECKER (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ARUNN_ADMIN"."PKG_ROW_COUNT_CHECKER" AS
 
    -- A simple busy-wait procedure to pause execution.
    PROCEDURE busy_wait(p_seconds IN NUMBER) IS
        v_start_time TIMESTAMP;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        WHILE SYSTIMESTAMP < v_start_time + NUMTODSINTERVAL(p_seconds, 'SECOND') LOOP
            NULL; -- Actively consume CPU to wait.
        END LOOP;
    END busy_wait;
 
    -- =========================================================================
    -- WORKER PROCEDURE: Processes one row from the control table.
    -- =========================================================================
    PROCEDURE EXECUTE_SINGLE_ROW(p_row_id IN ROWID) IS
        v_ctrl_rec              MIG_TABLE_ROW_COUNT_MOCK3%ROWTYPE;
        v_total_count_a         NUMBER;
        v_total_count_b         NUMBER;
        v_qualified_count_a     NUMBER;
        v_error_a               CLOB;
        v_error_b               CLOB;
        v_error_qualified       CLOB;
        v_final_status          VARCHAR2(50);
        v_qualified_query_text  CLOB;
        v_sql                   CLOB;
    BEGIN
        -- Lock and retrieve the specific row to be processed
        SELECT * INTO v_ctrl_rec FROM MIG_TABLE_ROW_COUNT_MOCK3 WHERE ROWID = p_row_id FOR UPDATE;
 
        -- 1. Total Count from Schema A (Remote)
        BEGIN
            v_sql := 'SELECT COUNT(1) FROM ' || v_ctrl_rec.TABLE_NAME_A || '@' || v_ctrl_rec.DB_LINK_NAME;
            EXECUTE IMMEDIATE v_sql INTO v_total_count_a;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_a := SQLERRM;
        END;
 
        -- 2. Total Count from Schema B (Local)
        BEGIN
            -- This assumes Schema C has direct select grants on Schema B's tables
            v_sql := 'SELECT COUNT(1) FROM ' || v_ctrl_rec.SCHEMA_NAME_B || '.' || v_ctrl_rec.TABLE_NAME_B;
            EXECUTE IMMEDIATE v_sql INTO v_total_count_b;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_b := SQLERRM;
        END;
 
        -- 3. Qualified Count from Schema A (Remote)
        BEGIN
            -- First, construct the full query for logging purposes
            v_qualified_query_text := 'SELECT * FROM ' || v_ctrl_rec.TABLE_NAME_A || '@' || v_ctrl_rec.DB_LINK_NAME;
            IF v_ctrl_rec.WHERE_CONDITION IS NOT NULL THEN
                v_qualified_query_text := v_qualified_query_text || ' WHERE ' || v_ctrl_rec.WHERE_CONDITION;
            END IF;
 
            -- Then, wrap it in a COUNT(1) for execution
            v_sql := 'SELECT COUNT(1) FROM (' || v_qualified_query_text || ')';
            EXECUTE IMMEDIATE v_sql INTO v_qualified_count_a;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_qualified := SQLERRM;
        END;
 
        -- 4. Determine the final status based on the results
        IF v_error_a IS NOT NULL THEN
            v_final_status := 'Error in query for Table A';
        ELSIF v_error_b IS NOT NULL THEN
            v_final_status := 'Error in query for Table B';
        ELSIF v_error_qualified IS NOT NULL THEN
            v_final_status := 'Error in qualified query for Table A';
        ELSE
            v_final_status := 'SUCCESS'; -- Or your preferred success status
        END IF;
 
        -- 5. Update the control table with all results
        UPDATE MIG_TABLE_ROW_COUNT_MOCK3
        SET
            TOTAL_COUNT_A       = v_total_count_a,
            TOTAL_COUNT_B       = v_total_count_b,
            QUALIFIED_COUNT_A   = v_qualified_count_a,
            QUALIFIED_QUERY     = v_qualified_query_text,
            ERROR_A             = v_error_a,
            ERROR_B             = v_error_b,
            ERROR_QUALIFIED     = v_error_qualified,
            STATUS              = v_final_status,
            UPDATED_AT          = SYSTIMESTAMP
        WHERE
            ROWID = p_row_id;
       
        COMMIT;
 
    EXCEPTION
        WHEN OTHERS THEN
          v_error_qualified := SQLERRM;
            -- Catch any unexpected errors in this procedure itself
            UPDATE MIG_TABLE_ROW_COUNT_MOCK3
            SET STATUS = 'FATAL FRAMEWORK ERROR', ERROR_A = v_error_qualified, UPDATED_AT = SYSTIMESTAMP
            WHERE ROWID = p_row_id;
            COMMIT;
    END EXECUTE_SINGLE_ROW;
 
    -- =========================================================================
    -- CONTROLLER PROCEDURE: Manages the parallel job submission.
    -- =========================================================================
    PROCEDURE MAIN_CONTROLLER IS
        v_parallelism_limit   CONSTANT NUMBER := 5;
        v_wait_seconds        CONSTANT NUMBER := 15;
        v_running_jobs        NUMBER;
        v_pending_rows_exist  BOOLEAN := TRUE;
        v_job_id              BINARY_INTEGER;
       
        CURSOR c_pending_rows IS
            SELECT rowid as row_identifier
            FROM MIG_TABLE_ROW_COUNT_MOCK3
            WHERE STATUS = 'PENDING'
            FOR UPDATE SKIP LOCKED;
    BEGIN
        WHILE v_pending_rows_exist LOOP
            -- Check how many jobs are currently marked as running
            SELECT COUNT(*) INTO v_running_jobs FROM MIG_TABLE_ROW_COUNT_MOCK3 WHERE STATUS = 'RUNNING';
 
            -- If we have capacity, submit new jobs
            IF v_running_jobs < v_parallelism_limit THEN
                FOR rec IN c_pending_rows LOOP
                    -- Submit a new job to process the fetched row
                    DBMS_JOB.SUBMIT(
                        job       => v_job_id,
                        what      => 'BEGIN PKG_ROW_COUNT_CHECKER.EXECUTE_SINGLE_ROW(p_row_id => CHARTOROWID(''' || rec.row_identifier || ''')); END;',
                        next_date => SYSDATE
                    );
 
                    -- Mark the row as 'RUNNING' and store the job ID
                    UPDATE MIG_TABLE_ROW_COUNT_MOCK3
                    SET STATUS = 'RUNNING', JOB_ID = v_job_id, UPDATED_AT = SYSTIMESTAMP
                    WHERE ROWID = rec.row_identifier;
 
                    COMMIT;
                   
                    -- Only submit one job per loop iteration to re-check counts
                    EXIT;
                END LOOP;
            END IF;
 
            -- Check if there are any pending or running tasks left
            SELECT COUNT(*) INTO v_running_jobs FROM MIG_TABLE_ROW_COUNT_MOCK3 WHERE STATUS IN ('PENDING', 'RUNNING');
 
            IF v_running_jobs = 0 THEN
                v_pending_rows_exist := FALSE;
            ELSE
                -- Wait for 15 seconds before the next check
                busy_wait(v_wait_seconds);
            END IF;
        END LOOP;
       
        DBMS_OUTPUT.PUT_LINE('All rows processed.');
        COMMIT;
 
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in MAIN_CONTROLLER: ' || SQLERRM);
            ROLLBACK;
            RAISE;
    END MAIN_CONTROLLER;
 
END PKG_ROW_COUNT_CHECKER;
/
/
