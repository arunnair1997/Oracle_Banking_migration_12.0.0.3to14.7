-- PACKAGE DATA_VALIDATION_PKG (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PACKAGE "ARUNN_ADMIN"."DATA_VALIDATION_PKG" AS
 
    /**
     * Executes a single validation test case. This version gets the full query
     * count (no sample records) and uses a revised, more detailed status logic.
     * @param p_test_case_id The user-defined string ID of the test case to execute.
     */
    PROCEDURE EXECUTE_SINGLE_TEST_CASE(p_test_case_id IN DATA_VALIDATION_CONTROL.TEST_CASE_ID%TYPE);
 
    /**
     * The main orchestrator procedure. Loops through pending tests and submits them
     * for parallel execution via DBMS_JOB.
     */
    PROCEDURE MAIN_VALIDATION_PROCESS;
 
END DATA_VALIDATION_PKG;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ARUNN_ADMIN"."DATA_VALIDATION_PKG" AS
 
    PROCEDURE busy_wait(p_seconds IN NUMBER);
 
    -- =========================================================================
    -- PUBLIC: Procedure to execute a single test case.
    -- =========================================================================
    PROCEDURE EXECUTE_SINGLE_TEST_CASE(p_test_case_id IN DATA_VALIDATION_CONTROL.TEST_CASE_ID%TYPE) IS
        v_rec           DATA_VALIDATION_CONTROL%ROWTYPE;
        v_full_count_a  NUMBER := 0;
        v_full_count_b  NUMBER := 0;
        v_error_a       CLOB;
        v_error_b       CLOB;
        v_final_status  DATA_VALIDATION_CONTROL.STATUS%TYPE;
        v_start_time    TIMESTAMP := SYSTIMESTAMP;
        v_end_time      TIMESTAMP;
        v_sql           CLOB;
        v_debug_step    VARCHAR2(200 CHAR) := 'Starting execution';
    BEGIN
        v_debug_step := 'Fetching control record';
        SELECT * INTO v_rec FROM DATA_VALIDATION_CONTROL WHERE TEST_CASE_ID = p_test_case_id;
 
        v_debug_step := 'Updating status to RUNNING';
        UPDATE DATA_VALIDATION_CONTROL SET STATUS = 'RUNNING', START_TIME = v_start_time WHERE TEST_CASE_ID = p_test_case_id;
        COMMIT;
 
        --  Execute Schema A Query
        IF v_rec.SCHEMA_A_QUERY IS NOT NULL AND v_rec.SCHEMA_A_QUERY != ' ' THEN
            BEGIN
                v_debug_step := 'Executing COUNT for Query A';
                v_sql := 'SELECT COUNT(1) FROM (' || v_rec.SCHEMA_A_QUERY || ')';
                EXECUTE IMMEDIATE v_sql INTO v_full_count_a;
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_a := 'Query Error: ' || SQLERRM;
            END;
        END IF;
 
        --  Execute Schema B Query
        IF v_rec.SCHEMA_B_QUERY IS NOT NULL AND v_rec.SCHEMA_B_QUERY != ' ' THEN
            BEGIN
                v_debug_step := 'Altering session for Query B';
                EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA = INTEGRATEDPP';
 
                v_debug_step := 'Executing COUNT for Query B';
                v_sql := 'SELECT COUNT(1) FROM (' || v_rec.SCHEMA_B_QUERY || ')';
                EXECUTE IMMEDIATE v_sql INTO v_full_count_b;
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_b := 'Query Error: ' || SQLERRM;
            END;
        END IF;
 
        v_debug_step := 'Determining final status';
        v_end_time := SYSTIMESTAMP;
 
        -- Revised Status Logic
        IF v_error_a IS NOT NULL AND v_error_b IS NOT NULL THEN
            v_final_status := 'Error in Query A and Query B';
        ELSIF v_error_a IS NOT NULL THEN
            v_final_status := 'Error in Query A';
        ELSIF v_error_b IS NOT NULL THEN
            v_final_status := 'Error in Query B';
        ELSIF v_full_count_a > 0 AND v_full_count_b > 0 THEN
            v_final_status := 'FAILED for BOTH';
        ELSIF v_full_count_a > 0 THEN
            v_final_status := 'FAILED for Query A';
        ELSIF v_full_count_b > 0 THEN
            v_final_status := 'FAILED for Query B';
        ELSE
            v_final_status := 'PASSED';
        END IF;
       
        -- *** MODIFIED: Added a separate BEGIN/END block for the final update to improve diagnostics ***
        BEGIN
            v_debug_step := 'Updating final results';
            UPDATE DATA_VALIDATION_CONTROL
            SET COUNT_A = v_full_count_a,
                COUNT_B = v_full_count_b,
                ERROR_A = v_error_a,
                ERROR_B = v_error_b,
                STATUS = v_final_status,
                END_TIME = v_end_time,
                UPDATED_AT = SYSTIMESTAMP
            WHERE TEST_CASE_ID = p_test_case_id;
        EXCEPTION
            WHEN OTHERS THEN
                 -- This will now only catch errors from the UPDATE statement itself.
                 v_error_a := 'Final Update Error: ' || SQLERRM;
                 v_final_status := 'ERROR';
                 UPDATE DATA_VALIDATION_CONTROL
                 SET ERROR_A = v_error_a, STATUS = v_final_status
                 WHERE TEST_CASE_ID = p_test_case_id;
        END;
       
        COMMIT;
 
    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_framework_error_msg CLOB;
            BEGIN
                v_framework_error_msg := 'Execution framework error at step [' || v_debug_step || ']: ' || SQLERRM;
                UPDATE DATA_VALIDATION_CONTROL
                SET STATUS = 'ERROR', ERROR_A = v_framework_error_msg, END_TIME = SYSTIMESTAMP, UPDATED_AT = SYSTIMESTAMP
                WHERE TEST_CASE_ID = p_test_case_id;
                COMMIT;
            END;
    END EXECUTE_SINGLE_TEST_CASE;
 
    PROCEDURE busy_wait(p_seconds IN NUMBER) IS
        v_start_time TIMESTAMP;
    BEGIN
        v_start_time := SYSTIMESTAMP;
        WHILE SYSTIMESTAMP < v_start_time + NUMTODSINTERVAL(p_seconds, 'SECOND') LOOP NULL; END LOOP;
    END busy_wait;
 
    PROCEDURE MAIN_VALIDATION_PROCESS IS
        v_parallelism_limit   CONSTANT NUMBER := 5;
        v_wait_seconds        CONSTANT NUMBER := 3;
        v_running_jobs        NUMBER;
        v_pending_tests_exist BOOLEAN := TRUE;
        v_job_id              BINARY_INTEGER;
        CURSOR c_pending_tests IS
            SELECT TEST_CASE_ID FROM DATA_VALIDATION_CONTROL WHERE STATUS = 'PENDING' FOR UPDATE;
    BEGIN
        WHILE v_pending_tests_exist LOOP
            SELECT COUNT(*) INTO v_running_jobs FROM DATA_VALIDATION_CONTROL WHERE STATUS = 'RUNNING';
            IF v_running_jobs < v_parallelism_limit THEN
                FOR rec IN c_pending_tests LOOP
                    DBMS_JOB.SUBMIT(
                        job => v_job_id,
                        what => 'BEGIN DATA_VALIDATION_PKG.EXECUTE_SINGLE_TEST_CASE(''' || rec.TEST_CASE_ID || '''); END;',
                        next_date => SYSDATE
                    );
                    UPDATE DATA_VALIDATION_CONTROL
                    SET STATUS = 'RUNNING', JOB_ID = v_job_id, UPDATED_AT = SYSTIMESTAMP
                    WHERE TEST_CASE_ID = rec.TEST_CASE_ID;
                    COMMIT;
                    EXIT;
                END LOOP;
            END IF;
            SELECT COUNT(*) INTO v_running_jobs FROM DATA_VALIDATION_CONTROL WHERE STATUS IN ('PENDING', 'RUNNING');
            IF v_running_jobs = 0 THEN v_pending_tests_exist := FALSE;
            ELSE busy_wait(v_wait_seconds);
            END IF;
        END LOOP;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in MAIN_VALIDATION_PROCESS: ' || SQLERRM);
            ROLLBACK;
            RAISE;
    END MAIN_VALIDATION_PROCESS;
END DATA_VALIDATION_PKG;
/
/
