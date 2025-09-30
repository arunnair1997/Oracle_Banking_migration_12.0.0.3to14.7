-- PROCEDURE PROC_CHECK_ROW_COUNTS (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PROC_CHECK_ROW_COUNTS" IS

    CURSOR ctrl_cur IS
        SELECT * FROM mig_table_row_count_mock3
        WHERE status IS NULL OR status NOT IN ('SUCCESS')
        FOR UPDATE SKIP LOCKED;

    v_ctrl_rec             mig_table_row_count_mock3%ROWTYPE;
    v_total_count_v12      NUMBER;
    v_total_count_v14      NUMBER;
    v_qualified_count_v12  NUMBER;
    v_error_msg            VARCHAR2(4000);
    v_qualified_query      VARCHAR2(4000);

BEGIN
    OPEN ctrl_cur;
    LOOP
  BEGIN
        FETCH ctrl_cur INTO v_ctrl_rec;
        EXIT WHEN ctrl_cur%NOTFOUND;

        v_error_msg := NULL;
        v_total_count_v12 := NULL;
        v_total_count_v14 := NULL;
        v_qualified_count_v12 := NULL;
        v_qualified_query := NULL;

        -- 1. Total Count from remote Table V12
        BEGIN
            EXECUTE IMMEDIATE 
                'SELECT COUNT(*) FROM ' || v_ctrl_rec.table_name_v12 || '@' || v_ctrl_rec.db_link_name
                INTO v_total_count_v12;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := 'Error in Table V12 Count: ' || SQLERRM;
        END;

        -- 2. Total Count from local Table V14
        BEGIN
            EXECUTE IMMEDIATE 
                'SELECT COUNT(*) FROM ' || v_ctrl_rec.schema_name_v14 || '.' || v_ctrl_rec.table_name_v14
                INTO v_total_count_v14;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := COALESCE(v_error_msg || '; ', '') || 'Error in Table V14 Count: ' || SQLERRM;
        END;

        -- 3. Qualified Count from remote Table V12
        BEGIN
    IF v_ctrl_rec.where_condition IS NOT NULL THEN
            v_qualified_query := 
                'SELECT COUNT(*) FROM ' || v_ctrl_rec.table_name_v12 || '@' || v_ctrl_rec.db_link_name ||
                ' WHERE ' || v_ctrl_rec.where_condition;
    ELSE
    v_qualified_query := 
                'SELECT COUNT(*) FROM ' ||  v_ctrl_rec.table_name_v12 || '@' || v_ctrl_rec.db_link_name;
      END IF;

            EXECUTE IMMEDIATE v_qualified_query
                INTO v_qualified_count_v12;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := COALESCE(v_error_msg || '; ', '') || 'Error in Qualified Query: ' || SQLERRM;
        END;

        -- 4. Update control table
        UPDATE mig_table_row_count_mock3
        SET total_count_v12     = v_total_count_v12,
            total_count_v14     = v_total_count_v14,
            qualified_count_v12 = v_qualified_count_v12,
            error_message       = v_error_msg,
            qualified_query     = v_qualified_query,
            status              = CASE WHEN v_error_msg IS NULL THEN 'SUCCESS' ELSE 'FAILED' END
        WHERE CURRENT OF ctrl_cur;

        -- 5. Insert into log table
        INSERT INTO mig_table_row_count_log (
            row_id_processed,
            table_name_v12,
            table_name_v14,
            total_count_v12,
            total_count_v14,
            qualified_count_v12,
            status,
            error_message
        ) VALUES (
            v_ctrl_rec.row_id,
            v_ctrl_rec.table_name_v12,
            v_ctrl_rec.table_name_v14,
            v_total_count_v12,
            v_total_count_v14,
            v_qualified_count_v12,
            CASE WHEN v_error_msg IS NULL THEN 'SUCCESS' ELSE 'FAILED' END,
            v_error_msg
        );

        
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;
    END LOOP;
    CLOSE ctrl_cur;
    COMMIT;

END;
/
/
