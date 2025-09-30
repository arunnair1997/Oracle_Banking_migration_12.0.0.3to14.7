-- PROCEDURE PR_56_LIAB_UDF (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_56_LIAB_UDF" (p_branch_code IN VARCHAR2,

                                                 p_dir         IN VARCHAR2) IS

  l_file UTL_FILE.FILE_TYPE;

  l_line VARCHAR2(32767);

  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object

  l_filename VARCHAR2(200);

BEGIN

  -- Construct filename

  l_filename := '56_LIAB_UDF_' || p_branch_code || '.csv';

  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);

  dbms_output.put_line('CHECK1');

  -- Write header line

  l_line := 'LIAB_NO, V12_UDF1, V14_UDF1, V12_UDF2, V14_UDF2, V12_UDF3, V14_UDF3, V12_UDF4, V14_UDF4, V12_UDF5, V14_UDF5, V12_UDF6, V14_UDF6, V12_UDF7, V14_UDF7, V12_UDF8, V14_UDF8, V12_UDF9, V14_UDF9, V12_UDF10, V14_UDF10, V12_UDF11, V14_UDF11, V12_UDF12, V14_UDF12, V12_UDF13, V14_UDF13, V12_UDF14, V14_UDF14, V12_UDF15, V14_UDF15, V12_UDF16, V14_UDF16, V12_UDF17, V14_UDF17, V12_UDF18, V14_UDF18, V12_UDF19, V14_UDF19, V12_UDF20, V14_UDF20, V12_UDF21, V14_UDF21, V12_UDF22, V14_UDF22, V12_UDF23, V14_UDF23, V12_UDF24, V14_UDF24, V12_UDF25, V14_UDF25, V12_UDF26, V14_UDF26, V12_UDF27, V14_UDF27, V12_UDF28, V14_UDF28, V12_UDF29, V14_UDF29, V12_UDF30, V14_UDF30, V12_UDF31, V14_UDF31, V12_UDF32, V14_UDF32, V12_UDF33, V14_UDF33, V12_UDF34, V14_UDF34, V12_UDF35, V14_UDF35, V12_UDF36, V14_UDF36, V12_UDF37, V14_UDF37, V12_UDF38, V14_UDF38, V12_UDF39, V14_UDF39, V12_UDF40, V14_UDF40, V12_UDF41, V14_UDF41, V12_UDF42, V14_UDF42, V12_UDF43, V14_UDF43, V12_UDF44, V14_UDF44, V12_UDF45, V14_UDF45, V12_UDF46, V14_UDF46, V12_UDF47, V14_UDF47, V12_UDF48, V14_UDF48, V12_UDF49, V14_UDF49, V12_UDF50, V14_UDF50';

  UTL_FILE.PUT_LINE(l_file, l_line);

  dbms_output.put_line('CHECK2');

  -- Loop through query result and write lines

  FOR rec IN (

        SELECT /*+ PARALLEL(8) */

               v12.liab_no,

               v12.udf_value_1  AS v12_udf1,  v14.v14_udf1,

               v12.udf_value_2  AS v12_udf2,  v14.v14_udf2,

               v12.udf_value_3  AS v12_udf3,  v14.v14_udf3,

               v12.udf_value_4  AS v12_udf4,  v14.v14_udf4,

               v12.udf_value_5  AS v12_udf5,  v14.v14_udf5,

               v12.udf_value_6  AS v12_udf6,  v14.v14_udf6,

               v12.udf_value_7  AS v12_udf7,  v14.v14_udf7,

               v12.udf_value_8  AS v12_udf8,  v14.v14_udf8,

               v12.udf_value_9  AS v12_udf9,  v14.v14_udf9,

               v12.udf_value_10 AS v12_udf10, v14.v14_udf10,

               v12.udf_value_11 AS v12_udf11, v14.v14_udf11,

               v12.udf_value_12 AS v12_udf12, v14.v14_udf12,

               v12.udf_value_13 AS v12_udf13, v14.v14_udf13,

               v12.udf_value_14 AS v12_udf14, v14.v14_udf14,

               v12.udf_value_15 AS v12_udf15, v14.v14_udf15,

               v12.udf_value_16 AS v12_udf16, v14.v14_udf16,

               v12.udf_value_17 AS v12_udf17, v14.v14_udf17,

               v12.udf_value_18 AS v12_udf18, v14.v14_udf18,

               v12.udf_value_19 AS v12_udf19, v14.v14_udf19,

               v12.udf_value_20 AS v12_udf20, v14.v14_udf20,

               v12.udf_value_21 AS v12_udf21, v14.v14_udf21,

               v12.udf_value_22 AS v12_udf22, v14.v14_udf22,

               v12.udf_value_23 AS v12_udf23, v14.v14_udf23,

               v12.udf_value_24 AS v12_udf24, v14.v14_udf24,

               v12.udf_value_25 AS v12_udf25, v14.v14_udf25,

               v12.udf_value_26 AS v12_udf26, v14.v14_udf26,

               v12.udf_value_27 AS v12_udf27, v14.v14_udf27,

               v12.udf_value_28 AS v12_udf28, v14.v14_udf28,

               v12.udf_value_29 AS v12_udf29, v14.v14_udf29,

               v12.udf_value_30 AS v12_udf30, v14.v14_udf30,

               v12.udf_value_31 AS v12_udf31, v14.v14_udf31,

               v12.udf_value_32 AS v12_udf32, v14.v14_udf32,

               v12.udf_value_33 AS v12_udf33, v14.v14_udf33,

               v12.udf_value_34 AS v12_udf34, v14.v14_udf34,

               v12.udf_value_35 AS v12_udf35, v14.v14_udf35,

               v12.udf_value_36 AS v12_udf36, v14.v14_udf36,

               v12.udf_value_37 AS v12_udf37, v14.v14_udf37,

               v12.udf_value_38 AS v12_udf38, v14.v14_udf38,

               v12.udf_value_39 AS v12_udf39, v14.v14_udf39,

               v12.udf_value_40 AS v12_udf40, v14.v14_udf40,

               v12.udf_value_41 AS v12_udf41, v14.v14_udf41,

               v12.udf_value_42 AS v12_udf42, v14.v14_udf42,

               v12.udf_value_43 AS v12_udf43, v14.v14_udf43,

               v12.udf_value_44 AS v12_udf44, v14.v14_udf44,

               v12.udf_value_45 AS v12_udf45, v14.v14_udf45,

               v12.udf_value_46 AS v12_udf46, v14.v14_udf46,

               v12.udf_value_47 AS v12_udf47, v14.v14_udf47,

               v12.udf_value_48 AS v12_udf48, v14.v14_udf48,

               v12.udf_value_49 AS v12_udf49, v14.v14_udf49,

               v12.udf_value_50 AS v12_udf50, v14.v14_udf50

        FROM ubsprod.getm_liab@fcubsv12 v12

        LEFT JOIN (

            SELECT SUBSTR(f.rec_key,

                          INSTR(f.rec_key,'~',1,2)+1,

                          INSTR(f.rec_key,'~',1,3) - INSTR(f.rec_key,'~',1,2) - 1) AS liab_no,

                   a.liab_branch,

                   f.field_val_1  AS v14_udf1,

                   f.field_val_2  AS v14_udf2,

                   f.field_val_3  AS v14_udf3,

                   f.field_val_4  AS v14_udf4,

                   f.field_val_5  AS v14_udf5,

                   f.field_val_6  AS v14_udf6,

                   f.field_val_7  AS v14_udf7,

                   f.field_val_8  AS v14_udf8,

                   f.field_val_9  AS v14_udf9,

                   f.field_val_10 AS v14_udf10,

                   f.field_val_11 AS v14_udf11,

                   f.field_val_12 AS v14_udf12,

                   f.field_val_13 AS v14_udf13,

                   f.field_val_14 AS v14_udf14,

                   f.field_val_15 AS v14_udf15,

                   f.field_val_16 AS v14_udf16,

                   f.field_val_17 AS v14_udf17,

                   f.field_val_18 AS v14_udf18,

                   f.field_val_19 AS v14_udf19,

                   f.field_val_20 AS v14_udf20,

                   f.field_val_21 AS v14_udf21,

                   f.field_val_22 AS v14_udf22,

                   f.field_val_23 AS v14_udf23,

                   f.field_val_24 AS v14_udf24,

                   f.field_val_25 AS v14_udf25,

                   f.field_val_26 AS v14_udf26,

                   f.field_val_27 AS v14_udf27,

                   f.field_val_28 AS v14_udf28,

                   f.field_val_29 AS v14_udf29,

                   f.field_val_30 AS v14_udf30,

                   f.field_val_31 AS v14_udf31,

                   f.field_val_32 AS v14_udf32,

                   f.field_val_33 AS v14_udf33,

                   f.field_val_34 AS v14_udf34,

                   f.field_val_35 AS v14_udf35,

                   f.field_val_36 AS v14_udf36,

                   f.field_val_37 AS v14_udf37,

                   f.field_val_38 AS v14_udf38,

                   f.field_val_39 AS v14_udf39,

                   f.field_val_40 AS v14_udf40,

                   f.field_val_41 AS v14_udf41,

                   f.field_val_42 AS v14_udf42,

                   f.field_val_43 AS v14_udf43,

                   f.field_val_44 AS v14_udf44,

                   f.field_val_45 AS v14_udf45,

                   f.field_val_46 AS v14_udf46,

                   f.field_val_47 AS v14_udf47,

                   f.field_val_48 AS v14_udf48,

                   f.field_val_49 AS v14_udf49,

                   f.field_val_50 AS v14_udf50

            FROM integratedpp.cszm_function_userdef_fields f

            JOIN integratedpp.gezm_liab a

              ON a.liab_no = SUBSTR(f.rec_key,

                                    INSTR(f.rec_key,'~',1,2)+1,

                                    INSTR(f.rec_key,'~',1,3) - INSTR(f.rec_key,'~',1,2)-1)

             AND a.liab_branch = p_branch_code

            WHERE f.function_id = 'GEDMLIAB'

        ) v14

        ON v12.liab_no = v14.liab_no

       AND v12.liab_branch = v14.liab_branch

        WHERE v12.auth_stat = 'A'

          AND v12.record_stat = 'O'

          AND v12.liab_branch = p_branch_code

        ORDER BY v12.liab_branch, v12.liab_no
 

) LOOP

            l_line := rec.liab_no || ',' ||

                  rec.v12_udf1 || ',' || rec.v14_udf1 || ',' ||

                  rec.v12_udf2 || ',' || rec.v14_udf2 || ',' ||

                  rec.v12_udf3 || ',' || rec.v14_udf3 || ',' ||

                  rec.v12_udf4 || ',' || rec.v14_udf4 || ',' ||

                  rec.v12_udf5 || ',' || rec.v14_udf5 || ',' ||

                  rec.v12_udf6 || ',' || rec.v14_udf6 || ',' ||

                  rec.v12_udf7 || ',' || rec.v14_udf7 || ',' ||

                  rec.v12_udf8 || ',' || rec.v14_udf8 || ',' ||

                  rec.v12_udf9 || ',' || rec.v14_udf9 || ',' ||

                  rec.v12_udf10 || ',' || rec.v14_udf10 || ',' ||

                  rec.v12_udf11 || ',' || rec.v14_udf11 || ',' ||

                  rec.v12_udf12 || ',' || rec.v14_udf12 || ',' ||

                  rec.v12_udf13 || ',' || rec.v14_udf13 || ',' ||

                  rec.v12_udf14 || ',' || rec.v14_udf14 || ',' ||

                  rec.v12_udf15 || ',' || rec.v14_udf15 || ',' ||

                  rec.v12_udf16 || ',' || rec.v14_udf16 || ',' ||

                  rec.v12_udf17 || ',' || rec.v14_udf17 || ',' ||

                  rec.v12_udf18 || ',' || rec.v14_udf18 || ',' ||

                  rec.v12_udf19 || ',' || rec.v14_udf19 || ',' ||

                  rec.v12_udf20 || ',' || rec.v14_udf20 || ',' ||

                  rec.v12_udf21 || ',' || rec.v14_udf21 || ',' ||

                  rec.v12_udf22 || ',' || rec.v14_udf22 || ',' ||

                  rec.v12_udf23 || ',' || rec.v14_udf23 || ',' ||

                  rec.v12_udf24 || ',' || rec.v14_udf24 || ',' ||

                  rec.v12_udf25 || ',' || rec.v14_udf25 || ',' ||

                  rec.v12_udf26 || ',' || rec.v14_udf26 || ',' ||

                  rec.v12_udf27 || ',' || rec.v14_udf27 || ',' ||

                  rec.v12_udf28 || ',' || rec.v14_udf28 || ',' ||

                  rec.v12_udf29 || ',' || rec.v14_udf29 || ',' ||

                  rec.v12_udf30 || ',' || rec.v14_udf30 || ',' ||

                  rec.v12_udf31 || ',' || rec.v14_udf31 || ',' ||

                  rec.v12_udf32 || ',' || rec.v14_udf32 || ',' ||

                  rec.v12_udf33 || ',' || rec.v14_udf33 || ',' ||

                  rec.v12_udf34 || ',' || rec.v14_udf34 || ',' ||

                  rec.v12_udf35 || ',' || rec.v14_udf35 || ',' ||

                  rec.v12_udf36 || ',' || rec.v14_udf36 || ',' ||

                  rec.v12_udf37 || ',' || rec.v14_udf37 || ',' ||

                  rec.v12_udf38 || ',' || rec.v14_udf38 || ',' ||

                  rec.v12_udf39 || ',' || rec.v14_udf39 || ',' ||

                  rec.v12_udf40 || ',' || rec.v14_udf40 || ',' ||

                  rec.v12_udf41 || ',' || rec.v14_udf41 || ',' ||

                  rec.v12_udf42 || ',' || rec.v14_udf42 || ',' ||

                  rec.v12_udf43 || ',' || rec.v14_udf43 || ',' ||

                  rec.v12_udf44 || ',' || rec.v14_udf44 || ',' ||

                  rec.v12_udf45 || ',' || rec.v14_udf45 || ',' ||

                  rec.v12_udf46 || ',' || rec.v14_udf46 || ',' ||

                  rec.v12_udf47 || ',' || rec.v14_udf47 || ',' ||

                  rec.v12_udf48 || ',' || rec.v14_udf48 || ',' ||

                  rec.v12_udf49 || ',' || rec.v14_udf49 || ',' ||

                  rec.v12_udf50 || ',' || rec.v14_udf50;
 
 
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

END pr_56_LIAB_UDF;


/
/
