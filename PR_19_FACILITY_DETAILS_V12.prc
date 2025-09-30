-- PROCEDURE PR_19_FACILITY_DETAILS_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_19_FACILITY_DETAILS_V12" (p_branch_code IN VARCHAR2,
                                                    p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '19_Facility_Details_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'liab_no, line_code, line_serial, main_line_id,  line_currency, revolving_line ,availability_flag,funded,line_start_date,line_expiry_date,limit_amount,available_amount,utilisation,effective line amt,lmt_amt_basis ';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select liab.liab_no,
                     fac.line_code,
                     fac.line_serial,
                     fac.main_line_id,
                     fac.line_currency,
                     fac.revolving_line,
                     fac.availability_flag,
                     fac.funded,
                     fac.line_start_date,
                     fac.line_expiry_date,
                     fac.limit_amount,
                     fac.available_amount,
                     fac.utilisation,
                     fac.dsp_eff_line_amount,
                     fac.lmt_amt_basis
                from ubsprod.getm_facility@fcubsv12 fac,
                     ubsprod.getm_liab@fcubsv12     liab
               where fac.auth_stat = 'A'
                 and fac.record_stat = 'O'
                 and fac.liab_id = liab.id
                 and liab.liab_branch = p_branch_code) LOOP
    l_line := rec.liab_no || ',' || rec.line_code || ',' || rec.line_serial || ',' ||
              rec.main_line_id || ',' || rec.line_currency ||',' ||
              rec.revolving_line ||',' || rec.availability_flag ||',' ||
              rec.funded ||',' || rec.line_start_date ||
              rec.line_expiry_date ||',' || rec.limit_amount ||',' ||
              rec.available_amount ||',' || rec.utilisation ||',' ||
              rec.dsp_eff_line_amount ||',' || rec.lmt_amt_basis;
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
END pr_19_Facility_Details_v12;
/
/
