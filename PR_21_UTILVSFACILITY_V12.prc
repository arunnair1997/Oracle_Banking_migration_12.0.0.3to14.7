-- PROCEDURE PR_21_UTILVSFACILITY_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_21_UTILVSFACILITY_V12" (p_dir IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '21_utilvsfacility_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'FAC_ID, LINE_ID, LIAB_ID, LIAB_NO, LINE_CURRENCY, LIMIT_AMT, EFF_LINE_AMT, REVOLVING, UTIL, EXP_UTIL, AVL_AMT, EXP_AVL_AMT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (SELECT u.facility_id AS fac_id,
                     f.line_code || f.line_serial AS line_id,
                     f.liab_id,
                     liab.liab_no,
                     f.line_currency,
                     f.limit_amount AS limit_amt,
                     DECODE(NVL(f.Lmt_Amt_Basis, 'L'),
                            'L',
                            f.limit_amount,
                            'M',
                            LEAST(f.Limit_Amount, f.Collateral_Contribution),
                            'A',
                            f.Limit_Amount + f.Collateral_Contribution) AS eff_line_amt,
                     NVL(f.revolving_line, 'N') AS revolving,
                     f.utilisation AS util,
                     DECODE(f.revolving_line,
                            'Y',
                            u.line_utils,
                            u.line_utils + u.matured_utils) AS exp_util,
                     f.available_amount AS avl_amt,
                     DECODE(NVL(f.Lmt_Amt_Basis, 'L'),
                            'L',
                            f.limit_amount,
                            'M',
                            LEAST(f.Limit_Amount, f.Collateral_Contribution),
                            'A',
                            f.Limit_Amount + f.Collateral_Contribution) +
                     NVL(f.Netting_Amount, 0) + NVL(f.Block_Amount, 0) -
                     DECODE(f.revolving_line,
                            'Y',
                            u.line_utils,
                            u.line_utils + u.matured_utils) +
                     NVL(f.Tanked_Util, 0) + NVL(f.Transfer_Amount, 0) AS exp_avl_amt
                FROM ubsprod.getm_facility@fcubsv12 f
                JOIN (SELECT utils.limit_id AS facility_id,
                            SUM(convert_currency_v12(utils.Util_Ccy,
                                                     line.LINE_CURRENCY,
                                                     utils.Util_Amt)) AS line_utils,
                            SUM(convert_currency_v12(utils.Util_Ccy,
                                                     line.LINE_CURRENCY,
                                                     utils.Matured_Amt)) AS matured_utils
                       FROM ubsprod.Getb_Utils@fcubsv12 utils
                       JOIN ubsprod.getm_facility@fcubsv12 line
                         ON NVL(utils.limit_id, 0) = line.id
                      WHERE utils.Util_Stat = 'A'
                        and utils.auth_stat = 'A'
                        AND utils.LIMIT_TYPE = 'F'
                        AND utils.limit_id IS NOT NULL
                      GROUP BY utils.limit_id) u
                  ON f.id = u.facility_id
                LEFT JOIN ubsprod.getm_liab@fcubsv12 liab
                  ON liab.id = f.liab_id) LOOP
    l_line := rec.FAC_ID || ',' || rec.LINE_ID || ',' || rec.LIAB_ID || ',' ||
              rec.LIAB_NO || ',' || rec.LINE_CURRENCY || ',' ||
              rec.LIMIT_AMT || ',' || rec.EFF_LINE_AMT || ',' ||
              rec.REVOLVING || ',' || rec.UTIL || ',' || rec.EXP_UTIL || ',' ||
              rec.AVL_AMT || ',' || rec.EXP_AVL_AMT;
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
END pr_21_utilvsfacility_v12;
/
/
