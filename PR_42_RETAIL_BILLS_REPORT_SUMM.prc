-- PROCEDURE PR_42_RETAIL_BILLS_REPORT_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_42_RETAIL_BILLS_REPORT_SUMM" (p_branch_code IN VARCHAR2,
                                                           p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '42_retail_bills_report_SUMM' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT_CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select V12.PRODUCT_CODE, V12.V12cnt, v14.V14cnt
                from (SELECT M.PRODUCT_CODE, count(M.CONTRACT_REF_NO) v12cnt
                        FROM ubsprod.RBtB_CONTRACT_MASTER@fcubsv12 M
                        LEFT OUTER JOIN ubsprod.rbtb_contract_parties@fcubsv12 CP
                          ON (M.CONTRACT_REF_NO = CP.CONTRACT_REF_NO AND
                             CP.PARTY_TYPE = 'DRAWER' AND
                             M.EVENT_SEQ_NO = CP.EVENT_SEQ_NO)
                        LEFT OUTER JOIN ubsprod.RBtM_INSTRUMENTS@fcubsv12 I
                          ON (M.INSTRUMENT_CODE = I.INSTRUMENT_CODE)
                        LEFT OUTER JOIN ubsprod.RBTM_OTHER_PARTY@fcubsv12 P
                          ON (M.OTHER_PARTY_ID = P.OTHR_PTY_ID)
                        LEFT OUTER JOIN ubsprod.CStB_CONTRACT@fcubsv12 C
                          ON (M.CONTRACT_REF_NO = C.CONTRACT_REF_NO AND
                             M.version_no = C.latest_version_no and
                             m.event_seq_no = c.latest_event_seq_no)
                        LEFT OUTER JOIN ubsprod.RBtM_OTHER_PARTY_CUSTOM@fcubsv12 PC
                          ON (M.OTHER_PARTY_ID = PC.OTHR_PTY_ID)
                        LEFT OUTER JOIN ubsprod.detm_clg_bank_code@fcubsv12 D
                          ON (D.clg_bank_code = M.coll_rem_bank_code)
                        LEFT OUTER JOIN ubsprod.detm_clg_brn_code@fcubsv12 E
                          ON (E.branch_code = M.coll_rem_branch_code and
                             E.bank_code = M.coll_rem_bank_code)
                       WHERE C.CONTRACT_STATUS = 'A'
                         AND C.AUTH_STATUS = 'A'
                         AND C.BRANCH = p_branch_code
                       GROUP BY M.PRODUCT_CODE) V12
              
                LEFT JOIN (SELECT M.PRODUCT_CODE,
                                 count(M.CONTRACT_REF_NO) v14cnt
                            FROM INTEGRATEDPP.RBzB_CONTRACT_MASTER M
                            LEFT OUTER JOIN INTEGRATEDPP.rbzb_contract_parties CP
                              ON (M.CONTRACT_REF_NO = CP.CONTRACT_REF_NO AND
                                 CP.PARTY_TYPE = 'DRAWER' AND
                                 M.EVENT_SEQ_NO = CP.EVENT_SEQ_NO)
                            LEFT OUTER JOIN INTEGRATEDPP.RBzM_INSTRUMENTS I
                              ON (M.INSTRUMENT_CODE = I.INSTRUMENT_CODE)
                            LEFT OUTER JOIN INTEGRATEDPP.RBzM_OTHER_PARTY P
                              ON (M.OTHER_PARTY_ID = P.OTHR_PTY_ID)
                            LEFT OUTER JOIN INTEGRATEDPP.CSzB_CONTRACT C
                              ON (M.CONTRACT_REF_NO = C.CONTRACT_REF_NO AND
                                 M.version_no = C.latest_version_no and
                                 m.event_seq_no = c.latest_event_seq_no)
                            LEFT OUTER JOIN INTEGRATEDPP.RBzM_OTHER_PARTY_CUSTOM PC
                              ON (M.OTHER_PARTY_ID = PC.OTHR_PTY_ID)
                            LEFT OUTER JOIN INTEGRATEDPP.dezm_clg_bank_code D
                              ON (D.clg_bank_code = M.coll_rem_bank_code)
                            LEFT OUTER JOIN INTEGRATEDPP.dezm_clg_brn_code E
                              ON (E.branch_code = M.coll_rem_branch_code and
                                 E.bank_code = M.coll_rem_bank_code)
                           WHERE C.CONTRACT_STATUS = 'A'
                             AND C.AUTH_STATUS = 'A'
                             AND C.BRANCH = p_branch_code
                           GROUP BY M.PRODUCT_CODE) V14
                  ON V12.PRODUCT_CODE = V14.PRODUCT_CODE) LOOP
    l_line := rec.PRODUCT_CODE || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END pr_42_retail_bills_report_SUMM;
/
/
