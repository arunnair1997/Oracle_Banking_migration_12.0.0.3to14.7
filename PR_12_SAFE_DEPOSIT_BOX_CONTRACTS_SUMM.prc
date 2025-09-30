-- PROCEDURE PR_12_SAFE_DEPOSIT_BOX_CONTRACTS_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_12_SAFE_DEPOSIT_BOX_CONTRACTS_SUMM" (p_branch_code IN VARCHAR2,
                                                                  p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '12_SAFE_DEPOSIT_BOX_CONTRACTS_SUMM' || p_branch_code ||
                '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT CODE,PRODUCT DESC, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select v12.product_code,
                     v12.product_description,
                     v12.v12cnt,
                     v14.v14cnt
                from (select M.PRODUCT_CODE,
                             CP.PRODUCT_DESCRIPTION,
                             count(M.CONTRACT_REF_NO) v12cnt
                        FROM UBSPROD.DLTB_CONTRACT_MASTER@fcubsv12 M
                        JOIN UBSPROD.CSTB_CONTRACT@fcubsv12 C
                          ON M.CONTRACT_REF_NO = C.CONTRACT_REF_NO
                         AND M.VERSION_NO = C.LATEST_VERSION_NO
                         AND M.EVENT_SEQ_NO = C.LATEST_EVENT_SEQ_NO
                        LEFT JOIN UBSPROD.DLTB_VISITOR_INFO@fcubsv12 V
                          ON V.CONTRACT_REF_NO = M.CONTRACT_REF_NO
                         AND V.EVENT_SEQ_NO = M.EVENT_SEQ_NO
                        LEFT JOIN UBSPROD.CSTM_PRODUCT@fcubsv12 CP
                          ON M.PRODUCT_CODE = CP.PRODUCT_CODE
                         AND CP.MODULE = 'DL'
                         AND CP.RECORD_STAT = 'O'
                         AND CP.ONCE_AUTH = 'Y'
                       WHERE C.CONTRACT_STATUS = 'A'
                         AND C.AUTH_STATUS = 'A'
                         AND M.SETTLEMENT_BRANCH = p_branch_code
                       group by M.PRODUCT_CODE, CP.PRODUCT_DESCRIPTION) V12
                LEFT JOIN (select M.PRODUCT_CODE,
                                 CP.PRODUCT_DESCRIPTION,
                                 count(M.CONTRACT_REF_NO) v14cnt
                            FROM INTEGRATEDPP.DLZB_CONTRACT_MASTER M
                            JOIN INTEGRATEDPP.CSZB_CONTRACT C
                              ON M.CONTRACT_REF_NO = C.CONTRACT_REF_NO
                             AND M.VERSION_NO = C.LATEST_VERSION_NO
                             AND M.EVENT_SEQ_NO = C.LATEST_EVENT_SEQ_NO
                            LEFT JOIN INTEGRATEDPP.DLZB_VISITOR_INFO V
                              ON V.CONTRACT_REF_NO = M.CONTRACT_REF_NO
                             AND V.EVENT_SEQ_NO = C.LATEST_EVENT_SEQ_NO
                             AND V.EVENT_SEQ_NO = M.EVENT_SEQ_NO
                            LEFT JOIN INTEGRATEDPP.CSZM_PRODUCT CP
                              ON M.PRODUCT_CODE = CP.PRODUCT_CODE
                             AND CP.MODULE = 'DL'
                             AND CP.RECORD_STAT = 'O'
                             AND CP.ONCE_AUTH = 'Y'
                           WHERE C.CONTRACT_STATUS = 'A'
                             AND C.AUTH_STATUS = 'A'
                             AND M.SETTLEMENT_BRANCH = p_branch_code
                           group by M.PRODUCT_CODE, CP.PRODUCT_DESCRIPTION) V14
                  ON V12.PRODUCT_CODE = V14.PRODUCT_CODE) LOOP
    l_line := rec.product_code || ',' || rec.product_description || ',' ||
              rec.v12cnt || ',' || rec.v14cnt;
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
END pr_12_SAFE_DEPOSIT_BOX_CONTRACTS_SUMM;
/
/
