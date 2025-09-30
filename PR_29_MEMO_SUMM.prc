-- PROCEDURE PR_29_MEMO_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_29_MEMO_SUMM" (p_branch_code IN VARCHAR2,
                                            p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '29_Memo_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PRODUCT CODE, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select v12.customer_no, v12.v12cnt, v14.v14cnt
                from (select cid.customer_no, count(cid.memo_id) v12cnt
                        from ubsprod.CSTM_INST_MASTER@fcubsv12        cim,
                             ubsprod.CSTM_INST_DETAIL@fcubsv12        cid,
                             ubsprod.CSTM_INST_DETAIL_CUSTOM@fcubsv12 cidm
                       where cim.customer_no = cid.customer_no
                         and cidm.customer_no = cim.customer_no
                         and cid.customer_no = cidm.customer_no
                         and cim.memo_id = cid.memo_id
                         and cim.memo_id = cidm.memo_id
                         and cid.memo_id = cim.memo_id
                         and cid.inst_id = cidm.inst_id
                         and cim.record_stat = 'O'
                         and cim.auth_stat = 'A'
                         and cim.BRANCH_CODE = p_branch_code
                       group by cid.customer_no) V12
                LEFT JOIN
              
               (select cid.customer_no, count(cid.memo_id) v14cnt
                 from integratedpp.CSZM_INST_MASTER        cim,
                      integratedpp.CSZM_INST_DETAIL        cid,
                      integratedpp.CSZM_INST_DETAIL_CUSTOM cidm
                where cim.customer_no = cid.customer_no
                  and cidm.customer_no = cim.customer_no
                  and cid.customer_no = cidm.customer_no
                  and cim.memo_id = cid.memo_id
                  and cim.memo_id = cidm.memo_id
                  and cid.memo_id = cim.memo_id
                  and cid.inst_id = cidm.inst_id
                  and cim.record_stat = 'O'
                  and cim.auth_stat = 'A'
                  and cim.BRANCH_CODE = p_branch_code
                group by cid.customer_no) V14
              
                  ON v12.customer_no = v14.customer_no) LOOP
    l_line := rec.customer_no || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END pr_29_Memo_SUMM;
/
/
