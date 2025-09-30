-- PROCEDURE PR_29_MEMO_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_29_MEMO_V12" (p_branch_code IN VARCHAR2,
                                           p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '29_Memo_' || p_branch_code || '_v12.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'CUSTOMER_NO, MEMO_ID, MEMO_DETAIL_ID, DESCRIPTION, INST_ID, INST_DATE, INST_EXPR_DATE, DISPLAY_TYPE, CHANNEL, LANGUAGE, MESSAGE, SHOW_TO_CUSTOMER, USER_MESSAGE, CREDIT_ADMIN_MEMO';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (select cid.customer_no,
       cid.memo_id,
       cid.memo_detail_id,
       cim.description,
       cid.inst_id,
       cid.inst_date,
       cid.inst_expr_date,
       cid.display_type,
       cid.channel,
       cid.language,
       cid.message,
       cid.show_to_customer,
       cid.user_message,
       cidm.CREDIT_ADMIN_MEMO
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
 order by cid.customer_no, cid.memo_id, cid.inst_id) LOOP
    l_line := rec.CUSTOMER_NO || ',' || rec.MEMO_ID || ',' ||
              rec.MEMO_DETAIL_ID || ',' || rec.DESCRIPTION || ',' ||
              rec.INST_ID || ',' || rec.INST_DATE || ',' ||
              rec.INST_EXPR_DATE || ',' || rec.DISPLAY_TYPE || ',' ||
              rec.CHANNEL || ',' || rec.LANGUAGE || ',' || rec.MESSAGE || ',' ||
              rec.SHOW_TO_CUSTOMER || ',' || rec.USER_MESSAGE || ',' ||
              rec.CREDIT_ADMIN_MEMO;
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
END pr_29_Memo_v12;
/
/
