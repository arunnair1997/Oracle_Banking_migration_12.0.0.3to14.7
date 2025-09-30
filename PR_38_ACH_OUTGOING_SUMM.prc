-- PROCEDURE PR_38_ACH_OUTGOING_SUMM (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PR_38_ACH_OUTGOING_SUMM" (p_branch_code IN VARCHAR2,
                                                        p_dir         IN VARCHAR2) IS
  l_file UTL_FILE.FILE_TYPE;
  l_line VARCHAR2(32767);
  --p_dir CONSTANT VARCHAR2(100) := 'YOUR_DIR'; -- replace with your Oracle directory object
  l_filename VARCHAR2(200);
BEGIN
  -- Construct filename
  l_filename := '38_ACH_OUTGOING_SUMM_' || p_branch_code || '.csv';
  l_file     := UTL_FILE.FOPEN(p_dir, l_filename, 'W', 32767);
  dbms_output.put_line('CHECK1');
  -- Write header line
  l_line := 'PROD REF, V12 COUNT, V14 COUNT';
  UTL_FILE.PUT_LINE(l_file, l_line);
  dbms_output.put_line('CHECK2');
  -- Loop through query result and write lines
  FOR rec IN (
select substr(V12.AC_ENTRY_REF_NO, 4, 4) AS SOURCE_REF,
       count(V12.CONTRACT_REF_NO) v12cnt,
       count(V14.SOURCE_REF_NO) v14cnt
  from (select CONTRACT_REF_NO,
               BRANCH_CODE,
               SOURCE_CODE,
               NETWORK,
               CUST_NO,
               CUST_AC_NO,
               TXN_AMOUNT,
               CPTY_AC_NO,
               CPTY_NAME,
               BOOKING_DT,
               ACTIVATION_DT,
               CONTRACT_STATUS,
               CUST_NAME,
               TXN_CCY,
               INSTRUCTION_DATE,
               AC_ENTRY_REF_NO
          from pctb_contract_master@fcubsv12
         where activation_dt > (select today
                                  from sttm_dates@fcubsv12
                                 where branch_code = '100')
           and product_code in ('ACH4',
                                'PFWA',
                                'PFWA',
                                'ACH3',
                                'IRSA',
                                'CORP',
                                'COLT',
                                'COLW',
                                'PCAW',
                                'PCAW',
                                'IRSD',
                                'RBOP',
                                'BACH',
                                'PFAS',
                                'PCAA',
                                'PCAA',
                                'UUBI',
                                'RTPN',
                                'UACC',
                                'BRTP',
                                'MDCL',
                                'DACH',
                                'PCAS',
                                'PCAS',
                                'UMSV',
                                'EACC',
                                'PFCA',
                                'PFCA',
                                'UTP2',
                                'USUP',
                                'USER',
                                'USAL',
                                'ETP2',
                                'EMSV',
                                'UEDU',
                                'EEDU',
                                'ESER',
                                'EUBI',
                                'UINS')
           and product_type = 'O'
           and branch_code = p_branch_code) V12

  LEFT OUTER JOIN (SELECT A.TXN_REF_NO,
                          A.SOURCE_REF_NO,
                          A.TXN_BRANCH,
                          A.SOURCE_CODE,
                          A.NETWORK_CODE,
                          A.CUSTOMER_NO,
                          A.DR_AC_NO,
                          A.TRANSFER_AMT,
                          A.CR_AC_NO,
                          A.CR_NAME,
                          A.TXN_BOOKING_DATE,
                          A.ACTIVATION_DATE,
                          A.TXN_STATUS,
                          A.DR_NAME,
                          A.TRANSFER_CCY,
                          A.INSTRUCTION_DATE
                     FROM integratedpp.PYZB_OUT_TXN_DRIVER A
                    WHERE A.TXN_BRANCH = p_branch_code) V14

    ON substr(V12.AC_ENTRY_REF_NO, 4, 4) = substr(V14.SOURCE_REF_NO, 4, 4)
 group by substr(V12.AC_ENTRY_REF_NO, 4, 4)) LOOP
    l_line := rec.SOURCE_REF || ',' || rec.v12cnt || ',' || rec.v14cnt;
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
END pr_38_ACH_OUTGOING_SUMM;
/
/
