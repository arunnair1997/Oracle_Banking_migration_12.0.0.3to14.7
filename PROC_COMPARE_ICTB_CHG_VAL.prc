-- PROCEDURE PROC_COMPARE_ICTB_CHG_VAL (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PROC_COMPARE_ICTB_CHG_VAL" as 

 V_ERROR_MSG VARCHAR(4000);

BEGIN

  insert into minus_ICTB_CHG_VAL
   select * from 
    (
    

select BRN, ACC, ELEM, ITM_DT, ELEM_VAL, ELEM_CCY, ELEM_TYPE, PROD from ubsprod.ICTB_CHG_VAL@fcubsv12
MINUS
select BRN, ACC, ELEM, ITM_DT, ELEM_VAL, ELEM_CCY, ELEM_TYPE, PROD from integratedpp.ICZB_CHG_VAL
)
      where rownum < 6;
  
  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    V_ERROR_MSG := SQLERRM;
    INSERT INTO TLOG
    values
      ('Bombed due to ' || V_ERROR_MSG || 'for ICTB_CHG_VAL at ' ||
       systimestamp);
END;
/
/
