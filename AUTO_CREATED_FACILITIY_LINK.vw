-- VIEW AUTO_CREATED_FACILITIY_LINK (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."AUTO_CREATED_FACILITIY_LINK" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID", "ID", "LINECODE") AS 
  select cd.account_number,cd.linkage_type,cd.linked_reference_no,acc.customer_id,lc.liab_id,fac.id,fac.line_code || fac.line_serial linecode
from
integratedpp.clzb_acc_coll_link_dtls cd, integratedpp.clzb_account_apps_master acc, integratedpp.gezm_liab_cust lc,arunn_admin.auto_create_facilitiy fac
where acc.account_number=cd.account_number
and acc.account_status='A' and acc.auth_stat='A'
and lc.customer_no=acc.customer_id
and lc.liab_id=fac.liab_id
and cd.linked_reference_no=fac.line_code||fac.line_serial
;
