-- VIEW MIG_CL_ALL_LINK_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."MIG_CL_ALL_LINK_V14" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID") AS 
  select cd.account_number,cd.linkage_type,cd.linked_reference_no,acc.customer_id,lc.liab_id
from
integratedpp.clzb_acc_coll_link_dtls cd, integratedpp.clzb_account_apps_master acc, integratedpp.gezm_liab_cust lc
where acc.account_number=cd.account_number
and acc.account_status='A' and acc.auth_stat='A'
and lc.customer_no=acc.customer_id
;
