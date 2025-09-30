-- VIEW MIG_CL_COLL_LINK_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."MIG_CL_COLL_LINK_V12" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID", "COLLATERAL_CODE") AS 
  select cd.account_number,cd.linkage_type,cd.linked_reference_no,acc.customer_id,lc.liab_id,coll.collateral_code from
cltb_acc_coll_link_dtls@fcubsv12 cd, cltb_account_apps_master@fcubsv12 acc, getm_liab_cust@fcubsv12 lc ,getm_collat@fcubsv12 coll
where acc.account_number=cd.account_number
and acc.account_status='A' and acc.auth_stat='A'
and lc.customer_no=acc.customer_id
and cd.linkage_type='C'
and lc.liab_id = coll.liab_id
and cd.linked_reference_no = coll.collateral_Code
;
