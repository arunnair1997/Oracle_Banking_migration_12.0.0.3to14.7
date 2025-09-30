-- VIEW REMAPPED_LINKAGES (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."REMAPPED_LINKAGES" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID", "ID", "LINECODE") AS 
  SELECT
    cd.account_number,
    cd.linkage_type,
    cd.linked_reference_no,
    acc.customer_id,
    lc.liab_id,
    fac.id,
    fac.line_code || fac.line_serial AS linecode
FROM
    integratedpp.clzb_acc_coll_link_dtls cd
INNER JOIN
    integratedpp.clzb_account_apps_master acc ON acc.account_number = cd.account_number
                                            AND acc.account_status = 'A'
                                            AND acc.auth_stat = 'A'
INNER JOIN
    integratedpp.gezm_liab_cust lc ON lc.customer_no = acc.customer_id
LEFT OUTER JOIN -- Use LEFT OUTER JOIN for the 'fac' table
    arunn_admin.auto_create_facilitiy fac ON lc.liab_id = fac.liab_id
                                          AND cd.linked_reference_no = fac.line_code || fac.line_serial
-- No additional WHERE clause for 'fac' conditions, as they are now part of the LEFT JOIN's ON clause
;
