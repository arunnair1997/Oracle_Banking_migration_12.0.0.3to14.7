-- VIEW AUTO_FAC_POOL_DTLS (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."AUTO_FAC_POOL_DTLS" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID", "ID", "LINECODE", "POOL_ID", "POOL_CODE") AS 
  SELECT

    lnk.ACCOUNT_NUMBER,

    lnk.LINKAGE_TYPE,

    lnk.LINKED_REFERENCE_NO,

    lnk.CUSTOMER_ID,

    lnk.LIAB_ID,

    lnk.ID,

    lnk.LINECODE,

    plink.POOL_ID,   -- This POOL_ID comes directly from the getb_pool_link table

    pl.pool_code     -- This pool_code comes from the getm_pool table, linked via plink.POOL_ID

FROM

    remapped_linkages lnk

LEFT JOIN -- 1. Link to getm_facility (optional)

    integratedpp.gezm_facility fac

    ON lnk.id = fac.id

    AND lnk.liab_id = fac.liab_id

LEFT JOIN -- 2. Link to getb_pool_link (optional, depends on fac)

    integratedpp.gezb_pool_link plink

    ON fac.id = plink.facility_id

    AND fac.liab_id = plink.liab_id

LEFT JOIN -- 3. Link to getm_pool (optional, depends directly on plink's POOL_ID)

    integratedpp.gczm_pool pl

    ON plink.pool_id = pl.id
;
