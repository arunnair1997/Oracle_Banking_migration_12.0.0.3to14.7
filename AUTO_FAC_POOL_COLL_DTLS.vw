-- VIEW AUTO_FAC_POOL_COLL_DTLS (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."AUTO_FAC_POOL_COLL_DTLS" ("ACCOUNT_NUMBER", "LINKAGE_TYPE", "LINKED_REFERENCE_NO", "CUSTOMER_ID", "LIAB_ID", "LIMIT_ID", "LINECODE", "POOL_ID", "POOL_CODE", "COLLATERAL_CODE") AS 
  SELECT

    lnk.ACCOUNT_NUMBER,

    lnk.LINKAGE_TYPE,

    lnk.LINKED_REFERENCE_NO,

    lnk.CUSTOMER_ID,

    lnk.LIAB_ID,

    lnk.ID as limit_id,

    lnk.LINECODE,

    plink.POOL_ID,

    pl.pool_code,

    coll.collateral_code

FROM

    remapped_linkages lnk

LEFT JOIN -- 1. Link to gezm_facility (optional)

    integratedpp.gezm_facility fac

    ON lnk.id = fac.id

    AND lnk.liab_id = fac.liab_id

LEFT JOIN -- 2. Link to gezb_pool_link (optional, depends on fac)

    integratedpp.gezb_pool_link plink

    ON fac.id = plink.facility_id

    AND fac.liab_id = plink.liab_id

LEFT JOIN -- 3. Link to gczm_pool (optional, depends directly on plink's POOL_ID)

    integratedpp.gczm_pool pl

    ON plink.pool_id = pl.id

LEFT JOIN -- 4. Link to gczm_pool_coll_linkages (optional, depends on plink & pl)

    integratedpp.gczm_pool_coll_linkages pcl

    ON pl.id = pcl.pool_id

   AND plink.liab_id = pcl.liab_id -- <--- FIXED THIS CONDITION: Using plink.liab_id

LEFT JOIN -- 5. Link to gczm_collat (optional, depends on pcl)

    integratedpp.gczm_collat coll

    ON coll.id = pcl.collateral_id
;
