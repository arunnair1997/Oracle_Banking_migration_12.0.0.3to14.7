-- VIEW TRVW_CUSTOMER_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."TRVW_CUSTOMER_V14" ("SOURCE_SYSTEM", "SOURCE_SYSTEM_CUST_NO", "CUSTOMER_NO", "HOST_CODE", "CUSTOMER_TYPE", "CUSTOMER_NAME1", "SHORT_NAME", "ADDRESS_LINE1", "ADDRESS_LINE2", "ADDRESS_LINE3", "ADDRESS_LINE4", "PINCODE", "COUNTRY", "LANGUAGE", "NATIONALITY", "DECEASED", "FROZEN", "WHEREABOUTS_UNKNOWN", "RM_ID", "SANCTIONS_CHECKS_REQUIRED", "RECORD_STAT", "AUTH_STAT", "MOD_NO", "MAKER_ID", "MAKER_DT_STAMP", "CHECKER_ID", "CHECKER_DT_STAMP", "ONCE_AUTH", "ACCESS_GROUP", "IS_FORGOTTEN", "CLS_CCY_ALLOWED", "CLS_PARTICIPANT", "FX_NETTING_CUSTOMER", "RISK_CATEGORY", "RISK_PROFILE", "FULL_NAME", "TAX_GROUP", "CUSTOMER_CATEGORY", "CHARGE_GROUP", "LOCATION_CCY", "CONF_METHOD", "PARENT", "FRONT_OFFICE_SHORT_NAME", "FRONT_OFFICE_LONG_NAME", "FO_CUST_TYPE", "FUND_PREFIX", "NON_NET_GROUP", "RISK_PARENT", "CSR_CODE", "LOAD_STATUS", "CB_FLAG", "CHARGE_DETAILS", "CHECK_NAME_FW", "CHECK_NAME_MW", "MTS_DEAL", "RESTRICTED_FLAG", "EFFECTIVE_DATE", "RESTRICTED_NOTE", "PMNT_SWIFT_MSG_TYPE", "PRIMARY_SETTLE", "PRIMARY_SETTLE_CUST", "PRIMARY_SETTLE_SWIFT", "CREDIT_CHECK_REQD", "FUTURE_CREDIT_CHECK", "CREDIT_CHECK_EFFDATE", "EOD_TRADE_SUMMARY_REQD", "EOD_NETTING_SUMMARY_REQD", "EOD_SETTLEMENT_SUMMARY_REQD", "SPL_HANDLING_REQD", "FX_SETTLEMENT_RISK_GROUP", "INTERNAL_TRADE_GROUP_CODE", "SALESPERSON_ID", "CGMI_BRANCH", "FCCOM_CODE", "NETTING_AGREEMENT", "CP_LINK_INDICATOR", "CP_LINK_INDICATOR_FXO", "LIAB_BR", "CUST_CLASSIFICATION", "GROUP_CODE", "SECTOR", "SECTOR_DESCRIPTION", "ADDITIONAL_SECTOR", "ADD_SECTOR_DESCRIPTION", "NATURE_OF_ENTITY", "LEGAL_IDENTITY_ID", "RP_CUSTOMER", "TR_RECORD_STAT", "TR_AUTH_STAT", "TR_ONCE_AUTH", "CLEARING_HOUSE_CCP", "COUNTERPARTY", "BROKER", "TRIPARTY_AGENT", "INTERMEDIARY", "TRADING_VENUE", "CODE_TYPE", "PROPREITARY_CODE") AS 
  SELECT
        a.source_system,
        a.source_system_cust_no,
        a.customer_no,
        a.host_code,
        a.customer_type,
        a.customer_name1,
        a.short_name,
        a.address_line1,
        a.address_line2,
        a.address_line3,
        a.address_line4,
        a.pincode,
        a.country,
        a.language,
        a.nationality,
        a.deceased,
        a.frozen,
        a.whereabouts_unknown,
        a.rm_id,
        a.sanctions_checks_required,
        a.record_stat,
        a.auth_stat,
        a.mod_no,
        a.maker_id,
        a.maker_dt_stamp,
        a.checker_id,
        a.checker_dt_stamp,
        a.once_auth,
        a.access_group,
        a.is_forgotten,
        b.cls_ccy_allowed,
        b.cls_participant,
        b.fx_netting_customer,
        b.risk_category,
        b.risk_profile,
        a.customer_name1 AS full_name,
        --fn_tr_get_liabno(a.customer_no) AS liability_no,
        --fn_tr_get_biccode(a.customer_no) AS bic_code,
        b.tax_group,
        b.customer_category,
        b.charge_group,
        b.location_ccy,
        b.conf_method,
        b.parent,
        b.front_office_short_name,
        b.front_office_long_name,
        b.fo_cust_type,
        b.fund_prefix,
        b.non_net_group,
        b.risk_parent,
        b.csr_code,
        b.load_status,
        b.cb_flag,
        b.charge_details,
        b.check_name_fw,
        b.check_name_mw,
        b.mts_deal,
        b.restricted_flag,
        b.effective_date,
        b.restricted_note,
        b.pmnt_swift_msg_type,
        b.primary_settle,
        b.primary_settle_cust,
        b.primary_settle_swift,
        b.credit_check_reqd,
        b.future_credit_check,
        b.credit_check_effdate,
        b.eod_trade_summary_reqd,
        b.eod_netting_summary_reqd,
        b.eod_settlement_summary_reqd,
        b.spl_handling_reqd,
        b.fx_settlement_risk_group,
        b.internal_trade_group_code,
        b.salesperson_id,
        b.cgmi_branch,
        b.fccom_code,
        b.netting_agreement,
        b.cp_link_indicator,
        b.cp_link_indicator_fxo,
        b.liab_br,
        b.cust_classification,
        b.group_code,
        b.sector --Bug 32664428 Start
       ,
        b.sector_description,
        b.additional_sector,
        b.add_sector_description,
        b.nature_of_entity,
        b.legal_identity_id --Bug 32664428 End
       ,
        b.rp_customer --OBTR_RP_Changes Added
    --Bug_33177512 Change starts
        ,b.record_stat,b.auth_stat,b.once_auth
        ,b.clearing_house_ccp,b.counterparty
        ,b.broker,b.triparty_agent
        ,b.intermediary,b.trading_venue
    --Bug_33177512 Change end
    --Bug_34136204 Change starts
    ,c.PARTY_CODE_TYPE
    ,c.party_code
    --Bug_34136204 Change ends
    FROM
        integratedpp.stzm_core_customer a,
        integratedpp.stzm_tr_customer b,
         arunn_admin.stvw_tr_cust_def_party_v14 c
    WHERE
        a.customer_no = b.customer_no
        and a.CUSTOMER_NO  = c.CUSTOMER_NO (+)
;
