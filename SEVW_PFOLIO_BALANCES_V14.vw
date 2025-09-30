-- VIEW SEVW_PFOLIO_BALANCES_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."SEVW_PFOLIO_BALANCES_V14" ("BRANCH_CODE", "POSITION_REF_NO", "PORTFOLIO_ID", "SECURITY_ID", "SEC_QUANTITY_TYPE", "OPENING_POSITION", "CURRENT_POSITION", "UNAUTH_BUY_POSITION", "UNAUTH_SELL_POSITION", "UNAUTH_WITHDRAW_POSITION", "UNAUTH_LODGE_POSITION", "OPENING_HOLDING", "CURRENT_HOLDING", "UNAUTH_BUY_HOLDING", "UNAUTH_SELL_HOLDING", "UNAUTH_WITHDRAW_HOLDING", "UNAUTH_LODGE_HOLDING", "OPENING_DELIVERED", "CURRENT_DELIVERED", "OPENING_BALANCE_BLOCKED", "CURRENT_BALANCE_BLOCKED", "UNAUTH_BALANCE_BLOCKED", "UNAUTH_RELEASED_BLOCK", "SCY", "SKL") AS 
  (
  SELECT DISTINCT
      a.branch_code,
      a.position_ref_no,
      a.portfolio_id,
      a.security_id,
      a.sec_quantity_type,
      SUM(a.opening_position)     opening_position,
      SUM(a.current_position)     current_position,
      SUM(a.unauth_buy_position)    unauth_buy_position,
      SUM(a.unauth_sell_position)   unauth_sell_position,
      SUM(a.unauth_withdraw_position)   unauth_withdraw_position,
      SUM(a.unauth_lodge_position)    unauth_lodge_position,
      SUM(a.opening_holding)      opening_holding,
      SUM(a.current_holding)      current_holding,
      SUM(a.unauth_buy_holding)     unauth_buy_holding,
      SUM(a.unauth_sell_holding)    unauth_sell_holding,
      SUM(a.unauth_withdraw_holding)    unauth_withdraw_holding,
      SUM(a.unauth_lodge_holding)   unauth_lodge_holding,
      SUM(a.opening_delivered)      opening_delivered,
      SUM(a.current_delivered)      current_delivered,
      SUM(a.opening_balance_blocked)    opening_balance_blocked,
      SUM(a.current_balance_blocked)    current_balance_blocked,
      SUM(a.unauth_balance_blocked)   unauth_balance_blocked,
      SUM(a.unauth_released_block)    unauth_released_block,
      b.security_ccy        scy,
      a.SK_LOCATION_ID  skl
  FROM    integratedpp.sezb_pfolio_skacbalances  a,
      integratedpp.sezm_security_master    b
  WHERE   b.internal_sec_id = a.security_id
  GROUP BY  a.branch_code,
      a.position_ref_no,
      a.portfolio_id,
      a.security_id,
      a.sec_quantity_type,
      b.security_ccy,
      a.SK_LOCATION_ID)
;
