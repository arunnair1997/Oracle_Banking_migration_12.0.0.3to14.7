-- FUNCTION CONVERT_CURRENCY_V12 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE FUNCTION "ARUNN_ADMIN"."CONVERT_CURRENCY_V12" (p_ccy1   IN VARCHAR2,
                                                p_ccy2   IN VARCHAR2,
                                                p_amount IN NUMBER)
  RETURN NUMBER IS
  l_rate   arunn_admin.cytm_rates_ty.mid_rate%TYPE;
  l_result NUMBER;
BEGIN
  -- Return same amount if currencies are equal
  IF p_ccy1 = p_ccy2 THEN
    RETURN ROUND(p_amount, 2);
  END IF;

  -- Try direct rate (multiply)
  BEGIN
    SELECT mid_rate
      INTO l_rate
      FROM ubsprod.cytm_rates@fcubsv12
     WHERE ccy1 = p_ccy1
       AND ccy2 = p_ccy2
       AND rate_type = 'STANDARD'
       AND ROWNUM = 1;
  
    RETURN ROUND(p_amount * l_rate, 2);
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END;

  -- Try reverse rate (divide)
  BEGIN
    SELECT mid_rate
      INTO l_rate
      FROM ubsprod.cytm_rates@fcubsv12
     WHERE ccy1 = p_ccy2
       AND ccy2 = p_ccy1
       AND rate_type = 'STANDARD'
       AND ROWNUM = 1;
  
    RETURN ROUND(p_amount / l_rate, 2);
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20001,
                              'No conversion rate found for ' || p_ccy1 ||
                              ' ? ' || p_ccy2);
  END;
END convert_currency_v12;
/
/
