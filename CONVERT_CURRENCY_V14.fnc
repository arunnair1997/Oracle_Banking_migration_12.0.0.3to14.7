-- FUNCTION CONVERT_CURRENCY_V14 (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE FUNCTION "ARUNN_ADMIN"."CONVERT_CURRENCY_V14" (
    p_ccy1   IN VARCHAR2,
    p_ccy2   IN VARCHAR2,
    p_amount IN NUMBER
) RETURN NUMBER IS
    l_rate   INTEGRATEDPP.cyzm_rates.mid_rate%TYPE;
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
        FROM integratedpp.cyzm_rates
        WHERE ccy1 = p_ccy1
          AND ccy2 = p_ccy2
          AND rate_type = 'STANDARD'
          AND ROWNUM = 1;
 
        RETURN ROUND(p_amount * l_rate, 2);
 
    EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
    END;
 
    -- Try reverse rate (divide)
    BEGIN
        SELECT mid_rate
        INTO l_rate
        FROM integratedpp.cyzm_rates
        WHERE ccy1 = p_ccy2
          AND ccy2 = p_ccy1
          AND rate_type = 'STANDARD'
          AND ROWNUM = 1;
 
        RETURN ROUND(p_amount / l_rate, 2);
 
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
              'No conversion rate found for ' || p_ccy1 || ' ? ' || p_ccy2);
    END;
END convert_currency_v14;
/
/
