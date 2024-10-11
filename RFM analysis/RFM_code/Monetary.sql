-- Подзапрос для вычисления общей суммы покупок (Monetary) для каждой карты
WITH a AS (
    SELECT 
        card, 
        SUM(summ_with_disc) AS Monetary
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтрация карт, содержащих только цифры
    AND {{datetime}}
    GROUP BY card
)

-- Основной запрос для вычисления процентилей суммы покупок (Monetary)
SELECT '0.1' AS "Перцентиль", 
       percentile_disc(0.1) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.2' AS "Перцентиль", 
       percentile_disc(0.2) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.3' AS "Перцентиль", 
       percentile_disc(0.3) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.4' AS "Перцентиль", 
       percentile_disc(0.4) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.5' AS "Перцентиль", 
       percentile_disc(0.5) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.6' AS "Перцентиль", 
       percentile_disc(0.6) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.7' AS "Перцентиль", 
       percentile_disc(0.7) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.8' AS "Перцентиль", 
       percentile_disc(0.8) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '0.9' AS "Перцентиль", 
       percentile_disc(0.9) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a
UNION ALL
SELECT '1.0' AS "Перцентиль", 
       percentile_disc(1.0) WITHIN GROUP (ORDER BY Monetary) AS "Monetary"
FROM a