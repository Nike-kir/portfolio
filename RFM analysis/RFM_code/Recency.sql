-- Подзапрос для вычисления максимального количества дней с момента последней покупки и количества покупок для каждой карты
WITH d AS (
    SELECT 
        card, 
        ('2022-06-10' - MAX(datetime)::DATE) AS days, 
        COUNT(datetime) AS purchase_count
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтрация только числовых значений карт
    AND {{datetime}}
    GROUP BY card
)

-- Основной запрос для вычисления перцентилей по столбцу days
SELECT '0.1' AS "Перцентиль", PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.2' AS "Перцентиль", PERCENTILE_DISC(0.2) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.3' AS "Перцентиль", PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.4' AS "Перцентиль", PERCENTILE_DISC(0.4) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.5' AS "Перцентиль", PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.6' AS "Перцентиль", PERCENTILE_DISC(0.6) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.7' AS "Перцентиль", PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.8' AS "Перцентиль", PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '0.9' AS "Перцентиль", PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d
UNION ALL
SELECT '1.0' AS "Перцентиль", PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY days) AS "Recency" FROM d