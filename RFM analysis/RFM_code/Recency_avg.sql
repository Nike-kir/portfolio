-- Подзапрос для вычисления максимального количества дней с момента последней покупки для каждой карты
WITH d AS (
    SELECT 
        card, 
        ('2022-06-10' - MAX(datetime)::DATE) AS days
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтрация только числовых значений карт
    AND {{datetime}}
    GROUP BY card
)
-- Основной запрос для вычисления среднего значения дней с момента последней покупки
SELECT AVG(days) AS average_days_since_last_purchase
FROM d