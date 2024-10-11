-- Подзапрос для подсчета количества транзакций (Frequency) для каждой карты
WITH a AS (
    SELECT 
        card, 
        COUNT(datetime) AS Frequency
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтрация только числовых значений карт
    AND {{datetime}}
    GROUP BY card
)

-- Основной запрос для вычисления среднего значения частоты покупок
SELECT AVG(Frequency) AS "Среднее значение частоты покупок"
FROM a