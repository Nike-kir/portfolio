-- Создаем временную таблицу uni, в которой собираем данные по каждой карте.
-- Вычисляем количество дней с момента последней покупки (Recency), общее количество покупок (Frequency) и сумму покупок (Monetary).
WITH uni AS (
    SELECT 
        card,
        ('2022-06-10' - MAX(datetime)::DATE) AS days,
        COUNT(datetime) AS n_purch,
        SUM(summ_with_disc) AS summ
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтруем только по картам (предположительно, номера карт состоят только из цифр)
    GROUP BY card
),

-- Создаем временную таблицу RFM, в которой присваиваем каждой карте категории Recency, Frequency и Monetary на основе данных из таблицы uni.
RFM AS (
    SELECT 
        card, 
        CASE  
            WHEN days <= 42 THEN 1 -- Recency: Если последняя покупка была в течение 42 дней, то 1 (High Recency), иначе...
            WHEN days <= 123 THEN 2 -- ...если последняя покупка была в течение 123 дней, то 2 (Medium Recency), иначе...
            ELSE 3 -- ...если последняя покупка была более чем 123 дня назад, то 3 (Low Recency).
        END AS Recency,
        CASE  
            WHEN n_purch <= 1 THEN 3 -- Frequency: Если количество покупок равно 1, то 3 (Low Frequency), иначе...
            WHEN n_purch <= 5 THEN 2 -- ...если количество покупок меньше или равно 5, то 2 (Medium Frequency), иначе...
            ELSE 1 -- ...если количество покупок больше 5, то 1 (High Frequency).
        END AS Frequency,
        CASE  
            WHEN summ <= 843 THEN 3 -- Monetary: Если сумма покупок меньше или равна 843, то 3 (Low Monetary), иначе...
            WHEN summ <= 2730 THEN 2 -- ...если сумма покупок меньше или равна 2730, то 2 (Medium Monetary), иначе...
            ELSE 1 -- ...если сумма покупок больше 2730, то 1 (High Monetary).
        END AS Monetary
    FROM uni
),

-- Создаем временную таблицу groups, где считаем количество клиентов в каждой группе RFM.
groups AS (
    SELECT 
        COUNT(*) AS Clients,
        Recency,
        Frequency,
        Monetary
    FROM RFM
    GROUP BY Recency, Frequency, Monetary
    ORDER BY Recency, Frequency, Monetary
)

-- Выбираем данные для отчета: код (сочетание категорий Recency, Frequency и Monetary) и количество клиентов в каждой группе.
SELECT 
    CONCAT(Recency, Frequency, Monetary) AS code,
    Clients
FROM groups