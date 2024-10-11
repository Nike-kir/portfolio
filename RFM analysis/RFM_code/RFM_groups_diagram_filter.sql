-- Создаем временную таблицу uni, где собираем данные по каждой карте.
-- Вычисляем количество дней с момента последней покупки (Recency), общее количество покупок (Frequency) и сумму покупок (Monetary).
WITH uni AS (
    SELECT 
        card,
        ('2022-06-10' - MAX(datetime)::DATE) AS days,
        COUNT(datetime) AS n_purch,
        SUM(summ_with_disc) AS summ
    FROM bonuscheques
    WHERE card SIMILAR TO '[0-9]+' -- Фильтруем только по картам (предположительно, номера карт состоят только из цифр)
    AND {{datetime}}
    GROUP BY card
),

-- Создаем временную таблицу RFM, где определяем категории Recency, Frequency и Monetary для каждой карты на основе данных из таблицы uni.
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

-- Создаем временную таблицу rfm_groups, где определяем группу клиента на основе комбинации RFM-кодов.
rfm_groups AS (
    SELECT 
        card AS "Клиент", 
        CONCAT(Recency, Frequency, Monetary) AS RFM_code,
        CASE 
            WHEN CONCAT(Recency, Frequency, Monetary) = '111' THEN 'VIP_клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) = '121' THEN 'Лояльные клиенты с высоким чеком'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('112', '122', '113', '123') THEN 'Лояльные клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('131', '132', '133') THEN 'Новички'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('213', '212', '211') THEN 'Спящие лояльные клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('233', '232', '231', '223', '222', '221') THEN 'Спящие редкие и разовые клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('313', '312', '311') THEN 'Уходящие лояльные клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('323', '322', '321') THEN 'Уходящие редкие клиенты'
            WHEN CONCAT(Recency, Frequency, Monetary) IN ('333', '332', '331') THEN 'Уходящие одноразовые клиенты'
        END AS groups
    FROM RFM
    ORDER BY 3
)

-- Выбираем данные для отчета: группа клиентов и количество клиентов в каждой группе.
SELECT 
    groups, 
    COUNT(*) AS "Клиентов"
FROM 
    rfm_groups
[[WHERE groups={{groups}}]]
GROUP BY 
    groups
ORDER BY 
    groups
