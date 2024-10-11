WITH total_spent AS (
    SELECT
        card,
        SUM(summ_with_disc) AS total_spent
    FROM
        bonuscheques
    WHERE 
        card SIMILAR TO '[0-9]+'
        AND {{datetime}}
    GROUP BY
        card
),
percentiles AS (
    SELECT
        card,
        total_spent,
        NTILE(10) OVER (ORDER BY total_spent) AS percentile
    FROM
        total_spent
), 
deciles AS (
    SELECT
        percentile,
        SUM(total_spent) AS total_spent_in_percentile
    FROM
        percentiles
    GROUP BY
        percentile
    ORDER BY
        percentile
)
SELECT 
	percentile AS "Перцентиль", 
	total_spent_in_percentile "Сумма в перцентиле",
	SUM(total_spent_in_percentile) OVER (ORDER BY percentile DESC) AS "Коммулятивная сумма",
	ROUND(total_spent_in_percentile/SUM(total_spent_in_percentile) OVER (),3)*100 as "% от вклада", 
	ROUND(SUM(total_spent_in_percentile) OVER (ORDER BY percentile DESC)/SUM(total_spent_in_percentile) OVER (),3)*100 as "Накопительный % от вклада" 
FROM 
    deciles 