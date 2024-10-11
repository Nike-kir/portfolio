-- Подсчет суммы продаж, прибыли и выручки по каждому товару (dr_ndrugs).
with abc_sales as (
    select
        dr_ndrugs as product,  -- Название товара
        sum(dr_kol) as amount,  -- Количество проданного товара
        sum(dr_kol*(dr_croz - dr_czak) - dr_sdisc) as profit,  -- Прибыль с учетом скидок
        sum(dr_kol*dr_croz - dr_sdisc) as revenue  -- Общая выручка с учетом скидок
    from sales s
    group by dr_ndrugs  -- Группировка по товарам
),

-- Подсчет недельных продаж для анализа XYZ по каждому товару.
xyz_sales as (
    select
        dr_ndrugs as product,  -- Название товара
        to_char(dr_dat, 'YYYY-WW') as ym,  -- Неделя в формате год-неделя
        sum(dr_kol) as sales  -- Объем продаж за неделю
    from sales
    group by product, ym  -- Группировка по товару и неделе
),

-- Анализ XYZ: классификация товара на основе коэффициента вариации.
xyz_analysis as (
    select
        product,  -- Название товара
        case
            when stddev_samp(sales)/avg(sales) > 0.25 then 'Z'  -- Высокая вариативность продаж (Z)
            when stddev_samp(sales)/avg(sales) > 0.1 then 'Y'  -- Средняя вариативность продаж (Y)
            else 'X'  -- Низкая вариативность продаж (X)
        end xyz_sales  -- Результат классификации XYZ
    from xyz_sales
    group by product  -- Группировка по товару
    having count(distinct ym) >= 4  -- Учитываются товары с продажами на протяжении как минимум 4 недель
)

-- Итоговый выбор данных с классификацией товаров по ABC и XYZ.
select
    s.product,  -- Название товара
    -- ABC-анализ по количеству проданных товаров:
    case
        when sum(amount) over(order by amount desc) / sum(amount) over() <= 0.8 then 'A'  -- 80% продаж приходятся на товары категории A
        when sum(amount) over(order by amount desc) / sum(amount) over() <= 0.95 then 'B'  -- Следующие 15% на товары категории B
        else 'C'  -- Остальные товары в категории C
    end amount_ABC,

    -- ABC-анализ по прибыли:
    case
        when sum(profit) over(order by profit desc) / sum(profit) over() <= 0.8 then 'A'  -- 80% прибыли приходятся на товары категории A
        when sum(profit) over(order by profit desc) / sum(profit) over() <= 0.95 then 'B'  -- Следующие 15% на товары категории B
        else 'C'  -- Остальные товары в категории C
    end profit_ABC,

    -- ABC-анализ по выручке:
    case
        when sum(revenue) over(order by revenue desc) / sum(revenue) over() <= 0.8 then 'A'  -- 80% выручки приходятся на товары категории A
        when sum(revenue) over(order by revenue desc) / sum(revenue) over() <= 0.95 then 'B'  -- Следующие 15% на товары категории B
        else 'C'  -- Остальные товары в категории C
    end revenue_ABC,

    -- Результат XYZ-анализа:
    xyz.xyz_sales  -- Классификация товара по XYZ
from abc_sales s
left join xyz_analysis xyz  -- Левое соединение для присоединения результата XYZ-анализа
on s.product = xyz.product  -- Соединение по товару
order by product  -- Сортировка по названию товара
