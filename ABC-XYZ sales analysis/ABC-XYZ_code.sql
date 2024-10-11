with abc_sales as (
    select
        dr_ndrugs as product,
        sum(dr_kol) as amount,
        sum(dr_kol*(dr_croz - dr_czak) - dr_sdisc) as profit,
        sum(dr_kol*dr_croz - dr_sdisc) as revenue
    from sales s
    group by dr_ndrugs
),
xyz_sales as (
    select
        dr_ndrugs as product,
        to_char(dr_dat, 'YYYY-WW') as ym,
        sum(dr_kol) as sales
    from sales
    group by product, ym
),
xyz_analysis as (
    select
        product,
        case
            when stddev_samp(sales)/avg(sales) > 0.25 then 'Z'
            when stddev_samp(sales)/avg(sales) > 0.1 then 'Y'
            else 'X'
        end xyz_sales
    from xyz_sales
    group by product
    having count(distinct ym) >= 4
)
select
    s.product,
    case
        when sum(amount) over(order by amount desc) / sum(amount) over() <= 0.8 then 'A'
        when sum(amount) over(order by amount desc) / sum(amount) over() <= 0.95 then 'B'
        else 'C'
    end amount_ABC,
    case
        when sum(profit) over(order by profit desc) / sum(profit) over() <= 0.8 then 'A'
        when sum(profit) over(order by profit desc) / sum(profit) over() <= 0.95 then 'B'
        else 'C'
    end profit_ABC,
    case
        when sum(revenue) over(order by revenue desc) / sum(revenue) over() <= 0.8 then 'A'
        when sum(revenue) over(order by revenue desc) / sum(revenue) over() <= 0.95 then 'B'
        else 'C'
    end revenue_ABC,
    xyz.xyz_sales
from abc_sales s
left join xyz_analysis xyz
on s.product = xyz.product
order by product