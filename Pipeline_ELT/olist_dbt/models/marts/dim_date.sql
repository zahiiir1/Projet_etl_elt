with dates as (

    select distinct
        cast(order_purchase_timestamp as date) as date

    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null

),

final as (

    select
        cast(strftime(date, '%Y%m%d') as integer) as date_id,
        date,
        extract(day from date) as day,
        extract(month from date) as month,
        strftime(date, '%B') as month_name,
        extract(quarter from date) as quarter,
        extract(year from date) as year,
        strftime(date, '%A') as day_of_week,
        extract(week from date) as week_number,

        case
            when strftime(date, '%w') in ('0', '6') then 1
            else 0
        end as is_weekend

    from dates

)

select * from final