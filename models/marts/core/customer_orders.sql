with orders as (
    select customer_id, order_date, order_id from {{ ref('stg_orders')}}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
)

select * from customer_orders