with paid_orders_2 as (
    select * from {{ ref('customer_dimensions') }}
),
customer_orders as (
    select first_order_date, customer_id from {{ ref('customer_orders') }}
),
customer_lifetime_value as (
    select clv_bad, order_id from {{ ref('customer_lifetime_value') }}
)

select
    paid_orders_2.*,
    ROW_NUMBER() OVER (ORDER BY paid_orders_2.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY paid_orders_2.customer_id ORDER BY paid_orders_2.order_id) as customer_sales_seq,
    CASE WHEN customer_orders.first_order_date = paid_orders_2.order_placed_at
    THEN 'new'
    ELSE 'return' END as nvsr,
    customer_lifetime_value.clv_bad as customer_value,
    customer_orders.first_order_date as fdos
FROM paid_orders_2
left join customer_orders on paid_orders_2.customer_id = customer_orders.customer_id
left outer join customer_lifetime_value on paid_orders_2.order_id = customer_lifetime_value.order_id
order by order_id