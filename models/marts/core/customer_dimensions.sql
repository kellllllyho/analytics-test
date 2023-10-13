with successful_orders as (
    select total_amount_paid, payment_finalized_date, order_id from {{ ref('stg_paid_orders') }}
),

orders as (
    select order_id, customer_id, order_date, status from {{ ref('stg_orders')}}
),

customers as (
    select first_name, last_name, customer_id from {{ ref('stg_customers') }}
),

paid_orders_2 as (select
orders.order_id,
orders.customer_id,
orders.order_date as order_placed_at,
orders.status as order_status,
successful_orders.total_amount_paid,
successful_orders.payment_finalized_date,
customers.first_name as customer_first_name,
customers.last_name as customer_last_name
from successful_orders 
inner join orders on successful_orders.order_id = orders.order_id
inner join customers on orders.customer_id = customers.customer_id)

select * from paid_orders_2