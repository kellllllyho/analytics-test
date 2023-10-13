with
    successful_order_details as (select * from {{ ref("customer_customer_order_details") }}),
    customer_orders as (
        select first_order_date, customer_id from {{ ref("int_customer_orders") }}
    ),
    customer_lifetime_value as (
        select clv_bad, order_id from {{ ref("customer_lifetime_value") }}
    )

select
    successful_order_details.*,
    row_number() over (order by successful_order_details.order_id) as transaction_seq,
    row_number() over (
        partition by successful_order_details.customer_id
        order by successful_order_details.order_id
    ) as customer_sales_seq,
    case
        when customer_orders.first_order_date = successful_order_details.order_placed_at
        then 'new'
        else 'return'
    end as nvsr,
    customer_lifetime_value.clv_bad as customer_value,
    customer_orders.first_order_date as fdos
from successful_order_details
left join
    customer_orders
    on successful_order_details.customer_id = customer_orders.customer_id
left outer join
    customer_lifetime_value
    on successful_order_details.order_id = customer_lifetime_value.order_id
order by order_id
