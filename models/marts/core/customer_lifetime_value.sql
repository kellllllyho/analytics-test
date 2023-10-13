with
    paid_orders as (
        select order_id, total_amount_paid, customer_id
        from {{ ref("customer_order_details") }}
    ),

    customer_lifetime_value as (
        select paid_orders.order_id, sum(paid_orders_two.total_amount_paid) as clv_bad
        from paid_orders
        left join
            paid_orders paid_orders_two
            on paid_orders.customer_id = paid_orders_two.customer_id
            and paid_orders.order_id >= paid_orders_two.order_id
        group by paid_orders.order_id
        order by paid_orders.order_id
    )

select *
from customer_lifetime_value
