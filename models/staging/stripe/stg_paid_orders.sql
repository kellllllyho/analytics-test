with
    payments as (
        select order_id, created_at, amount, status from {{ ref("stg_payments") }}
    ),

    successful_orders as (
        select
            order_id,
            max(created_at) as payment_finalized_date,
            sum(amount) as total_amount_paid,
            status
        from payments
        where status <> 'fail'
        group by order_id, status
    )

select *
from successful_orders
