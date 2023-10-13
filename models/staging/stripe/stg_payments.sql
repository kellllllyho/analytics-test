select
    -- ids
    id as payment_id,
    orderid as order_id,
    -- strings
    paymentmethod as payment_method,
    status,
    -- numerics
    amount as amount_cents,
    amount / 100 as amount,
    -- dates
    created as created_at

from {{ source("stripe", "payment") }}
