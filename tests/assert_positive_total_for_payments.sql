-- Refunds have a negative amount, so the total amount should always be >+ 0
select order_id, sum(amount) as total_amount
from {{ ref("stg_payments") }}
group by order_id
having not (total_amount >= 0)
