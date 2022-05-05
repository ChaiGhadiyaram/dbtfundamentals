with payments as (

    select * from {{ ref('stg_payment')}}

),

final as (
    select customer_id, order_id, sum(amount)
    from payments group by 1,2
)

select * from final