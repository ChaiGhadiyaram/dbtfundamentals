with payments as (

    select
        id as customer_id,
        orderid as order_id,
        amount
    from {{ source('ROne_Ana', 'payment') }}

)
select * from payments