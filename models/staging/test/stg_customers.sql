with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from ro_data.customers

)
select * from customers