with stg_orders as (
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from {{ source('northwind', 'Orders') }}
),
stg_order_details as (
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        sum(quantity) as quantity,
        sum(quantity * unitprice) as extendedpriceamount,
        sum(quantity * unitprice * discount) as discountamount,
        sum((quantity * unitprice) - (quantity * unitprice * discount)) as soldamount
    from {{ source('northwind', 'Order_Details') }}
    group by orderid, productid
)

select
    o.orderid,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    od.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_orders o
join stg_order_details od on o.orderid = od.orderid