with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_product as (
    select * from {{ ref('dim_product') }}
),
d_shipper as (
    select * from {{ ref('dim_shipper') }}
)
select 
    f_sales.orderid,
    f_sales.customerkey,
    d_customer.companyname,
    f_sales.employeekey,
    d_employee.employeenamefirstlast,
    f_sales.orderdatekey,
    f_sales.requireddatekey,
    f_sales.shippeddatekey,
    d_date.date as orderdate,
    f_sales.quantityonorder,
    f_sales.totalorderamount,
    f_sales.extendedpriceamount,
    f_sales.discountamount,
    f_sales.soldamount,
    f_sales.daysfromordertoshipped,
    f_sales.daysfromordertorequired,
    f_sales.shippedontime,
    f_sales.shippercompanyname,
    d_product.productname,
    d_product.categoryname,
    d_product.categorydescription,
    d_shipper.shippername
from f_sales
left join d_customer on f_sales.customerkey = d_customer.customerkey
left join d_employee on f_sales.employeekey = d_employee.employeekey
left join d_date on f_sales.orderdatekey = d_date.datekey
left join d_shipper on f_sales.shippercompanykey = d_shipper.shipperkey
