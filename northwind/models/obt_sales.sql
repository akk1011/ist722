with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select 
        customerkey, 
        companyname as customername, 
        address as customeraddress, 
        city as customercity 
    from {{ ref('dim_customer') }}
),
d_employee as (
    select 
        employeekey, 
        employeenamelastfirst as employeename, 
        employeetitle 
    from {{ ref('dim_employee') }}
),
d_date as (
    select 
        datekey, 
        year, 
        month, 
        day 
    from {{ ref('dim_date') }}
),
d_product as (
    select 
        productkey, 
        productname, 
        categoryname, 
        categorydescription 
    from {{ ref('dim_product') }}
)

select
    d_customer.customername,
    d_customer.customeraddress,
    d_customer.customercity,
    d_employee.employeename,
    d_employee.employeetitle,
    d_date.year,
    d_date.month,
    d_date.day,
    d_product.productname,
    d_product.categoryname,
    d_product.categorydescription,
    f.orderid, f.orderdatekey, f.customerkey, f.employeekey, f.productkey,
    f.quantity, f.extendedpriceamount, f.discountamount, f.soldamount
from f_sales as f
left join d_customer on f.customerkey = d_customer.customerkey
left join d_employee on f.employeekey = d_employee.employeekey
left join d_date on f.orderdatekey = d_date.datekey
left join d_product on f.productkey = d_product.productkey