with stg_shippers as (
    select 
        shipperid,
        companyname as shippername
    from {{ source('northwind', 'Shippers') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['shipperid']) }} as shipperkey,
    shipperid,
    shippername
from stg_shippers
