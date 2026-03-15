with stg_sales as
(
    select 
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
),
stg_sales_details as
(
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        sum(Quantity) as quantity,
        sum(quantity*unitprice) as extendedpriceamount,
        sum(discount) as discountamount
    from {{ source('northwind','Order_Details') }}
    group by orderid, {{ dbt_utils.generate_surrogate_key(['productid']) }}
)

select
    o.*,
    sd.productkey,
    sd.quantity,
    sd.extendedpriceamount,
    sd.discountamount,
    sd.extendedpriceamount - sd.discountamount as soldamount
    from stg_sales o
    join stg_sales_details sd on o.OrderID=sd.orderid
