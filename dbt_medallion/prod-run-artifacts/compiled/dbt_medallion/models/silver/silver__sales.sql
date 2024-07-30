


with cleaning_view as (
    

        SELECT *
        FROM `ip-trabajo-gverdugo`.`bronze`.`sales`

    
), fields_view as (
    
            select 
                
                    SAFE_CAST(`Row ID` as INT64) as `row_id`,
                
                    SAFE_CAST(`Order ID` as STRING) as `order_id`,
                
                    SAFE.PARSE_DATE('%d/%m/%Y', `Order Date`) as `order_date`,
                
                    SAFE.PARSE_DATE('%d/%m/%Y', `Ship Date`) as `ship_date`,
                
                    SAFE_CAST(`Ship Mode` as STRING) as `ship_mode`,
                
                    SAFE_CAST(`Customer ID` as STRING) as `customer_id`,
                
                    SAFE_CAST(`Customer Name` as STRING) as `customer_name`,
                
                    SAFE_CAST(`Segment` as STRING) as `segment`,
                
                    SAFE_CAST(`Country` as STRING) as `country`,
                
                    SAFE_CAST(`City` as STRING) as `city`,
                
                    SAFE_CAST(`State` as STRING) as `state`,
                
                    SAFE_CAST(`Postal Code` as INT64) as `postal_code`,
                
                    SAFE_CAST(`Region` as STRING) as `region`,
                
                    SAFE_CAST(`Product ID` as STRING) as `product_id`,
                
                    SAFE_CAST(`Category` as STRING) as `category`,
                
                    SAFE_CAST(`Sub-Category` as STRING) as `sub_category`,
                
                    SAFE_CAST(`Product Name` as STRING) as `product_name`,
                
                    SAFE_CAST(`Sales` as FLOAT64) as `sales`,
                
            from cleaning_view
        
), final as(
    select * from fields_view
)
select * from final