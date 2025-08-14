
  
    
    
    
        
        insert into `analytics`.`crypto_hourly_gainers__dbt_backup`
        ("ts", "symbol", "pct_1h")-- hourly gainers (> 3% over last hour)
select
  ts,
  symbol,
  pct_1h
from analytics.hourly_prices
where pct_1h > 3
  