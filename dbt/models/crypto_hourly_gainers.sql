-- hourly gainers (> 3% over last hour)
select
  ts,
  symbol,
  pct_1h
from analytics.hourly_prices
where pct_1h > 3
