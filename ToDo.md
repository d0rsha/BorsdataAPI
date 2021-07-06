## ToDo

1) Export stock_prices, reports and KPI's to excel.         ./

2) Restore saved excel file to df

3) Update stock_prices with last_price efficiently
Check last price date, if ( last price date < updatedAt) { append last price } 

4) Update reports with last reports efficiently (no API call to check last updated)
You can check last year and last period from saved files. Get period+1, `if (period+1 > 4) { period = 1; year+1 }`

5) Update KPI's with KPI's efficiently
Use /v{version}/instruments/kpis/updated or get_updated_kpis()  
You only get last calculated time at server, how to check historical KPI's? 

https://github.com/Borsdata-Sweden/API/wiki/KPI-Screener to get last KPI value - Can't get historical values.   
https://github.com/Borsdata-Sweden/API/wiki/KPI-History to save historic KPI's

6) Save updated df to excel