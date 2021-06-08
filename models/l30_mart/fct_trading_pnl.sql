{{ 
config(
	  materialized='incremental'
	  , tags=["Fact Data"]
	  , pre_hook ="ALTER WAREHOUSE dbt_dev_wh SET WAREHOUSE_SIZE ='XXLARGE'" 
      , post_hook="ALTER WAREHOUSE dbt_dev_wh SET WAREHOUSE_SIZE ='XSMALL'"
	  ) 
}}
SELECT src.*
  FROM {{ref('tfm_trading_pnl')}} src

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
 WHERE (trader, instrument, date, stock_exchange_name) NOT IN (select trader, instrument, date, stock_exchange_name from {{ this }})

{% endif %}