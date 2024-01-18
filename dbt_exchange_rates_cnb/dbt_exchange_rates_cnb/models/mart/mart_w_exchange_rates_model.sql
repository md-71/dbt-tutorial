/*
    This view is 1:1 from core table - weekly agragated calculation of exchange rate avarage plus new column week_desc
    - exchange_rate_average_all_days - it is calculated for each 7 days in week, but holidays and weekends have the same exchange rates
									   as previous working day
	- exchange_rate_average_working_days - it is calculated only from working days when exchange rates are calculated and new 
*/

--{{ config(materialized='view') }} -- it is defined in dbt_project.yml


with w_exchange_rates as (
    select * from {{ ref('core_w_exchange_rates_model')}}
)

select
    concat(start_date, '_', end_date) as week_desc,
    *
from w_exchange_rates