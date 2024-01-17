/*
    This view is 1:1 from core model core_d_exchange_rates_model enriched about detail from core_d_days_info_model
	- for example: is this day working or not

*/

--{{ config(materialized='view') }} -- it is defined in dbt_project.yml


with d_exchange_rates_cnb as (
    select * from {{ ref('core_d_exchange_rates_model')}}
)
,
d_days_info as (
    select * from {{ ref('core_d_days_info_model')}}
)

select
	der.*,
	ddi.annual_serial_number,
	ddi.week_day,
	ddi.working_day,
	ddi.correction_needed
from d_exchange_rates_cnb der
inner join d_days_info ddi
	on der.date_valid = ddi.date_valid