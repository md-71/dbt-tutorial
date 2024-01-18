/*
    This table is weekly agragated table for calculation of exchange rate avarage
    - exchange_rate_average_all_days - it is calculated for each 7 days in week, but holidays and weekends have the same excange rates
									   as previous working day
	- exchange_rate_average_working_days - it is calculated only from working days when exchange rates are calculated and new 

*/

--{{ config(materialized='table') }} -- this is defined in dbt_project.yml, I can override here if I want 


with d_exchange_rates_cnb as (
    select * from {{ ref('stage_d_exchange_rates_cnb_model')}}
)
,
d_days_info as (
    select * from {{ ref('stage_d_days_info_model')}}
)
,
w_exchange_rates as (
	select
		min(der.date_valid) as start_date,
		max(der.date_valid) as end_date,
        der.country_name as country_name,
        der.currency_name as currency_name,
        max(der.currency_ammount) as currency_ammount,
        der.currency_code as currency_code,
        round(avg(der.exchange_rate_amount),3) as exchange_rate_average_all_days,
		round
			(sum
				(case when ddi.working_day = True
			 		then exchange_rate_amount
			 		else 0
			 	end
				) /
				sum
					(case when ddi.working_day = True
				  		then 1
				 		else 0
					end
					)
			 , 3) as exchange_rate_average_working_days
    from d_exchange_rates_cnb der
    inner join d_days_info ddi
        on der.date_valid = ddi.date_valid
	cross join
		(select min(date_valid) as date_valid
		 	from d_days_info where week_day = 0) ddi1
    where der.date_valid >= ddi1.date_valid
    group by FLOOR (date_part('day', der.date_valid) - ddi.week_day) / 7,
		der.country_name,
		der.currency_name,
		der.currency_code
	order by
		min(der.date_valid),		
		der.country_name
)


select * from w_exchange_rates