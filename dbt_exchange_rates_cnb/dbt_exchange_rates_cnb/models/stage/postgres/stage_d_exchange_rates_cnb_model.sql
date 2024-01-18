
/*
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    This table is daily information about exchange rates from CNB
*/

with d_exchange_rates_cnb as (
    select
        date_valid,
        country_name,
        currency_name,
        currency_ammount,
        currency_code,
        exchange_rate_amount
    from public.d_exchange_rates_cnb
)

select * from d_exchange_rates_cnb