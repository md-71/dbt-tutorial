
/*
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    This table is daily information related date_exchange_rate
*/

with d_days_info as (
    select
        date_valid,
        date_exchange_rate,
        annual_serial_number,
        week_day,
        working_day,
        correction_needed
    from public.d_days_info
)

select * from d_days_info