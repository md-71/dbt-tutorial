
/*
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    This table is daily information related date_exchange_rate
*/

with d_days_info as (
    select * from {{ ref('stage_d_days_info_model')}}
)

select * from d_days_info