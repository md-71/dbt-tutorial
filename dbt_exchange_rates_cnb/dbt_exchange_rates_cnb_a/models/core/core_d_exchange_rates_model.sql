/*
    This table is 1:1 from source - daily information about exchange rates from CNB
*/

--{{ config(materialized='table') }} -- this is defined in dbt_project.yml, I can override here if I want 


with d_exchange_rates_cnb as (
    select * from {{ ref('stage_d_exchange_rates_cnb_model')}}
)

select * from d_exchange_rates_cnb