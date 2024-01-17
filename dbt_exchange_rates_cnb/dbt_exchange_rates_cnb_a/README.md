### Overview of this project
- This project has 3 main folders for models:
  stage folder with subfolder postgres:
   - It is prepared that each type of source will have its own subfolder.
   - Here are prepared 2 materialized views 1:1 from source tables in PostgreSQL database
  core:
   - Here are prepared 3 transformed and materialized tables with business logic.
     They are prepared in this way, which can be used for final views in mart and reused in
     some another solution.
  mart:
   - Here are prepared 2 final materialized view which are possible to use for analyzing
     and modeling in some reporting tool.
     Weekly averages of currency rates you will see in view public.mart_w_exchange_rates_model
     - We calculate 2 averages:
       - exchange_rate_average_all_days:
          - it is calculated for each 7 days in week, but holidays and weekends have the same
            exchange rates as previous working day
       - exchange_rate_average_working_days:
         - it is calculated only from working days when exchange rates are calculated and new


### Using the starter project
Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
