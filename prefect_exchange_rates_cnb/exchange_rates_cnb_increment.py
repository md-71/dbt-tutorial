from prefect import flow
from prefect.server.schemas.schedules import CronSchedule
from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from exchange_rates_cnb import increment


my_dbt_flow = dbt_flow(
    project = DbtProject(
        name = "dbt_exchange_rates_cnb",
        project_dir = Path() / "dbt_exchange_rates_cnb",
        profiles_dir = Path.home() / ".dbt",
    ),
    profile = DbtProfile(
        target = "dev",
    ),
    flow_kwargs = {
        "task_runner": SequentialTaskRunner(),
    },
)

@flow
def upstream_increment():
    print("Flow upstream_increment is running.")
    increment_done = increment()
    print("Flow upstream_increment is ending.")
    return increment_done

@flow
def exchange_rates_cnb_increment():
    increment_done = upstream_increment()
    if increment_done == False:
        print ("Increment to database failed. Model dbt will not be updated.")
    else:
        print("Flow dbt_exchange_rates_cnb is calling.")
        dbt_future = my_dbt_flow(wait_for=[increment_done])
        print("Flow dbt_exchange_rates_cnb is after calling.")

if __name__ == "__main__":
    exchange_rates_cnb_increment.serve(name="exchange_rates_cnb_increment_deployment", schedule = CronSchedule(cron="30 17 * * *",timezone="Europe/Prague"))
