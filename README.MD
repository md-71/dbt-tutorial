###Overview of solution:
This solution has 3 parts:
- First is python script exchange_rates_cnb.py with 2 functions inside folder: dbt_exchange_rates_cnb.
  Functions work with PostgreSQL database.
  - function init:
    - This function is without parameter. It creates (If they already exist then first does drop.) 2 tables.
      in database: public.d_exchange_rates_cnb and public.d_days_info
      - public.d_exchange_rates_cnb has values as they are in CNB API for exchange rates
      - public.d_days_info has information from first row of API result plus some helping calculated columns.
        (Detail about columns which helps to calculate weekly average of exchange rates is inside dbt model.)
    - After creation of tables it inserts init data from 3 months ago to yesterday from CNB API to tables.
  - function increment:
    - This function has default parameter with is today date in string format: YYYY-MM-DD, but it can be
      called with another date in the same format.
      (Function inside converts date to Czech format DD.MM.YYYY which is used for calling of API.)
    - Function inserts exchange rates for date from parameter to 2 above mentioned tables in database.
  Possible commands:
  your_folder_for_python\python.exe exchange_rates_cnb.py init
   - it does init to database
  your_folder_for_python\python.exe exchange_rates_cnb.py increment
   - it does increment to database with today data
    (but first must be init - here is creating of database objects)
  your_folder_for_python\python.exe exchange_rates_cnb.py increment date_string
   - it does increment to database with date as parameter, format for date_string is:
     YYYY-MM-DD, example is 2023-12-31
    (but first must be init - here is creating of database objects)
  Resource for API:
  - Learn more about CNB API [in the docs](https://docs.getdbt.com/docs/introduction)
  - Other information [in the discussions](https://developers.cnb.cz/discuss/633c52c8da0e79003c33a2c1)

- Second is dbt project inside of folder: dbt_exchange_rates_cnb/dbt_exchange_rates_cnb.
  - This project takes data from PostgreSQL database and create model,
    which transforms data to requested format.
    Result is again present in PostgreSQL database in these final views:
    public.mart_d_exchange_rates_detail_model and public.mart_w_exchange_rates_model
    - Weekly averages of currency rates you will see in view public.mart_w_exchange_rates_model
    - For check detail used for calculation is view: public.mart_d_exchange_rates_detail_model
    Views can be used for example in Power BI for creating of report with aggregate data (average)
    and for drill through to the detail for all days which are used for current week average.
  - More in README.md file inside subfolder dbt_exchange_rates_cnb\dbt_exchange_rates_cnb.

- Third is installation of scheduler Prefect with one python script exchange_rates_cnb_increment.py
  in folder dbt_exchange_rates_cnb:
  - This scheduler after installation will run daily increment function for new data from CNB
    to inserting them to the PostgreSQL database and after this it will run dbt project for
    recalculation of project objects.


###Installation instructions:
A. Prerequisites:
   - Windows 11 and above as operation system
     (Current solution in GIT was made in Windows 11 Enterprise - version 22H2.)
     - If you have Linux then you need update some scripts settings regarding using of path.)
   - Running PostgreSQL database with Public schema - some version 16
     (Current solution in GIT was made with using of postgresql-16.1-1-windows-x64)
   - Python - at least version 3.8 (Current solution in GIT was made in version 3.9.16)
   - GIT

B. Installation steps:
   1.  Clone project from https://github.com/md-71/dbt-tutorial.git
       or you can download ZIP file from page: https://github.com/md-71/dbt-tutorial
       (Expand the code button and at the end is "Download ZIP".)
       - if you did clone, you would have all under folder: dbt-tutorial
       - if you download ZIP file expand it to your directory without creating of new folder
         dbt-tutorial-main, that you do not have it twice. You can rename it to dbt-tutorial.
         We use this name in our example paths below.
   2.  Run cmd.exe to open command line.
   3.  Switch to the folder dbt-tutorial (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial)
   4.  Create virtual environment in this working folder dbt-tutorial.
       (for example: C:\Users\your_user_name\Anaconda3\python.exe -m venv dbt_msd_env)
       Run command: python -m pip install --upgrade pip
       - it is for the latest version of pip
      Run command: pip install --upgrade pip setuptools wheel
      - it is for the latest version of tools
   5.  Activate virtual environment from working folder dbt-tutorial: dbt_msd_env\Scripts\activate
   6.  Install requirements from file requirements.txt from current folder dbt-tutorial
       (for example: C:\Users\your_user_name\Anaconda3\python.exe -m pip install -r requirements.txt)
   7.  Switch to folder dbt_exchange_rates_cnb and create file database.ini here.
       (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial\dbt_exchange_rates_cnb)
       - database.ini will have this structure:
[postgresql]
host=your_host
database=your_database
user=your_user
password=your_password
       - Replace your_host, your_database, your_user, your_password with values for your database.
         User must have rights to create and drop object in your database and write to the objects.
   8. Do init to database with using init function from python script exchange_rates_cnb.py
       (for example: C:\Users\your_user_name\Anaconda3\python.exe exchange_rates_cnb init)
       - you can check data in PostgreSQL database in tables:
         public.d_exchange_rates_cnb and public.d_days_info
   9. Create file profiles.yml in folder C:\Users\your_user_name\.dbt
       - replace your_user_name with your user name
       - profiles.yml will have this structure:
dbt_exchange_rates_cnb:
  outputs:
    dev:
      dbname: your_database
      host: your_host
      pass: your_password
      port: 5432
      schema: public
      threads: 1
      type: postgres
      user: your_user
  target: dev
       - Indentation needs to be preserved as it is in example above.
       - Replace only your_database, your_host, your_password, your_user with values for your database.
   10. Switch to subfolder dbt_exchange_rates_cnb
       (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial\dbt_exchange_rates_cnb\dbt_exchange_rates_cnb)
   11. Run command from current folder: dbt run
       (If you receive issue: 'dbt' is not recognized as an internal or external command,
       operable program or batch file. You need again run installation of package dbt-postgres manually.
       Because I do not know why, but step 6. did not do all. Run installation by example:
       C:\Users\your_user_name\Anaconda3\python.exe -m pip install dbt-postgres
       Then again do command: dbt run)
       - This will create dbt project, check if all is without error from the on-screen listing.
       - If you will have some error, you can try to run command: dbt debug
         and solve your problems regarding the on-screen listing information.
       - You can check if all from you project is created in PostgreSQL database with transformed init data.
         You will see here these objects:
         - tables: public.core_d_days_info_model, public.core_d_exchange_rates_model and
           public.core_w_exchange_rates_model
         - views: public.mart_d_exchange_rates_detail_model and public.mart_w_exchange_rates_model
           Weekly averages of currency rates you will see in view public.mart_w_exchange_rates_model
   12. Switch to parent folder dbt_exchange_rates_cnb
       (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial\dbt_exchange_rates_cnb)
   13. Install Prefect scheduler package - last version
       (for example: C:\Users\your_user_name\Anaconda3\python.exe -m pip install -U prefect)
   14. Check if installation is ok with command: prefect version
       (If you receive issue: 'prefect' is not recognized as an internal or external command,
       operable program or batch file. You need again run installation of package now with command:
       pip install -U prefect
       Then again check installation with command: prefect version)
       - if installation is OK, you will see something similar this:
         Version:             2.14.15
         API version:         0.8.4
         Python version:      3.9.16
         Git commit:          e585e980
         Built:               Thu, Jan 11, 2024 6:18 PM
         OS/Arch:             win32/AMD64
         Profile:             default
         Server type:         ephemeral
         Server:
           Database:          sqlite
           SQLite version:    3.40.1
   15. Run command from current folder prefect_exchange_rates_cnb: prefect server start
       - this command will start prefect server
       - copy link after text "Check out the dashboard at " from the on-screen listing.
         (for example, it looks like this: Check out the dashboard at http://127.0.0.1:4200)
         Open this link in your web browser. You will see Perfect dashboard.
       - do not close this cmd window, that prefect server is running and leave it now
   16. Open another command line window with cmd.exe command.
   17. Switch to working folder created in step 2.
       (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial)
   18. Activate virtual environment from working folder (dbt_msd_env\Scripts\activate)
   19. Switch to folder dbt_exchange_rates_cnb
       (for example: cd C:\Users\your_user_name\Demos\dbt-tutorial\dbt_exchange_rates_cnb)
   20. Create deployment of workflow with scheduling.
       Run python module exchange_rates_cnb_increment.py in current folder.
       (for example: C:\Users\your_user_name\Anaconda3\python.exe exchange_rates_cnb_increment.py)
         - If you will receive error: ModuleNotFoundError: No module named 'prefect_dbt_flow'
           Then I do not know why, but installation from requirements.txt from point 6. did not do
           what was expected. Run this command: pip install prefect-dbt-flow
       If all is OK, you will see green information:
       "Your flow 'exchange-rates-cnb-increment' is being served and polling for scheduled runs!"
       - Do not close this cmd window, that scheduling of flow is running and leave it now.
       - Go to the web browser to Perfect dashboard. Select "Deployments" from menu on left side.
         Click on 3 dots on rights side of your deployment exchange_rates_cnb_increment_deployment.
         Select "Edit". You will see that your Scheduling is setup: At 05:30 PM every day (Europe/Prague)
         You can edit it if you want. I realized that new exchange rates are provided by CNB about 3 PM.
   21. If you did point 20. after scheduled time 05:30 PM, you need rum flow manually,
       that you do not lose exchange rates for this day. Select "Deployments" from menu on left side.
       Click on 3 dots on rights side of your deployment exchange_rates_cnb_increment_deployment.
       Select Quick run. And flow will start now.
   22. You can check that flow will start in scheduled time or manually by point 21.
       It is visible in cmd window where scheduling is running - see point 20.
       - If all is OK than you will see at the end on-screen listing something like this:
         Flow upstream_increment is running.
         Connecting to the PostgreSQL database...
         Insert for parameter: 17.01.2024 is done.
         Database connection closed.
         Flow upstream_increment is ending.
         Flow dbt_exchange_rates_cnb is calling.
         Flow dbt_exchange_rates_cnb is after calling.
       - If current date is already present in database or something other is wrong then you will see this:
         Flow upstream_increment is running.
         Connecting to the PostgreSQL database...
         Error: duplicate key value violates unique constraint "d_exchange_rates_cnb_pkey"
         DETAIL:  Key (date_valid, currency_code)=(2024-01-17, AUD) already exists.
         Insert for parameter: 17.01.2024 is not done.
         Database connection closed.
         Flow upstream_increment is ending.
         Increment to database failed. Model dbt will not be updated.
   23. When flow from point 22. did inserting of exchange rates for current date,  you can check,
       that dbt model is updated in PostgreSQL database and new date is present in objects - see point 16.