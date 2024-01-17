### Modul exchange_rates_cnb is for importin of rate exchanges from CNB web pages
It is python package exchange_rates_cnb with 2 functions inside folder: exchange_rates_cnb.
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
    (Function inside converts date to Czech format DD-MM-YYYY which is used for calling of API.)
  - Function inserts exchange rates for date from parameter to 2 above mentioned tables in database.


### Requirements for installing of environment are in file:
requirements.txt
- for installation of dependencies use command: python -m pip freeze > requirements.txt
### GIT is without this file:
database.ini
- put this file to the same folder as is module exchange_rates_cnb.py
with this structure as example:
[postgresql]
host=your_host
database=your_database
user=your_user
password=your_password


### Using the possible command
your_folder_for_python\python.exe -m exchange_rates_cnb.py init
   - it does init to database
your_folder_for_python\python.exe -m exchange_rates_cnb.py increment
   - it does increment to database with today data
    (but first must be init - here is creating of database objects)
your_folder_for_python\python.exe -m exchange_rates_cnb.py increment date_string
   - it does increment to database with date as parameter, format for date_string is:
     YYYY-MM-DD, example is 2023-12-31
    (but first must be init - here is creating of database objects)
### OR if you are in python command line and import package: import exchange_rates_cnb,
### you can run it with these commands:
exchange_rates_cnb.py init()
   - it does init to database
exchange_rates_cnb.py increment   
   - it does increment to database with today data
    (but first must be init - here is creating of database objects)
exchange_rates_cnb.increment date_string(YYYY-MM-DD)
   - it does increment to database with date as parameter, format for date_string is YYYY-MM-DD,
     example is 2023-12-31
    (but first must be init - here is creating of database objects)


### Resources:
- Learn more about CNB API [in the docs](https://docs.getdbt.com/docs/introduction)
- Other information [in the discussions](https://developers.cnb.cz/discuss/633c52c8da0e79003c33a2c1)
