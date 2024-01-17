import os
from datetime import date,datetime
import requests
import pandas as pd
import psycopg2
from configparser import ConfigParser
from workalendar.europe import CzechRepublic
import calendar
from dateutil.relativedelta import relativedelta
import sys


def database_object_definition():
    database_object_info_list = []
    # Definition of database object
    table_name = "d_exchange_rates_cnb"
    table_columns =  (
                ["date_valid",
                "country_name",
                "currency_name",
                "currency_ammount",
                "currency_code",
                "exchange_rate_amount"] )
    command = (
                """
                CREATE TABLE """ + table_name + """ ( 
                date_valid DATE NOT NULL, 
                country_name VARCHAR(100) NOT NULL,
                currency_name VARCHAR(100) NOT NULL,
                currency_ammount SMALLINT NOT NULL,
                currency_code CHAR(3) NOT NULL,
                exchange_rate_amount NUMERIC (7,3) NOT NULL,
                CONSTRAINT d_exchange_rates_cnb_pkey PRIMARY KEY (date_valid, currency_code)
                );
                """)
    #Add info about object to list as list
    database_object_info_list.insert(len(database_object_info_list),[table_name,table_columns,command])
    # Definition of database object
    table_name = "d_days_info"
    table_columns =  (
                ["date_valid",
                "date_exchange_rate",
                "annual_serial_number",
                "week_day",
                "working_day",
                "correction_needed"] )
    command = (
                """
                CREATE TABLE """ + table_name + """ ( 
                date_valid DATE PRIMARY KEY, 
                date_exchange_rate DATE NOT NULL,
                annual_serial_number SMALLINT NOT NULL,
                week_day SMALLINT NOT NULL,
                working_day BOOLEAN NOT NULL,
                correction_needed BOOLEAN NOT NULL
                );
                """)
    #Add info about object to list as list
    database_object_info_list.insert(len(database_object_info_list),[table_name,table_columns,command])
    return database_object_info_list


def config(filename='database.ini', section='postgresql'):
    path = os.path.realpath(os.path.dirname(__file__)) # database.ini file must be in the same path as this module
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(os.path.join(path,filename))
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


def database_postgre_connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn = None
    finally:
        return conn


def database_init_objects(conn, table_name, command): 
    try:           
        # create a cursor 
        cur = conn.cursor()
        # Check if exist table
        cur.execute("select exists(select from pg_tables where schemaname = 'public' and tablename  = '" + table_name + "');")
        results1 = cur.fetchone() # convert cursor to tuple
        results2 = results1[0] # to take value from tuple - it is boolean
        if results2:
            print ("Table " + table_name + " with old data or structure will be droped.")
            command_drop = "DROP TABLE " + table_name + " CASCADE;"
            cur.execute(command_drop)
            print ("Droped")
        print ("Table " + table_name + " will be created with actual structure and for inserting new data.")
        cur.execute(command)
        cur.execute("select exists(select from pg_tables where schemaname = 'public' and tablename  = '" + table_name + "');")
        results1 = cur.fetchone() # convert cursor to tuple
        results2 = results1[0] # to take value from tuple - it is boolean
        if results2:
            # commit the changes
            conn.commit()
            print ("Created")
            objects_done = True
        else:
            objects_done = False
            conn.rollback()
            print ("Something is wrong. Table " + table_name + " with actual structure is not created. Rollback is done.")
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        objects_done = False
        print(error)
        conn.rollback()
    finally:
        return objects_done


def reading_source(file_url, table_columns_1, table_columns_2, date_valid, week_day, working_day):
    try:
        row_values_1 = []
        row_values_2 = []
        response = requests.get(file_url)
        if (response.status_code):
            data = response.text
            # line-by-line processing
            for line in enumerate(data.split('\n')):        
                if line[0] == 0:
                    # first row of source
                    if line[1] == '':
                        print ("No data for " + date_valid + "!")
                        df_1 = pd.DataFrame() # empty data frame for return
                        df_2 = pd.DataFrame() # empty data frame for return
                        break # It is end of source
                    date_exchange_rate = line[1][6:10] + "-" + line[1][3:5] + "-" + line[1][0:2] # string format for inserting as date to db
                    annual_serial_number = int(line[1][12:len(line[1])])                    
                elif line[0] == 1:
                    pass # skip header of values in Czech language
                    if line[1] == '':
                        print ("No data for " + date_valid + "!")
                        df_1 = pd.DataFrame() # empty data frame for return
                        df_2 = pd.DataFrame() # empty data frame for return
                        break # It is end of source
                else:
                    #From third row with which is line[0] == 2 we have currency
                    row_values_1 = line[1].split('|') #
                    if row_values_1[0] == '':
                        if line[0] == 2:
                            print ("No data for " + date_valid + "!")
                            df_1 = pd.DataFrame() # empty data frame for return
                            df_2 = pd.DataFrame() # empty data frame for return
                        break # It is end of source
                    row_values_1[2] = int(row_values_1[2]) # convert string to integer for currency_ammount
                    row_values_1[4] = float(row_values_1[4].replace(",",".")) # convert string to float for exchange_rate_amount
                    row_values_1.insert(0,date_valid)
                    #row_values_1.insert(1,date_exchange_rate)
                    #row_values_1.insert(2,annual_serial_number)
                    if line[0] == 2:
                        # data frames creating
                        df_1 = pd.DataFrame([list(pd.Series(row_values_1))], columns = table_columns_1)
                        row_values_2.insert(0,date_valid)
                        row_values_2.insert(1,date_exchange_rate)
                        row_values_2.insert(2,annual_serial_number)
                        row_values_2.insert(3,week_day)
                        row_values_2.insert(4,working_day)
                        if date_valid != date_exchange_rate and working_day == True:
                            row_values_2.insert(5,True)
                        else:
                            row_values_2.insert(5,False)
                        df_2 = pd.DataFrame([list(pd.Series(row_values_2))], columns = table_columns_2) # this table has for each date_valid 1 row
                    else:
                        # new row added to data frame df_1
                        df_1.loc[len(df_1)] = row_values_1            
            #print(df_1)
            #print(df_2)
    except Exception as err:
        print(str(err))
        df_1 = pd.DataFrame() # empty data frame for return
        df_2 = pd.DataFrame() # empty data frame for return
    finally:
        return [df_1,df_2] # list of data frames is return


def execute_mogrify(conn, df_list, database_object_info_list):
    i = 0
    query = []
    tuples = []
    #Prepering inserts for all tables
    for df_list_item in df_list:
        # Create a list of tupples from the dataframe values
        tuples.insert(len(tuples),[tuple(x) for x in df_list_item.to_numpy()]) # command len(tuples) is using for finding of first free index in list
        # Comma-separated dataframe columns
        cols = ','.join(list(df_list_item.columns))
        cols_count = len(df_list_item.columns)
        cursor = conn.cursor()
        params_str = ("(" + "%s," * cols_count)[:-1]+ ")" # count of "%s" must be same as count of columns
        values = [cursor.mogrify(params_str, tup).decode('utf8') for tup in tuples[i]]
        # Create a list of inserts for each table
        query.insert(len(query), "INSERT INTO %s(%s) VALUES " % (database_object_info_list[i] [0], cols) + ",".join(values))
        i += 1
    try:
        j = 0
        for j in range (0,i):
            # Execute insert for each table 
            cursor.execute(query[j], tuples[j])
        # Commit for all is at the end
        conn.commit()
        insert_done = True
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()        
        insert_done = False
    finally:
        return insert_done
        cursor.close()

def init():
    try:
        #Connect to database
        conn = database_postgre_connect()
        if conn == None:
            raise Exception("Database objects are not created, please, check the issue.")
        #Creating of objects in database
        database_object_info_list = database_object_definition()
        for database_object_info_list_item in database_object_info_list:
            objects_done = database_init_objects(conn, database_object_info_list_item [0], database_object_info_list_item [2])
            if objects_done == False:
                 raise Exception("Database objects are not created, please, check th issue.")
        #Setting proper dates for import
        today_date = date.today()  # Today
        yesterday_date = today_date - relativedelta(days = 1)  # Get yesterdy
        three_months_ago_date = today_date - relativedelta(months = 3)  # Get date 3 months ago
        daterange = pd.date_range(three_months_ago_date, yesterday_date)
        #Inserting initial data data to database in cyccle for each day separately
        for processing_date in daterange:
            date_valid = processing_date.strftime('%Y-%m-%d')
            date_valid_cz = processing_date.strftime('%d.%m.%Y')
            week_day = calendar.weekday(processing_date.year, processing_date.month, processing_date.day) # index from 0 to 6 - Monday is 0
            #Finding if day is working
            cal = CzechRepublic()
            working_day = cal.is_working_day(processing_date)
            file_url = 'https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/denni_kurz.txt?date=' + date_valid_cz
            #Calling function for reading data from source
            df_list = reading_source(file_url, database_object_info_list[0] [1], database_object_info_list[1] [1], date_valid, week_day, working_day)
            if df_list[0].empty: # It is enough to check only first data frame - if empty then both empty
                #Nothing to insert to database
                print("Data from source not found for parameter: " + date_valid_cz)
            else:
                #Calling function for inserting data to database
                insert_done = execute_mogrify(conn, df_list, database_object_info_list)
                if insert_done:
                    init_done = True
                    print ("Insert for parameter: " + date_valid_cz + " is done.")
                else:
                    init_done = False
                    print ("Insert for parameter: " + date_valid_cz + " is not done.")
    except Exception as err:
        init_done = False
        print(str(err))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            return init_done


def increment(date_valid = date.today().strftime('%Y-%m-%d')): #Default date for increment is today in sting format: "YYYY-MM-DD"
    try:
        #Connect to database
        conn = database_postgre_connect()
        if conn == None:
            raise Exception("Database objects are not created, please, check the issue.")
        #Reading of objects in database
        database_object_info_list = database_object_definition()
        #Seeting parameters for increment
        date_valid_cz = date_valid[8:10] + "." + date_valid[5:7] + "." + date_valid[0:4] # convert to czech format of date in string
        processing_date = datetime.strptime(date_valid, '%Y-%m-%d')
        week_day = calendar.weekday(processing_date.year, processing_date.month, processing_date.day) # index from 0 to 6 - Monday is 0
        #Finding if day is working
        cal = CzechRepublic()
        working_day = cal.is_working_day(processing_date)
        file_url = 'https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/denni_kurz.txt?date=' + date_valid_cz
        #Calling function for reading data from source
        df_list = reading_source(file_url, database_object_info_list[0] [1], database_object_info_list[1] [1], date_valid, week_day, working_day)
        if df_list[0].empty: # It is enough to check only first data frame - if empty then both empty
            #Nothing to insert to database
            print("Data from source not found for parameter: " + date_valid_cz)
        else:
            #Calling function for inserting data to database
            insert_done = execute_mogrify(conn, df_list, database_object_info_list)
            if insert_done:
                increment_done = True
                print ("Insert for parameter: " + date_valid_cz + " is done.")
            else:
                increment_done = False
                print ("Insert for parameter: " + date_valid_cz + " is not done.")
    except Exception as err:
        increment_done = False
        print(str(err))
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            return increment_done

def main():
#if __name__ == "__main__":
    if sys.argv[1:] == []:
        print("Possible calls:")
        print ("your_folder_for_python\python.exe exchange_rates_cnb.py init")
        print ("   - it do init to database")
        print ("your_folder_for_pytho n\python.exe exchange_rates_cnb.py increment")
        print ("   - it do increment to database with today data (but first must be init)")
        print ("your_folder_for_python\python.exe exchange_rates_cnb.py increment date_string")
        print ("   - it do increment to database with date as parametr, format for date_string is YYYY-MM-DD, eample is 2023-12-31 (but first must be init)")
    elif sys.argv[1] == 'init':
        print("Calling of fumction init starting.")
        init()
    elif sys.argv[1] == 'increment':
        print("Calling of fumction init starting.")
        if len(sys.argv[1:]) == 1:
            increment()
        else:
            increment(sys.argv[2])
    else:
        print ("This not calling of proper arguments.")
        print ("Possible calls:")
        print ("your_folder_for_python\python.exe -m exchange_rates_cnb.py init")
        print ("   - it do init to database")
        print ("your_folder_for_python\python.exe -m exchange_rates_cnb.py increment")
        print ("   - it do increment to database with today data (but first must be init)")
        print ("your_folder_for_python\python.exe -m exchange_rates_cnb.py increment date_string")
        print ("   - it do increment to database with date as parametr, format for date_string is YYYY-MM-DD, eample is 2023-12-31 (but first must be init)")