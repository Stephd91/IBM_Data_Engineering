# Import libraries required for connecting to mysql
import mysql.connector
import datetime

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# ****************** Connect to MySQL ******************
## connect to database
mysql_connection = mysql.connector.connect(user='root', password='MTg3ODAtc3RlcGhh',host='127.0.0.1',database='sales')
## create cursor
mysql_cursor = mysql_connection.cursor()

# ****************** Connect to DB2 or PostgreSql ******************
## connection details
dsn_hostname = '127.0.0.1'
dsn_user='postgres'
dsn_pwd ='MjE4NjItc3RlcGhh'
dsn_port ='5432'
dsn_database ='postgres'  

## create connection
psql_connection = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)
## create cursor
psql_cursor = psql_connection.cursor()

# ****************** TASK 1 ******************
# Find out the last rowid from PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the PostgreSql.
def get_last_rowid():
    SQL = "SELECT rowid FROM public.sales_data ORDER BY rowid DESC LIMIT 1;"
    psql_cursor.execute(SQL)
    last_row = psql_cursor.fetchall()
    last_rowid = last_row[0][0]
    psql_connection.commit()
    #psql_connection.close()
    return last_rowid


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# ****************** TASK 2 ******************
# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = f"SELECT * FROM sales_data WHERE rowid > {rowid};"
    mysql_cursor.execute(SQL)
    new_records = mysql_cursor.fetchall()
    mysql_connection.commit()
    mysql_connection.close()
    return new_records

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# ****************** TASK 3 ******************
# Insert the additional records from MySQL into PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in PostgreSql.

def insert_records(records):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Get the current timestamp for each row
    for row in records:
        SQL = "INSERT INTO public.sales_data (rowid, product_id, customer_id, quantity, timeestamp) VALUES (%s, %s, %s, %s, %s)"
        data = (row[0], row[1], row[2], row[3], timestamp)  # Include the timestamp in the data tuple
        psql_cursor.execute(SQL, data)
        psql_connection.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
psql_connection.close()
