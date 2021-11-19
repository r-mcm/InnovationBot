import psycopg2
from decouple import config

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Create the table in SQL
cur.execute("""
    CREATE TABLE accounts(
    AcccountName text PRIMARY KEY,
    Region text,
    Portfolio text,
    SubPortfolio text,
    TwitterHandle text
)
""")

#Copy data from csv into the SQL table
with open('SQLTablesCSV.csv', 'r', encoding='utf8') as f:
    next(f) # Skip the header row.
    cur.copy_from(f, 'accounts', sep=',')

#Commit the transaction
conn.commit()