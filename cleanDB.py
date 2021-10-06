import psycopg2
from decouple import config

# Assign user and password for database
user = config('user',default='')
password = config('password',default='')

# Connect to the postgres DB
conn = psycopg2.connect("dbname=tweetsDB user=" + user +  " password=" + password)

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute the SQL query
cur.execute("""
    DROP TABLE tweets
""")
conn.commit()