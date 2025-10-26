# run_schema.py
import sqlite3

# Connect to your database (will create movies.db if it doesn't exist)
conn = sqlite3.connect('movies.db')
cur = conn.cursor()

# Read the schema.sql file
with open('schema.sql', 'r') as f:
    sql_script = f.read()

# Execute all commands in schema.sql
cur.executescript(sql_script)

conn.commit()
conn.close()

print("✅ Schema applied successfully!")
