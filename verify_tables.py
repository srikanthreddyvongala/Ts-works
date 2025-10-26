import sqlite3

conn = sqlite3.connect('movies.db')
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
print("Tables in database:", tables)

conn.close()
