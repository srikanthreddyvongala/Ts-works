# verify.py
import sqlite3

conn = sqlite3.connect('movies.db')
cur = conn.cursor()

print("🎬 Movies table:")
for row in cur.execute('SELECT * FROM movies'):
    print(row)

print("\n🌟 Transformed_movies table:")
for row in cur.execute('SELECT * FROM transformed_movies'):
    print(row)

conn.close()
