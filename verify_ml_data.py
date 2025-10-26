# verify_ml_data.py
import sqlite3

conn = sqlite3.connect('movies.db')
cur = conn.cursor()

print("🎬 Movies sample:")
for row in cur.execute('SELECT * FROM movies LIMIT 5;'):
    print(row)

print("\n⭐ Ratings sample:")
for row in cur.execute('SELECT * FROM ratings LIMIT 5;'):
    print(row)

conn.close()
