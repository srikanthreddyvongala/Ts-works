import pandas as pd
from sqlalchemy import create_engine

# ---------------- CONFIG ----------------
DB_URL = 'sqlite:///movies.db'
engine = create_engine(DB_URL)

# ---------------- Q1: Movie with highest average rating ----------------
query1 = """
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId
ORDER BY avg_rating DESC
LIMIT 1;
"""
df_q1 = pd.read_sql(query1, engine)
print("Q1: Movie with highest average rating")
print(df_q1, "\n")

# ---------------- Q2: Top 5 genres with highest average rating ----------------
query2 = """
SELECT g.name AS genre, AVG(r.rating) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.genreId = mg.genreId
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genreId
ORDER BY avg_rating DESC
LIMIT 5;
"""
df_q2 = pd.read_sql(query2, engine)
print("Q2: Top 5 genres with highest average rating")
print(df_q2, "\n")

# ---------------- Q3: Director with most movies ----------------
query3 = """
SELECT md.Director, COUNT(*) AS movie_count
FROM movie_details md
GROUP BY md.Director
ORDER BY movie_count DESC
LIMIT 1;
"""
df_q3 = pd.read_sql(query3, engine)
print("Q3: Director with most movies")
print(df_q3, "\n")

# ---------------- Q4: Average rating per year ----------------
query4 = """
SELECT m.year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.year
ORDER BY m.year;
"""
df_q4 = pd.read_sql(query4, engine)
print("Q4: Average rating per year")
print(df_q4, "\n")
