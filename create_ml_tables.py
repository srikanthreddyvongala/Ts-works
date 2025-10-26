# create_ml_tables.py
from sqlalchemy import create_engine, text

engine = create_engine('sqlite:///movies.db', echo=False)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS movies (
            movieId INTEGER PRIMARY KEY,
            title TEXT,
            genres TEXT
        );
    """))
    
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ratings (
            userId INTEGER,
            movieId INTEGER,
            rating REAL,
            timestamp INTEGER
        );
    """))
    
    conn.commit()

print("✅ Tables 'movies' and 'ratings' created successfully in movies.db.")
