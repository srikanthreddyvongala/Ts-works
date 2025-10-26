# create_db.py
from sqlalchemy import create_engine, text

# Create SQLite database file (auto-created)
engine = create_engine('sqlite:///movies.db', echo=False)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            genre TEXT,
            release_year INTEGER,
            rating REAL
        );
    """))
    conn.commit()
print("✅ Database 'movies.db' and table 'movies' created successfully.")
