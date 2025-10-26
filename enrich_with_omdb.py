
# enrich_with_omdb.py
"""
Enrich MovieLens movies with OMDb data and save to 'movie_details' table.

Usage:
  1) Make sure your virtual environment is activated and packages installed:
       pip install pandas sqlalchemy requests

  2) Set your OMDB_API_KEY as an env var (see instructions).

  3) Run:
       python enrich_with_omdb.py

Notes:
  - Default processes first N movies (N=100). Change MAX_MOVIES below to process more.
  - OMDb free key has limits; don't bulk-query thousands at once.
"""

import os
import re
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# -------------- CONFIG --------------
DB_URL = 'sqlite:///movies.db'
OMDB_KEY = os.getenv('OMDB_API_KEY')  # required
MAX_MOVIES = 500  # CHANGE this to a larger number when you are sure of limits
SLEEP_SECONDS = 0.5 # delay between requests to be polite (increase if you hit rate limits)
# ------------------------------------

if not OMDB_KEY:
    raise SystemExit("ERROR: OMDB_API_KEY environment variable not set. See script header for instructions.")

engine = create_engine(DB_URL, echo=False)

def parse_title_and_year(title_str):
    """
    MovieLens titles often look like: "Toy Story (1995)"
    Returns (title_text, year) where year is int or None.
    """
    if not isinstance(title_str, str):
        return (None, None)
    # Try to extract year in parentheses at end
    m = re.match(r'^(.*)\s+\((\d{4})\)\s*$', title_str.strip())
    if m:
        title = m.group(1).strip()
        year = int(m.group(2))
        return (title, year)
    else:
        # fallback: return full title, no year
        return (title_str.strip(), None)

def query_omdb(title, year=None):
    """
    Query OMDb by title (and year if available).
    Returns dict of selected fields, or None if not found.
    """
    params = {'apikey': OMDB_KEY, 't': title}
    if year:
        params['y'] = str(year)
    try:
        resp = requests.get('http://www.omdbapi.com/', params=params, timeout=10)
    except requests.RequestException as e:
        print(f"Request error for '{title}' ({year}): {e}")
        return None

    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} for '{title}' ({year})")
        return None

    data = resp.json()
    if data.get('Response') == 'True':
        # pick useful fields (may be 'N/A')
        return {
            'title_searched': title,
            'year_searched': year,
            'imdbID': data.get('imdbID'),
            'omdb_title': data.get('Title'),
            'omdb_year': data.get('Year'),
            'Director': data.get('Director'),
            'Actors': data.get('Actors'),
            'Genre': data.get('Genre'),
            'Runtime': data.get('Runtime'),
            'Plot': data.get('Plot'),
            'Language': data.get('Language'),
            'Country': data.get('Country'),
            'Awards': data.get('Awards'),
            'BoxOffice': data.get('BoxOffice'),
            'Metascore': data.get('Metascore'),
            'imdbRating': data.get('imdbRating'),
            'Type': data.get('Type')
        }
    else:
        # not found
        # print reason if available
        # data might have "Error": "Movie not found!"
        # We'll return None to indicate not found
        return None

def main():
    # Read movies from DB
    df_movies = pd.read_sql('SELECT movieId, title FROM movies', engine)
    print(f"Total movies in DB: {len(df_movies)}")
    df_movies = df_movies.head(MAX_MOVIES)  # limit to avoid exceeding free quota
    print(f"Processing first {len(df_movies)} movies (MAX_MOVIES={MAX_MOVIES}).")

    results = []
    for idx, row in df_movies.iterrows():
        movieId = row['movieId']
        raw_title = row['title']
        title, year = parse_title_and_year(raw_title)
        if not title:
            print(f"Skipping movieId {movieId} due to invalid title: {raw_title}")
            continue

        # Query OMDb
        info = query_omdb(title, year)
        if info is None:
            # Try again with title only (if year was given and failed)
            if year is not None:
                time.sleep(SLEEP_SECONDS)
                info = query_omdb(title, None)

        if info:
            info['movieId'] = movieId
            info['raw_title'] = raw_title
            results.append(info)
            print(f"[OK] movieId={movieId} -> {info.get('omdb_title')} ({info.get('omdb_year')})")
        else:
            # keep a record of failed lookup (so you can inspect later)
            results.append({
                'movieId': movieId,
                'raw_title': raw_title,
                'title_searched': title,
                'year_searched': year,
                'imdbID': None,
                'omdb_title': None,
                'omdb_year': None,
                'Director': None,
                'Actors': None,
                'Genre': None,
                'Runtime': None,
                'Plot': None,
                'Language': None,
                'Country': None,
                'Awards': None,
                'BoxOffice': None,
                'Metascore': None,
                'imdbRating': None,
                'Type': None
            })
            print(f"[MISS] movieId={movieId} -> '{title}' ({year}) not found in OMDb")

        time.sleep(SLEEP_SECONDS)

    # Save to SQL table 'movie_details'
    df_out = pd.DataFrame(results)
    # create/replace the movie_details table
    df_out.to_sql('movie_details', engine, if_exists='replace', index=False)
    print(f"Saved {len(df_out)} rows to 'movie_details' table in movies.db")

if __name__ == '__main__':
    main()
