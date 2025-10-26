# ---------------- 1a: Imports & Config ----------------
import os
import re
import time
import pandas as pd
import requests
from sqlalchemy import create_engine

# ---------------- CONFIG ----------------
DB_URL = 'sqlite:///movies.db'
OMDB_KEY = os.getenv('OMDB_API_KEY')  # make sure your key is set
MAX_MOVIES = 100           # for testing; increase later
SLEEP_SECONDS = 0.25       # polite delay between API calls

# ---------------- 1b: Helper Functions ----------------
def clean_data(df_movies, df_ratings):
    df_movies['title'] = df_movies['title'].fillna('Unknown')
    df_movies['genres'] = df_movies['genres'].fillna('Unknown')
    df_ratings['rating'] = df_ratings['rating'].fillna(0).astype(float)
    df_ratings['timestamp'] = df_ratings['timestamp'].fillna(0).astype(int)
    df_movies['movieId'] = df_movies['movieId'].astype(int)
    df_ratings['movieId'] = df_ratings['movieId'].astype(int)
    df_ratings['userId'] = df_ratings['userId'].astype(int)
    return df_movies, df_ratings

def parse_title_and_year(title_str):
    if not isinstance(title_str, str):
        return (None, None)
    match = re.match(r'^(.*)\s+\((\d{4})\)\s*$', title_str.strip())
    if match:
        return match.group(1).strip(), int(match.group(2))
    else:
        return title_str.strip(), None

def query_omdb(title, year=None):
    if not OMDB_KEY:
        raise SystemExit("ERROR: OMDB_API_KEY not set")
    params = {'apikey': OMDB_KEY, 't': title}
    if year:
        params['y'] = str(year)
    try:
        resp = requests.get('http://www.omdbapi.com/', params=params, timeout=10)
        data = resp.json()
        if data.get('Response') == 'True':
            return {
                'imdbID': data.get('imdbID'),
                'Title': data.get('Title'),
                'Year': data.get('Year'),
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
            return None
    except requests.RequestException as e:
        print(f"Request error for '{title}': {e}")
        return None

def enrich_movies(df_movies, df_omdb):
    return df_movies.merge(df_omdb, left_on='movieId', right_on='movieId', how='left')

def parse_genres(df):
    df['genre_list'] = df['genres'].apply(lambda x: x.split('|') if pd.notnull(x) else [])
    return df

def add_decade_column(df):
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df['decade'] = (df['Year'] // 10 * 10).fillna(0).astype(int)
    return df

# ---------------- 1c: Extract Function ----------------
def extract_movies():
    movies_df = pd.read_csv('movies.csv').head(MAX_MOVIES)
    ratings_df = pd.read_csv('ratings.csv')
    print(f"Extracted {len(movies_df)} movies and {len(ratings_df)} ratings from CSV")
    enriched_movies = []
    for _, row in movies_df.iterrows():
        movieId = row['movieId']
        raw_title = row['title']
        title, year = parse_title_and_year(raw_title)
        info = query_omdb(title, year)
        if info is None and year is not None:
            time.sleep(SLEEP_SECONDS)
            info = query_omdb(title, None)
        if info:
            info['movieId'] = movieId
            info['raw_title'] = raw_title
            enriched_movies.append(info)
            print(f"[OK] {title} ({year})")
        else:
            enriched_movies.append({
                'movieId': movieId,
                'raw_title': raw_title,
                'imdbID': None,
                'Title': title,
                'Year': year,
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
            print(f"[MISS] {title} ({year}) not found in OMDb")
        time.sleep(SLEEP_SECONDS)
    enriched_df = pd.DataFrame(enriched_movies)
    return enriched_df, ratings_df

# ---------------- 2: Extract + Transform ----------------
def extract_and_transform():
    df_movies = pd.read_csv('movies.csv').head(MAX_MOVIES)
    df_ratings = pd.read_csv('ratings.csv')
    df_omdb, _ = extract_movies()
    df_movies, df_ratings = clean_data(df_movies, df_ratings)
    df_enriched = enrich_movies(df_movies, df_omdb)
    df_enriched = parse_genres(df_enriched)
    df_enriched = add_decade_column(df_enriched)
    return df_enriched, df_ratings

# ---------------- 3: Load Function ----------------
def load_to_db(df_enriched, df_ratings, db_url=DB_URL):
    engine = create_engine(db_url)
    # Movies table
    df_movies = df_enriched[['movieId', 'Title', 'Year']].drop_duplicates()
    df_movies.rename(columns={'Title':'title','Year':'year'}, inplace=True)
    df_movies.to_sql('movies', engine, if_exists='replace', index=False)
    # Genres table
    all_genres = set(g for glist in df_enriched['genre_list'] for g in glist)
    df_genres = pd.DataFrame({'name': list(all_genres)})
    df_genres['genreId'] = df_genres.index + 1
    df_genres.to_sql('genres', engine, if_exists='replace', index=False)
    genre_map = dict(zip(df_genres['name'], df_genres['genreId']))
    # Movie-Genres table
    rows = []
    for _, row in df_enriched.iterrows():
        movieId = row['movieId']
        for genre in row['genre_list']:
            if genre:
                rows.append({'movieId': movieId, 'genreId': genre_map[genre]})
    df_movie_genres = pd.DataFrame(rows)
    df_movie_genres.to_sql('movie_genres', engine, if_exists='replace', index=False)
    # Ratings table
    df_ratings.to_sql('ratings', engine, if_exists='replace', index=False)
    # Movie details table
    details_cols = ['movieId','imdbID','Director','Actors','Plot','BoxOffice','Runtime',
                    'Language','Country','Awards','Metascore','imdbRating','Type']
    df_details = df_enriched[details_cols].drop_duplicates()
    df_details.to_sql('movie_details', engine, if_exists='replace', index=False)
    print("✅ Data loaded to database successfully!")

# ---------------- 4: Main Function ----------------
def main():
    df_enriched, df_ratings = extract_and_transform()
    load_to_db(df_enriched, df_ratings, DB_URL)
    print(f"✅ ETL completed: {len(df_enriched)} movies enriched, {len(df_ratings)} ratings saved")

# ---------------- 5: Run Script ----------------
if __name__ == '__main__':
    main()
