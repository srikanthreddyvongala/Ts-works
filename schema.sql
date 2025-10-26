
CREATE TABLE IF NOT EXISTS movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS genres (
    genreId INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INTEGER NOT NULL,
    genreId INTEGER NOT NULL,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genreId) REFERENCES genres(genreId) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS ratings (
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER,
    PRIMARY KEY (userId, movieId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS movie_details (
    movieId INTEGER PRIMARY KEY,
    Director TEXT,
    Actors TEXT,
    Genre TEXT,
    Runtime TEXT,
    Plot TEXT,
    Language TEXT,
    Country TEXT,
    Awards TEXT,
    BoxOffice TEXT,
    Metascore TEXT,
    imdbRating TEXT,
    Type TEXT,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);
