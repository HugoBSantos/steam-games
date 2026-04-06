CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.supported_languages(
    language_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.categories(
	category_id SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tags(
	tag_id SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.developers(
    developer_id SERIAL PRIMARY KEY, 
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.genres(
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.publishers(
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.games(
	game_id INT PRIMARY KEY,
	developer_id INT REFERENCES silver.developers (developer_id),
	publisher_id INT REFERENCES silver.publishers (publisher_id),
	name VARCHAR(100) NOT NULL,
	release_date DATE NOT NULL,
	estimated_owners VARCHAR(100) NOT NULL,
	peak_ccu INT NOT NULL,
	required_age SMALLINT NOT NULL,
	price DECIMAL NOT NULL,
	discount SMALLINT NOT NULL,
	dlc_count SMALLINT NOT NULL,
	about_the_game TEXT,
	reviews TEXT,
	header_image VARCHAR(100) NOT NULL,
	website VARCHAR(100),
	support_url VARCHAR(100),
	support_email VARCHAR(100),
	notes TEXT,
	windows BOOLEAN NOT NULL,
	mac BOOLEAN NOT NULL,
	linux BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tags_games(
    game_id INT NOT NULL REFERENCES silver.games (game_id),
    tags_id INT NOT NULL REFERENCES silver.tags (tag_id),
    PRIMARY KEY(game_id, tags_id)
);

CREATE TABLE IF NOT EXISTS silver.game_genre(
    game_id INT NOT NULL REFERENCES silver.games (game_id),
    genre_id INT NOT NULL REFERENCES silver.genres (genre_id),
    PRIMARY KEY(game_id, genre_id)
);

CREATE TABLE IF NOT EXISTS silver.game_language(
    game_id INT NOT NULL REFERENCES silver.games (game_id),
    language_id INT NOT NULL REFERENCES silver.supported_languages (language_id),
    has_full_audio BOOLEAN NOT NULL,
    PRIMARY KEY (game_id, language_id)
);

CREATE TABLE IF NOT EXISTS silver.game_category(
    game_id INT NOT NULL REFERENCES silver.games (game_id),
    category_id INT NOT NULL REFERENCES silver.categories (category_id),
    PRIMARY KEY(game_id, category_id)
);

CREATE TABLE IF NOT EXISTS silver.game_developer(
	game_id INT NOT NULL REFERENCES silver.games (game_id),
	developer_id INT NOT NULL REFERENCES silver.developers (developer_id),
	PRIMARY KEY(game_id, developer_id)
);

CREATE TABLE IF NOT EXISTS silver.game_publisher(
	game_id INT NOT NULL REFERENCES silver.games (game_id),
	publisher_id INT NOT NULL REFERENCES silver.publishers (publisher_id),
	PRIMARY KEY(game_id, publisher_id)
);

CREATE TABLE IF NOT EXISTS silver.metrics(
	metric_id SERIAL PRIMARY KEY,
	game_id INT UNIQUE REFERENCES silver.games (game_id),
	metacritic_score SMALLINT NOT NULL,
	metacritic_url VARCHAR(100),
	user_score SMALLINT NOT NULL,
	positive INT NOT NULL,
	negative INT NOT NULL,
	score_rank SMALLINT,
	achievements SMALLINT NOT NULL,
	recommendations INT,
	average_playtime_forever INT NOT NULL,
	average_playtime_two_weeks INT NOT NULL,
	median_playtime_forever INT NOT NULL,
	median_playtime_two_weeks INT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.screenshots(
    screenshot_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES silver.games (game_id),
    url VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.movies(
    movie_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES silver.games (game_id),
    url VARCHAR(100) NOT NULL
);