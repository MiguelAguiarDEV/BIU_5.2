-- ====================================
-- MovieBind Database compatible con MySQL 8
-- ====================================
CREATE DATABASE IF NOT EXISTS moviebind;
USE moviebind;

-- 1) BORRAR TABLAS SI YA EXISTEN
DROP TABLE IF EXISTS movie_keywords;
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS movie_actors;
DROP TABLE IF EXISTS contracts;
DROP TABLE IF EXISTS keywords;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS actors;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS contract_types;
DROP TABLE IF EXISTS profiles;
DROP TABLE IF EXISTS users;

-- 2) CREAR TABLAS

CREATE TABLE users (
  nickname          VARCHAR(50) PRIMARY KEY,
  password          VARCHAR(255) NOT NULL,
  email             VARCHAR(100) NOT NULL UNIQUE,
  registration_date DATE NOT NULL
);

CREATE TABLE profiles (
  user_nickname  VARCHAR(50) PRIMARY KEY,
  dni            VARCHAR(20) NOT NULL UNIQUE,
  first_name     VARCHAR(50),
  last_name      VARCHAR(50),
  age            INT,
  mobile_number  VARCHAR(20),
  birth_date     DATE,
  FOREIGN KEY (user_nickname) REFERENCES users(nickname) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE contract_types (
  code        VARCHAR(20) PRIMARY KEY,
  description VARCHAR(100)
);

CREATE TABLE contracts (
  user_nickname VARCHAR(50),
  dni VARCHAR(20),
  contract_type VARCHAR(20),
  address VARCHAR(100),
  city VARCHAR(50),
  postal_code VARCHAR(20),
  country VARCHAR(50),
  start_date DATE,
  end_date DATE,
  PRIMARY KEY (user_nickname, contract_type),
  FOREIGN KEY (user_nickname) REFERENCES users(nickname) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (contract_type) REFERENCES contract_types(code) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE movies (
  title           VARCHAR(100),
  director        VARCHAR(100),
  duration        INT,
  color           BOOLEAN,
  aspect_ratio    VARCHAR(10),
  release_year    INT,
  age_rating      VARCHAR(10),
  country         VARCHAR(50),
  language        VARCHAR(20),
  budget          INT,
  gross_income    INT,
  imdb_link       VARCHAR(255),
  PRIMARY KEY (title, director)
);

CREATE TABLE actors (
  name VARCHAR(100) PRIMARY KEY
);

CREATE TABLE movie_actors (
  movie_title    VARCHAR(100),
  movie_director VARCHAR(100),
  actor_name     VARCHAR(100),
  PRIMARY KEY (movie_title, movie_director, actor_name),
  FOREIGN KEY (movie_title, movie_director) REFERENCES movies(title, director) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (actor_name) REFERENCES actors(name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE genres (
  name VARCHAR(50) PRIMARY KEY
);

CREATE TABLE movie_genres (
  movie_title    VARCHAR(100),
  movie_director VARCHAR(100),
  genre_name     VARCHAR(50),
  PRIMARY KEY (movie_title, movie_director, genre_name),
  FOREIGN KEY (movie_title, movie_director) REFERENCES movies(title, director) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (genre_name) REFERENCES genres(name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE keywords (
  word VARCHAR(50) PRIMARY KEY
);

CREATE TABLE movie_keywords (
  movie_title    VARCHAR(100),
  movie_director VARCHAR(100),
  keyword_word   VARCHAR(50),
  PRIMARY KEY (movie_title, movie_director, keyword_word),
  FOREIGN KEY (movie_title, movie_director) REFERENCES movies(title, director) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (keyword_word) REFERENCES keywords(word) ON DELETE CASCADE ON UPDATE CASCADE
);

-- 3) POBLAR CON DATOS

INSERT INTO contract_types(code, description) VALUES
  ('C001','Basic Plan'),
  ('C002','Premium Plan'),
  ('C003','Family Plan'),
  ('C004','Student Plan'),
  ('C005','Pay-per-view');

INSERT INTO users(nickname, password, email, registration_date) VALUES
  ('cinephile89','Passw0rd!','cinephile89@example.com','2025-01-10'),
  ('filmBuff','Secr3tPwd','filmbuff@example.com','2025-01-12'),
  ('popcornLover','M1pl4n4pwd','popcornlover@example.com','2025-01-15');

INSERT INTO profiles(user_nickname, dni, first_name, last_name, age, mobile_number, birth_date) VALUES
  ('cinephile89','12345678A','Juan','Pérez',35,'612345678','1990-05-15'),
  ('filmBuff','23456789B','María','García',28,'623456789','1997-07-22'),
  ('popcornLover','34567890C','Luis','Fernández',42,'634567890','1983-03-10');

INSERT INTO contracts(user_nickname, dni, contract_type, address, city, postal_code, country, start_date, end_date) VALUES
  ('cinephile89','12345678A','C001','Calle Mayor 1','Madrid','28013','España','2024-01-15','2025-01-15'),
  ('filmBuff','23456789B','C002','Av. Constitución 2','Sevilla','41001','España','2024-01-20','2025-01-20'),
  ('popcornLover','34567890C','C003','C/ Alcalá 45','Madrid','28014','España','2024-02-05','2025-02-05');

INSERT INTO movies(title, director, duration, color, aspect_ratio, release_year, age_rating, country, language, budget, gross_income, imdb_link) VALUES
  ('El Gran Viaje','Pedro Almodóvar',120,1,'16:9',2020,'PG-13','España','es',1000000,5000000,'https://www.imdb.com/title/tt1234567/'),
  ('La Aventura','Sofia Coppola',95,1,'16:9',2019,'PG','USA','en',2000000,8000000,'https://www.imdb.com/title/tt7654321/');

INSERT INTO actors(name) VALUES
  ('Penélope Cruz'),
  ('Tom Hanks');

INSERT INTO movie_actors(movie_title, movie_director, actor_name) VALUES
  ('El Gran Viaje','Pedro Almodóvar','Penélope Cruz'),
  ('La Aventura','Sofia Coppola','Tom Hanks');

INSERT INTO genres(name) VALUES
  ('Drama'),
  ('Adventure');

INSERT INTO movie_genres(movie_title, movie_director, genre_name) VALUES
  ('El Gran Viaje','Pedro Almodóvar','Drama'),
  ('La Aventura','Sofia Coppola','Adventure');

INSERT INTO keywords(word) VALUES
  ('viaje'),
  ('amistad');

INSERT INTO movie_keywords(movie_title, movie_director, keyword_word) VALUES
  ('El Gran Viaje','Pedro Almodóvar','viaje'),
  ('La Aventura','Sofia Coppola','amistad');

-- ====================
-- FIN DEL SCRIPT SQL
-- ====================
