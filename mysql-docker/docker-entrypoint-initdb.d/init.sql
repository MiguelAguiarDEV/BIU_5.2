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
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS views;
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
  PRIMARY KEY (user_nickname, contract_type, start_date),
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
  imdb_rating     DECIMAL(3,1),
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

CREATE TABLE views (
  user_nickname  VARCHAR(50),
  movie_title    VARCHAR(100),
  movie_director VARCHAR(100),
  view_date      DATE,
  PRIMARY KEY (user_nickname, movie_title, movie_director, view_date),
  FOREIGN KEY (user_nickname) REFERENCES users(nickname),
  FOREIGN KEY (movie_title, movie_director) REFERENCES movies(title, director)
);

CREATE TABLE ratings (
  imdb_id        VARCHAR(20),
  user_nickname  VARCHAR(50),
  movie_title    VARCHAR(100),
  movie_director VARCHAR(100),
  rating         DECIMAL(2,1),
  comment        TEXT,
  rating_date    DATE,
  PRIMARY KEY (imdb_id, user_nickname),
  FOREIGN KEY (user_nickname) REFERENCES users(nickname),
  FOREIGN KEY (movie_title, movie_director) REFERENCES movies(title, director)
);

-- 3) POBLAR CON MUCHOS DATOS
-- Contract types
INSERT INTO contract_types(code, description) VALUES
  ('C001','Basic Plan'),
  ('C002','Premium Plan'),
  ('C003','Family Plan'),
  ('C004','Student Plan');

-- Users & Profiles
INSERT INTO users(nickname, password, email, registration_date) VALUES
  ('cinephile89','Passw0rd!','cinephile89@example.com','2025-01-10'),
  ('filmBuff','Secr3tPwd','filmbuff@example.com','2025-01-12'),
  ('popcornLover','M1pl4n4pwd','popcornlover@example.com','2025-01-15'),
  ('classicFan','OldSch00l','classicfan@example.com','2025-01-18'),
  ('docuHunter','D0cu4llDay','docuhunter@example.com','2025-01-20'),
  ('kidsZone','K1dzOnly!','kidszone@example.com','2025-01-22'),
  ('ultraViewer','4kMaxPow','ultraviewer@example.com','2025-01-25'),
  ('globexUser','Int3rCine!','globexuser@example.com','2025-01-27'),
  ('cineKing','KingC1ne','cineking@example.com','2025-02-01'),
  ('dramaQueen','QWeDrama!','dramaqueen@example.com','2025-02-03'),
  ('comedyHero','HaHaHa123','comedyhero@example.com','2025-02-05'),
  ('actionFan','Go4it!','actionfan@example.com','2025-02-07'),
  ('horrorLord','Sc@ryM0vie','horrorlord@example.com','2025-02-10'),
  ('sciFiGeek','Futur3IsNow','scifigeek@example.com','2025-02-12'),
  ('romanticSoul','LoveStory','romanticsoul@example.com','2025-02-14'),
  ('thrillerSeeker','EdgeOfSeat','thrillerseeker@example.com','2025-02-16'),
  ('mysteryMind','WhoDunIt','mysterymind@example.com','2025-02-18'),
  ('animationAddict','Drawn2Fun','animationaddict@example.com','2025-02-20'),
  ('biopicBuff','TrueStory','biopicbuff@example.com','2025-02-22'),
  ('indieFan','UnderTheRadar','indiefan@example.com','2025-02-24'),
  ('oldUser1','OldPass1','olduser1@example.com','2024-01-01'),
  ('oldUser2','OldPass2','olduser2@example.com','2023-10-01');

INSERT INTO profiles(user_nickname,dni,first_name,last_name,age,mobile_number,birth_date) VALUES
  ('cinephile89','12345678A','Juan','Pérez',35,'612345678','1990-05-15'),
  ('filmBuff','23456789B','María','García',28,'623456789','1997-07-22'),
  ('popcornLover','34567890C','Luis','Fernández',42,'634567890','1983-03-10'),
  ('classicFan','45678901D','Carmen','López',60,'645678901','1964-11-02'),
  ('docuHunter','56789012E','Andrés','Martínez',33,'656789012','1991-06-18'),
  ('kidsZone','67890123F','Sofía','Núñez',12,'667890123','2012-09-30'),
  ('ultraViewer','78901234G','Carlos','Gómez',40,'678901234','1985-12-25'),
  ('globexUser','89012345H','Lucía','Ramírez',29,'689012345','1996-04-08'),
  ('cineKing','90123456I','Pedro','Navarro',38,'691234567','1987-10-05'),
  ('dramaQueen','01234567J','Eva','Moreno',27,'602345678','1998-08-19'),
  ('comedyHero','11234567K','Tomás','Sánchez',31,'613456789','1993-02-11'),
  ('actionFan','21234568L','David','Ortiz',45,'624567890','1979-09-01'),
  ('horrorLord','31234569M','Ana','Ruiz',22,'635678901','2002-11-11'),
  ('sciFiGeek','41234570N','Miguel','Vega',26,'646789012','1999-03-23'),
  ('romanticSoul','51234571O','Sara','Blanco',30,'657890123','1995-12-02'),
  ('thrillerSeeker','61234572P','Javier','Castro',34,'668901234','1991-04-27'),
  ('mysteryMind','71234573Q','Lorena','Díaz',29,'679012345','1996-06-06'),
  ('animationAddict','81234574R','Raúl','Herrera',24,'690123456','2001-05-15'),
  ('biopicBuff','91234575S','Isabel','Gómez',50,'601234567','1975-07-30'),
  ('indieFan','01324576T','Héctor','Suárez',39,'612345670','1985-01-22'),
  ('oldUser1','99999999A','Antiguo','Usuario1',50,'600000001','1975-01-01'),
  ('oldUser2','88888888B','Antigua','Usuario2',55,'600000002','1970-01-01');

-- Contracts
INSERT INTO contracts(user_nickname,dni,contract_type,address,city,postal_code,country,start_date,end_date) VALUES
  ('cinephile89','12345678A','C001','Calle Mayor 1','Madrid','28013','España','2024-01-15','2025-01-15'),
  ('filmBuff','23456789B','C002','Av. Constitución 2','Sevilla','41001','España','2024-01-20','2025-01-20'),
  ('popcornLover','34567890C','C003','C/ Alcalá 45','Madrid','28014','España','2024-02-05','2025-02-05'),
  ('classicFan','45678901D','C004','C/ Cervantes 10','Barcelona','08001','España','2024-03-01','2025-03-01'),
  ('ultraViewer','78901234G','C001','C/ Prueba 1','Madrid','28001','España','2024-05-01','2025-05-20'),
  ('globexUser','89012345H','C002','C/ Prueba 2','Madrid','28002','España','2024-05-01','2025-05-25');

-- Movies
INSERT INTO movies(title,director,duration,color,aspect_ratio,release_year,age_rating,country,language,budget,gross_income,imdb_link,imdb_rating) VALUES
  ('El Gran Viaje','Pedro Almodóvar',120,1,'16:9',2020,'PG-13','España','es',1000000,5000000,'https://www.imdb.com/title/tt1234567/',7.8),
  ('La Aventura','Sofia Coppola',95,1,'16:9',2019,'PG','USA','en',2000000,8000000,'https://www.imdb.com/title/tt7654321/',7.2),
  ('Secretos del Pasado','Pedro Almodóvar',110,1,'16:9',2021,'R','España','es',1200000,4500000,'https://www.imdb.com/title/tt2345678/',8.1),
  ('Planeta Azul','David Attenborough',60,1,'16:9',2020,'G','UK','en',5000000,25000000,'https://www.imdb.com/title/tt3456789/',9.3),
  ('Aventuras en el Bosque','Chris Columbus',88,1,'4:3',2018,'PG','USA','en',3000000,15000000,'https://www.imdb.com/title/tt4567890/',7.5),
  ('Cine de Oro: Vol. 1','Luis Buñuel',130,0,'4:3',1955,'PG','España','es',100000,800000,'https://www.imdb.com/title/tt5678901/',8.7),
  ('Voces del Mundo','Bong Joon-ho',105,1,'21:9',2022,'PG-13','Corea del Sur','ko',4000000,16000000,'https://www.imdb.com/title/tt6789012/',8.4),
  ('Comedia Urbana','Judd Apatow',102,1,'16:9',2021,'R','USA','en',25000000,75000000,'https://www.imdb.com/title/tt7890123/',6.9),
  ('Drama Familiar','Greta Gerwig',115,1,'16:9',2022,'PG-13','USA','en',15000000,55000000,'https://www.imdb.com/title/tt8901234/',7.2),
  ('Misterio en la Mansión','Rian Johnson',128,1,'2.35:1',2019,'PG-13','USA','en',40000000,100000000,'https://www.imdb.com/title/tt9012345/',8.0),
  ('Risas y Lágrimas','Alejandro Amenábar',110,1,'16:9',2023,'PG-13','España','es',2000000,7000000,'https://www.imdb.com/title/tt9999999/',7.9),
  ('El Misterio Azul','Alejandro Amenábar',100,1,'16:9',2022,'PG','España','fr',1500000,4000000,'https://www.imdb.com/title/tt8888888/',7.5),
  ('Comedia de la Vida','Alejandro Amenábar',90,1,'16:9',2021,'PG','España','en',1200000,3500000,'https://www.imdb.com/title/tt7777777/',7.1),
  ('Viaje a París','François Ozon',95,1,'16:9',2023,'PG','Francia','fr',1000000,3000000,'https://www.imdb.com/title/tt6666666/',7.0),
  ('Tokyo Dreams','Hirokazu Koreeda',105,1,'16:9',2023,'PG','Japón','ja',1100000,3200000,'https://www.imdb.com/title/tt5555555/',7.3);

-- Actors
INSERT INTO actors(name) VALUES
  ('Penélope Cruz'),('Tom Hanks'),('Antonio Banderas'),('Emma Watson'),('Morgan Freeman'),
  ('Audrey Hepburn'),('Lee Jung-jae'),('Steve Carell'),('Scarlett Johansson'),
  ('Leonardo DiCaprio'),('Cate Blanchett'),('Samuel L. Jackson'),('Natalie Portman'),
  ('Chris Hemsworth'),('Sandra Bullock'),('Robert Downey Jr.'),('Jennifer Lawrence'),
  ('Paz Vega');

-- Movie-Actors
INSERT INTO movie_actors(movie_title,movie_director,actor_name) VALUES
  ('El Gran Viaje','Pedro Almodóvar','Penélope Cruz'),
  ('Secretos del Pasado','Pedro Almodóvar','Antonio Banderas'),
  ('La Aventura','Sofia Coppola','Tom Hanks'),
  ('Planeta Azul','David Attenborough','Morgan Freeman'),
  ('Aventuras en el Bosque','Chris Columbus','Emma Watson'),
  ('Cine de Oro: Vol. 1','Luis Buñuel','Audrey Hepburn'),
  ('Voces del Mundo','Bong Joon-ho','Lee Jung-jae'),
  ('Comedia Urbana','Judd Apatow','Steve Carell'),
  ('Drama Familiar','Greta Gerwig','Scarlett Johansson'),
  ('Misterio en la Mansión','Rian Johnson','Samuel L. Jackson'),
  ('Comedia Urbana','Judd Apatow','Leonardo DiCaprio'),
  ('Drama Familiar','Greta Gerwig','Cate Blanchett'),
  ('Misterio en la Mansión','Rian Johnson','Jennifer Lawrence'),
  ('Voces del Mundo','Bong Joon-ho','Natalie Portman'),
  ('Risas y Lágrimas','Alejandro Amenábar','Paz Vega');

-- Genres
INSERT INTO genres(name) VALUES
  ('Drama'),('Adventure'),('Documentary'),('Kids'),('Classic'),
  ('Mystery'),('Comedy'),('Sci-Fi'),('Horror'),('Romance'),
  ('Musical');

-- Movie-Genres
INSERT INTO movie_genres(movie_title,movie_director,genre_name) VALUES
  ('El Gran Viaje','Pedro Almodóvar','Drama'),
  ('Secretos del Pasado','Pedro Almodóvar','Mystery'),
  ('La Aventura','Sofia Coppola','Adventure'),
  ('Planeta Azul','David Attenborough','Documentary'),
  ('Aventuras en el Bosque','Chris Columbus','Kids'),
  ('Cine de Oro: Vol. 1','Luis Buñuel','Classic'),
  ('Voces del Mundo','Bong Joon-ho','Drama'),
  ('Drama Familiar','Greta Gerwig','Drama'),
  ('Misterio en la Mansión','Rian Johnson','Mystery'),
  ('Comedia Urbana','Judd Apatow','Comedy'),
  ('Misterio en la Mansión','Rian Johnson','Drama'),
  ('Risas y Lágrimas','Alejandro Amenábar','Comedy'),
  ('Risas y Lágrimas','Alejandro Amenábar','Drama'),
  ('El Misterio Azul','Alejandro Amenábar','Mystery'),
  ('Comedia de la Vida','Alejandro Amenábar','Comedy');

-- Keywords
INSERT INTO keywords(word) VALUES
  ('viaje'),('amistad'),('naturaleza'),('bosque'),('clásico'),
  ('memorias'),('internacional'),('ciudad'),('familia'),('misterio'),
  ('risa'),('amor'),('suspense'),('futuro'),('historia');

-- Movie-Keywords
INSERT INTO movie_keywords(movie_title,movie_director,keyword_word) VALUES
  ('El Gran Viaje','Pedro Almodóvar','viaje'),
  ('Secretos del Pasado','Pedro Almodóvar','memorias'),
  ('La Aventura','Sofia Coppola','amistad'),
  ('Planeta Azul','David Attenborough','naturaleza'),
  ('Aventuras en el Bosque','Chris Columbus','bosque'),
  ('Cine de Oro: Vol. 1','Luis Buñuel','clásico'),
  ('Voces del Mundo','Bong Joon-ho','internacional'),
  ('Comedia Urbana','Judd Apatow','risa'),
  ('Drama Familiar','Greta Gerwig','familia'),
  ('Misterio en la Mansión','Rian Johnson','misterio');

-- Inserciones manuales para la tabla views (sin aleatoriedad)
INSERT INTO views(user_nickname, movie_title, movie_director, view_date) VALUES
  ('cinephile89','El Gran Viaje','Pedro Almodóvar','2025-04-03'),
  ('filmBuff','El Gran Viaje','Pedro Almodóvar','2025-04-06'),
  ('cinephile89','La Aventura','Sofia Coppola','2025-04-01'),
  ('filmBuff','La Aventura','Sofia Coppola','2025-04-04'),
  ('cinephile89','Planeta Azul','David Attenborough','2025-04-02'),
  ('filmBuff','Planeta Azul','David Attenborough','2025-04-05'),
  ('popcornLover','Secretos del Pasado','Pedro Almodóvar','2025-04-07'),
  ('classicFan','Aventuras en el Bosque','Chris Columbus','2025-04-08'),
  ('docuHunter','Planeta Azul','David Attenborough','2025-04-09'),
  ('kidsZone','Aventuras en el Bosque','Chris Columbus','2025-04-10'),
  ('ultraViewer','Voces del Mundo','Bong Joon-ho','2025-04-11'),
  ('globexUser','Comedia Urbana','Judd Apatow','2025-04-12'),
  ('cineKing','Drama Familiar','Greta Gerwig','2025-04-13'),
  ('dramaQueen','Misterio en la Mansión','Rian Johnson','2025-04-14'),
  ('comedyHero','Comedia Urbana','Judd Apatow','2025-04-15'),
  ('actionFan','Risas y Lágrimas','Alejandro Amenábar','2025-04-16'),
  ('horrorLord','El Misterio Azul','Alejandro Amenábar','2025-04-17'),
  ('sciFiGeek','Tokyo Dreams','Hirokazu Koreeda','2025-04-18'),
  ('romanticSoul','Viaje a París','François Ozon','2025-04-19'),
  ('thrillerSeeker','Misterio en la Mansión','Rian Johnson','2025-04-20');

-- Inserciones manuales para la tabla ratings (sin aleatoriedad)
INSERT INTO ratings(imdb_id, user_nickname, movie_title, movie_director, rating, comment, rating_date) VALUES
  ('tt1234567','cinephile89','El Gran Viaje','Pedro Almodóvar',8.5,'Excelente película','2025-04-03'),
  ('tt7654321','filmBuff','La Aventura','Sofia Coppola',7.2,'Muy entretenida','2025-04-04'),
  ('tt2345678','popcornLover','Secretos del Pasado','Pedro Almodóvar',9.0,'Impresionante','2025-04-07'),
  ('tt3456789','docuHunter','Planeta Azul','David Attenborough',9.3,'Documental fascinante','2025-04-09'),
  ('tt4567890','kidsZone','Aventuras en el Bosque','Chris Columbus',7.5,'Ideal para niños','2025-04-10'),
  ('tt5678901','classicFan','Cine de Oro: Vol. 1','Luis Buñuel',8.7,'Un clásico','2025-04-08'),
  ('tt6789012','ultraViewer','Voces del Mundo','Bong Joon-ho',8.4,'Muy buena','2025-04-11'),
  ('tt7890123','globexUser','Comedia Urbana','Judd Apatow',6.9,'Divertida','2025-04-12'),
  ('tt8901234','cineKing','Drama Familiar','Greta Gerwig',7.2,'Emotiva','2025-04-13'),
  ('tt9012345','dramaQueen','Misterio en la Mansión','Rian Johnson',8.0,'Gran misterio','2025-04-14'),
  ('tt7890123b','comedyHero','Comedia Urbana','Judd Apatow',7.0,'Me hizo reír','2025-04-15'),
  ('tt9999999','actionFan','Risas y Lágrimas','Alejandro Amenábar',7.9,'Entretenida','2025-04-16'),
  ('tt8888888','horrorLord','El Misterio Azul','Alejandro Amenábar',7.5,'Interesante','2025-04-17'),
  ('tt5555555','sciFiGeek','Tokyo Dreams','Hirokazu Koreeda',7.3,'Muy original','2025-04-18'),
  ('tt6666666','romanticSoul','Viaje a París','François Ozon',7.0,'Bonita historia','2025-04-19'),
  ('tt9012345b','thrillerSeeker','Misterio en la Mansión','Rian Johnson',8.1,'Me mantuvo en suspenso','2025-04-20');
