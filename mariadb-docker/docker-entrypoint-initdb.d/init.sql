-- ====================================
-- MovieBind Database (ajuste de planes y nicknames)
-- ====================================

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
-- (idéntico a lo anterior, omito por brevedad)

CREATE TABLE users (
  nickname          VARCHAR(50) PRIMARY KEY,
  password          VARCHAR(255) NOT NULL CHECK (CHAR_LENGTH(password) >= 8),
  email             VARCHAR(100) NOT NULL UNIQUE,
  registration_date DATE        NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE profiles (
  user_nickname  VARCHAR(50) PRIMARY KEY
    REFERENCES users(nickname)
      ON DELETE CASCADE
      ON UPDATE CASCADE,
  dni            VARCHAR(20) NOT NULL UNIQUE,
  first_name     VARCHAR(50),
  last_name      VARCHAR(50),
  age            INT,
  mobile_number  VARCHAR(20),
  birth_date     DATE
);

CREATE TABLE contract_types (
  code        VARCHAR(20) PRIMARY KEY,
  description VARCHAR(100)
);

-- ... demás tablas idénticas ...

-- 3) POBLAR CON DATOS

-- 3.1 contract_types (sólo 5 tipos)
INSERT INTO contract_types(code, description) VALUES
  ('C001','Basic Plan'),
  ('C002','Premium Plan'),
  ('C003','Family Plan'),
  ('C004','Student Plan'),
  ('C005','Pay-per-view');

-- 3.2 users (30 nicknames realistas)
INSERT INTO users(nickname, password, email, registration_date) VALUES
  ('cinephile89','Passw0rd!','cinephile89@example.com','2025-01-10'),
  ('filmBuff','Secr3tPwd','filmbuff@example.com','2025-01-12'),
  ('popcornLover','M1pl4n4pwd','popcornlover@example.com','2025-01-15'),
  ('reelWatcher','C0ntr@seña','reelwatcher@example.com','2025-02-01'),
  ('movieManiac','MiPass123','moviemaniac@example.com','2025-02-10'),
  ('flickFanatic','P4labr3Seg','flickfanatic@example.com','2025-02-20'),
  ('screenQueen','L0gr@Pass','screenqueen@example.com','2025-03-05'),
  ('blockbusterBob','Adm1n1234','blockbusterbob@example.com','2025-03-15'),
  ('indieSeeker','Pelicula!','indieseeker@example.com','2025-03-20'),
  ('trailerJunkie','Contraseña1','trailerjunkie@example.com','2025-03-25'),
  ('filmCritic','Acceso2025','filmcritic@example.com','2025-04-01'),
  ('silverScreen','RedMovie!','silverscreen@example.com','2025-04-05'),
  ('cinemaLover','Bind2025!','cinemalover@example.com','2025-04-10'),
  ('bingeWatcher','Digital99','bingewatcher@example.com','2025-04-12'),
  ('midnightViewer','Cont3nidos','midnightviewer@example.com','2025-04-15'),
  ('tvAddict','MyMovie!1','tvaddict@example.com','2025-04-18'),
  ('streamingPro','L4nch1p0p','streamingpro@example.com','2025-04-20'),
  ('filmGeek','StreamMe2','filmgeek@example.com','2025-04-22'),
  ('movieGuru','CineFan123','movieguru@example.com','2025-04-24'),
  ('sagaHunter','FilmBuff!','sagahunter@example.com','2025-04-26'),
  ('sagaChaser','MovieL0ver','sagachaser@example.com','2025-04-28'),
  ('reelMaster','WatchMore','reelmaster@example.com','2025-04-29'),
  ('cineAddict','SerialCine','cineaddict@example.com','2025-04-29'),
  ('theatreBuff','BindFan24','theatrebuff@example.com','2025-04-29'),
  ('directorDreamer','ViewAll25','directordreamer@example.com','2025-04-29'),
  ('actorWatcher','Cinephile26','actorwatcher@example.com','2025-04-29'),
  ('scriptReader','FilmAddict','scriptreader@example.com','2025-04-29'),
  ('plotTwister','ScreenLove','plottwister@example.com','2025-04-29'),
  ('pictureFan','MovieTime','picturefan@example.com','2025-04-29'),
  ('sceneStealer','PlayReel!','scenestealer@example.com','2025-04-29');

-- 3.3 profiles (mismos datos reales, actualizando sólo el nickname)
INSERT INTO profiles(user_nickname, dni, first_name, last_name, age, mobile_number, birth_date) VALUES
  ('cinephile89','12345678A','Juan','Pérez',35,'612345678','1990-05-15'),
  ('filmBuff','23456789B','María','García',28,'623456789','1997-07-22'),
  ('popcornLover','34567890C','Luis','Fernández',42,'634567890','1983-03-10'),
  ('reelWatcher','45678901D','Ana','López',31,'645678901','1994-11-05'),
  ('movieManiac','56789012E','David','Martínez',38,'656789012','1987-09-30'),
  ('flickFanatic','67890123F','Laura','Sánchez',29,'667890123','1996-01-18'),
  ('screenQueen','78901234G','Carlos','Gómez',45,'678901234','1980-08-12'),
  ('blockbusterBob','89012345H','Marta','Ruiz',34,'689012345','1991-12-03'),
  ('indieSeeker','90123456J','José','Díaz',50,'691234567','1975-04-27'),
  ('trailerJunkie','01234567K','Elena','Hernández',22,'702345678','2003-06-11'),
  ('filmCritic','12345098L','Pablo','Moreno',27,'713456789','1998-10-02'),
  ('silverScreen','23456019M','Andrea','Jiménez',33,'724567890','1992-02-14'),
  ('cinemaLover','34567012N','Miguel','Alonso',47,'735678901','1978-07-19'),
  ('bingeWatcher','45678023P','Cristina','Romero',26,'746789012','1999-09-23'),
  ('midnightViewer','56789034Q','Sergio','Torres',39,'757890123','1986-01-07'),
  ('tvAddict','67890045R','Raquel','Flores',41,'768901234','1984-05-29'),
  ('streamingPro','78901056S','Javier','Rivera',30,'779012345','1995-03-16'),
  ('filmGeek','89012067T','Claudia','Castro',32,'780123456','1993-11-30'),
  ('movieGuru','90123078V','Alberto','Ortiz',55,'791234567','1970-10-25'),
  ('sagaHunter','01234089W','Isabel','Vega',24,'602345678','2001-08-05'),
  ('sagaChaser','12340123X','Fernando','Soto',48,'613456789','1977-12-20'),
  ('reelMaster','23451234Y','Lucía','Aguilar',37,'624567890','1988-02-02'),
  ('cineAddict','34562345Z','Antonio','Molina',46,'635678901','1979-04-09'),
  ('theatreBuff','45673456A','Patricia','Gil',28,'646789012','1997-07-08'),
  ('directorDreamer','56784567B','Daniel','Ramos',23,'657890123','2002-09-15'),
  ('actorWatcher','67895678C','Sara','Marín',44,'668901234','1981-11-18'),
  ('scriptReader','78906789D','Francisco','Suárez',53,'679012345','1972-06-25'),
  ('plotTwister','89017890E','Carmen','Serrano',36,'630123456','1989-03-05'),
  ('pictureFan','90128901F','Andrés','Núñez',29,'641234567','1996-12-17'),
  ('sceneStealer','01230112G','Paula','Domínguez',26,'652345678','1999-05-21');

-- 3.4 contracts (30 contratos, ciclo de 5 planes)
INSERT INTO contracts(user_nickname, dni, contract_type, address, city, postal_code, country, start_date, end_date) VALUES
  ('cinephile89','12345678A','C001','Calle Mayor 1','Madrid','28013','España','2024-01-15','2025-01-15'),
  ('filmBuff','23456789B','C002','Av. Constitución 2','Sevilla','41001','España','2024-01-20','2025-01-20'),
  ('popcornLover','34567890C','C003','C/ Alcalá 45','Madrid','28014','España','2024-02-05','2025-02-05'),
  ('reelWatcher','45678901D','C004','P.º de Gracia 10','Barcelona','08007','España','2024-02-10','2025-02-10'),
  ('movieManiac','56789012E','C005','Ronda Hispanidad 5','Valencia','46023','España','2024-03-01','2025-03-01'),
  ('flickFanatic','67890123F','C001','C/ Nueva 20','Zaragoza','50001','España','2024-03-15','2025-03-15'),
  ('screenQueen','78901234G','C002','Av. Diagonal 220','Barcelona','08018','España','2024-04-01','2025-04-01'),
  ('blockbusterBob','89012345H','C003','C/ Real 15','La Coruña','15002','España','2024-04-15','2025-04-15'),
  ('indieSeeker','90123456J','C004','C/ San Fernando 7','Cádiz','11003','España','2024-05-01','2025-05-01'),
  ('trailerJunkie','01234567K','C005','Av. América 33','Madrid','28002','España','2024-05-15','2025-05-15'),
  ('filmCritic','12345098L','C001','C/ San Juan 12','Alicante','03001','España','2024-06-01','2025-06-01'),
  ('silverScreen','23456019M','C002','C/ Mayor 8','Toledo','45001','España','2024-06-15','2025-06-15'),
  ('cinemaLover','34567012N','C003','Pl. del Pilar 6','Zaragoza','50003','España','2024-07-01','2025-07-01'),
  ('bingeWatcher','45678023P','C004','C/ Príncipe 26','Madrid','28012','España','2024-07-15','2025-07-15'),
  ('midnightViewer','56789034Q','C005','C/ Estafeta 11','Pamplona','31001','España','2024-08-01','2025-08-01'),
  ('tvAddict','67890045R','C001','C/ Larios 3','Málaga','29005','España','2024-08-15','2025-08-15'),
  ('streamingPro','78901056S','C002','C/ Virgen 21','Murcia','30001','España','2024-09-01','2025-09-01'),
  ('filmGeek','89012067T','C003','Av. Lugo 14','Lugo','27001','España','2024-09-15','2025-09-15'),
  ('movieGuru','90123078V','C004','C/ Real 9','Santander','39001','España','2024-10-01','2025-10-01'),
  ('sagaHunter','01234089W','C005','C/ Colón 17','Palma','07001','España','2024-10-15','2025-10-15'),
  ('sagaChaser','12340123X','C001','C/ Registro 4','Logroño','26001','España','2024-11-01','2025-11-01'),
  ('reelMaster','23451234Y','C002','C/ del Pez 30','Madrid','28004','España','2024-11-15','2025-11-15'),
  ('cineAddict','34562345Z','C003','C/ Mayor 3','Segovia','40001','España','2024-12-01','2025-12-01'),
  ('theatreBuff','45673456A','C004','Pl. España 9','Salamanca','37001','España','2024-12-15','2025-12-15'),
  ('directorDreamer','56784567B','C005','C/ Mar 2','Tarragona','43001','España','2025-01-01','2026-01-01'),
  ('actorWatcher','67895678C','C001','C/ Paz 28','Córdoba','14001','España','2025-01-15','2026-01-15'),
  ('scriptReader','78906789D','C002','Av. Paz 1','Sta. Cruz Tfe.','38001','España','2025-02-01','2026-02-01'),
  ('plotTwister','89017890E','C003','C/ Obispo 19','Bilbao','48001','España','2025-02-15','2026-02-15'),
  ('pictureFan','90128901F','C004','C/ Mural 18','León','24001','España','2025-03-01','2026-03-01'),
  ('sceneStealer','01230112G','C005','C/ San Pedro 27','La Laguna','38201','España','2025-03-15','2026-03-15');

-- 3.5 movies, 3.6 actors, 3.7 movie_actors,
-- 3.8 genres, 3.9 movie_genres,
-- 3.10 keywords, 3.11 movie_keywords
-- (idénticos a los datos “reales” que ya tenías)
-- ====================
-- FIN DEL SCRIPT SQL
-- ====================
