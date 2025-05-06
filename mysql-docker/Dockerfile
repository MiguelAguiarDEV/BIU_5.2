FROM mysql:8.4

ENV MYSQL_DATABASE=moviebind
ENV MYSQL_USER=user
ENV MYSQL_PASSWORD=1234
ENV MYSQL_ROOT_PASSWORD=1234

EXPOSE 3306

COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/