#!bin/sh
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

docker run --name postgres-container -e POSTGRES_USER=root -e POSTGRES_PASSWORD=secret -p 5432:5432 -d postgres

