# Data-engineer-project
creating end to end pipeline starting from ingest to consumption 

docker build -t test:pandas .
docker run -it test:pands


C:/work/Data-engineer_project/1-dockerfile/postgres_data

docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v C:/work/Data-engineer_project/1-dockerfile/postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:14

