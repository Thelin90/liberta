# Liberta

![Screenshot](/img/libertaoverview.png)

This application reads data from S3 and creates a simple star schema from it.

Application utilises `PySpark`, `docker-compose`, `metabase`, `postgresql`, `S3 (MinIO)`.

## Data set

The dataset `rawevents` is automatically stored within a bucket, and can be accessed once docker has built it up.

### Star schema

Star schema after looking at the data

![Screenshot](/img/starschema.png)

## Run application

Follow steps below.

Run manually by first initialise `virtualenv`  

```bash
virtualenv venv -p python3.7
source venv/bin/activate
```

Then install dependency packages:

Go to:
```bash
cd scripts
```
Run:
```bash
./run.sh
```

Explicitly for MacOS users

```bash
brew install lzop
```

`subprocess` will otherwise fail when running application locally (decompression when reading S3 files).

#### Local run

`cd tools/docker`

```bash
docker-compose up
```

A [minio](http://127.0.0.1:9000/minio/rawdata/) server with a default bucket and data has now been created, which can be used.
A [metabase](http://localhost:3000) server will be setup

## Run ETL

Go to the root of the project, then type:

```bash
make tablestopg
```

The code will run, please go to the `spark-ui` for more info, in the console it will log which ports it will be using.

There the process can be followed:

![Screenshot](/img/spark-ui-progress.png)

## Setup Metabase

Surf into [metabase](http://localhost:3000)

1) For the first part just enter som fake information to register
2) Second part, connect to the postgres database

`1`
![Screenshot](/img/signup.png)

`2`
![Screenshot](/img/enterstuff.png)
See in the `env` file.
`host = docker ip` 
`user = metabase`
`database = metabase`
`password = password`

`2`
![Screenshot](/img/getinmetabase.png)

Note that to get the correct host when connecting to the database, run:

```bash
docker ps -a
```

Then look at the container id of `postgres`, then run:

```bash
docker inspect <container-id>
```

`"IPAddress": "172.28.0.2"`

Look up: `IPAdress`

Change `localhost` to the IP, and connect to database.

## Tests

TODO:

:(

### Data Analysis

#### DAU

What is an active user is?

- An active user by the first time the application got installed
