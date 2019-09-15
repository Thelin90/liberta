# Liberta

![Screenshot](/img/libertaoverview.png)

This application reads data from S3 and creates a simple star schema from it.

Application utilises `PySpark`, `docker-compose`, `metabase`, `postgresql`, `S3 (MinIO)`.

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

![Screenshot](/img/metabaselogin.png)

`user = metabase`
`database = metabase`
`password = password`

### Data Analysis

