## Run application

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

