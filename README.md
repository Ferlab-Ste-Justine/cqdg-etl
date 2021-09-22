# CQDG-ETL

## dev/debug mode

This mode allows you to start the ETL in standalone mode inside your favorite IDE without using spark-submit.
To start the ETL in dev mode you need to add the following param `-d | --dev` to the command line params.

The ETL will try to access Minio S3 at `http://localhost:9000` with login: `minio` password: `minio123`. 
This configuration can be changed in `SparkConfig` if needed.

Examples of pre-process / process command line params:

**Note: Include dependencies with 'provided' scope**

```shell
# start the pre-process ETL, requires both input and output locations
pre-process --dev -i s3a://cqdg/clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -o s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15
```
```shell
# start the process ETL, requires input + ontology and output locations
# input is most commonly the previous pre-processed output
process --dev -i s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -t s3a://cqdg/ontology-input -o s3a://cqdg/clinical-data-etl-indexer
```

## spark-submit

How to start the ETL with spark-submit using Minio S3 as object storage: 

```shell
sbt clean assembly
```

```shell
export KEYCLOAK_SECRET_KEY=foo
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
```

```shell
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.0 --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --class ca.cqdg.etl.EtlApp target/scala-2.12/cqdg-etl.jar pre-process -i s3a://cqdg/clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -o s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15
```
```shell
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.0 --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --class ca.cqdg.etl.EtlApp target/scala-2.12/cqdg-etl.jar process -i s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -t s3a://cqdg/ontology-input -o s3a://cqdg/clinical-data-etl-indexer
```
