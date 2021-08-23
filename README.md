# CQDG-ETL

sbt clean assembly

export KEYCLOAK_SECRET_KEY=foo
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123

spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.0 --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --class ca.cqdg.etl.rework.EtlApp target/scala-2.12/cqdg-etl.jar pre-process -i s3a://cqdg/clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -o s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.0 --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --class ca.cqdg.etl.rework.EtlApp target/scala-2.12/cqdg-etl.jar process -i s3a://cqdg/clinical-data-with-ids/e2adb961-4f58-4e13-a24f-6725df802e2c/11-PLA-STUDY/15 -t s3a://cqdg/ontology-input -o s3a://cqdg/clinical-data-etl-indexer

