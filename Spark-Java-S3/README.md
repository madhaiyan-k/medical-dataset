# Spark-Java-S3

To build the project from the command-line:

```
bin/build.sh

OR

$: mvn clean install

OR

$: mvn install

```
Create container in the AWS with name "maplelabs".

Create directory with name "maple".

Upload the OP_DTL_GNRL_PAY.csv and OP_DTL_RSRCH_PAY.csv datasets.

export SPARK_MAJOR_VERSION=2

Run

```
spark-submit --class org.maplelabs.main.SparkReadS3Dataset spark-java-0.0.1-SNAPSHOT.jar awsAccessKeyId awsSecretAccessKey

OR

spark-submit --verbose --class org.maplelabs.main.SparkReadS3Dataset spark-java-0.0.1-SNAPSHOT.jar awsAccessKeyId awsSecretAccessKey
```
