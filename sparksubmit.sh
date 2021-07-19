echo "START"

spark-submit --class com.moongyu123.moonspark.app.SparkStreamingApp \
--master local[4] \
target/moonspark-1.0-SNAPSHOT-jar-with-dependencies.jar


