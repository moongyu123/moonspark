package com.moongyu123.moonspark.app

import com.moongyu123.moonspark.dataset.Listings
import org.apache.spark.sql.{Encoders, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime
import org.slf4j.LoggerFactory

object SparkStreamingApp {

  val logger = LoggerFactory.getLogger(getClass)
  final val CUSTOM_USER_PATH = "/Users/lion00"                      //for develop 사용자지정경로
  final val WORK_SPACE = CUSTOM_USER_PATH + "/workspace/moonspark"  //for develop spark소스경로
  final val cpDir = CUSTOM_USER_PATH + "/serialization"             //spark offset

  def main(args: Array[String]): Unit = {
//    structStreamFromFileToFile()  //input csv file to other file
    structStreamFromFileToParse()
  }

  def structStreamFromFileToFile(): Unit = {

    val spark = SparkSession
      .builder
      .appName(getClass.getName)
      .getOrCreate()

    val listingsSchema = org.apache.spark.sql.Encoders.product[Listings].schema

    val csvDF = spark
      .readStream
      .option("mode", "PERMISSIVE")  //PERMISSIVE:schema에 맞지않으면 null ,DROPMALFORMED : 오류시 row버림 , FAILFAST: 오류시 Exception
//      .option("columnNameOfCorruptRecord", "error_string")
      .option("maxFilesPerTrigger",10)  //trigger당 처리할 최대 파일개수
      .option("sep", ",")
      .schema(listingsSchema)      // Specify schema of the csv files
      .csv(s"$WORK_SPACE/src/main/resources/in_data/listings")    // Equivalent to format("csv").load("/path/to/directory")

    logger.debug(s"isStreaming ? ${csvDF.isStreaming}")

    //stream데이터의 hdfs write
    csvDF.writeStream
        .format("json")        // can be "orc", "json", "csv", for debug "console"
        .option("path", s"$WORK_SPACE/src/main/resources/out_data")
        .option("checkpointLocation", cpDir)
        .outputMode("append") //append, complete
        .start().awaitTermination()

    /*logger.debug(s"isStreaming ? ${csvDF.isStreaming}")
    csvDF.createOrReplaceTempView("TempCsvDF")
    spark.sql("select count(*) from TempCsvDF")
    spark.sqlContext.clearCache()*/
  }

  def structStreamFromFileToParse(): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getName)
      .getOrCreate()

    val listingsSchema = org.apache.spark.sql.Encoders.product[Listings].schema

    val csvDF = spark
      .readStream
      .option("mode", "PERMISSIVE")
      .option("sep", ",")
      .schema(listingsSchema)      // Specify schema of the csv files
      .csv(s"$WORK_SPACE/src/main/resources/in_data/listings")    // Equivalent to format("csv").load("/path/to/directory")

    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }

      override def process(value: Row): Unit = {
        println("-----------------")
//        println(value)

      }

      override def close(errorOrNull: Throwable): Unit = {
        logger.error(s"$errorOrNull")

      }
    }

    val writeStream = csvDF.writeStream
        .trigger(ProcessingTime("5 seconds"))
        .outputMode("append")
        .foreach(writer)
        .start()

    writeStream.awaitTermination()


    logger.debug(s"isStreaming ? ${csvDF.isStreaming}")

  }
}
