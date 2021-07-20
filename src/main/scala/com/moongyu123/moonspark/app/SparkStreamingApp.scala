package com.moongyu123.moonspark.app

import java.util.Properties

import com.moongyu123.moonspark.dataset.{Listings, Reviews}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSONObject

object SparkStreamingApp {

  val logger = LoggerFactory.getLogger(getClass)
  final val CUSTOM_USER_PATH = "/Users/lion00"                      //for develop 사용자지정경로
  final val WORK_SPACE = CUSTOM_USER_PATH + "/workspace/moonspark"  //for develop spark소스경로
  final val cpDir = CUSTOM_USER_PATH + "/serialization"             //spark offset

  def main(args: Array[String]): Unit = {

//    TEST case1. FS to FS
//    structStreamFromFileToFile()  //input file to other file

//    TEST case2. FS to Kafka
    var target = ""
    if(args.length < 1){
      target = "listings"
    }else{
      target = args(0)
    }

    structStreamFromFileToKafka(target)   //input file to kafka
  }


  def schemaSelection(target:String):StructType={
    val schema = target match {
      case "listings" => org.apache.spark.sql.Encoders.product[Listings].schema
      case "reviews" => org.apache.spark.sql.Encoders.product[Reviews].schema
        // add more datatype
      case _ => org.apache.spark.sql.Encoders.product[Listings].schema
    }
    schema
  }
  /**
    * FileSystem(hdfs,s3..) TO FileSystem
    */
  def structStreamFromFileToFile(target:String): Unit = {

    val spark = SparkSession
      .builder
      .appName(getClass.getName)
      .getOrCreate()

    val csvDF = spark
      .readStream
      .option("mode", "PERMISSIVE")  //PERMISSIVE:schema에 맞지않으면 null ,DROPMALFORMED : 오류시 row버림 , FAILFAST: 오류시 Exception
      .option("maxFilesPerTrigger",10)  //trigger당 처리할 최대 파일개수
      .option("sep", ",")
      .schema(schemaSelection(target))      // Specify schema of the csv files
      .csv(s"$WORK_SPACE/src/main/resources/in_data/$target")    // Equivalent to format("csv").load("/path/to/directory")

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

  /**
    * FileSystem(hdfs,s3..) TO Kafka(producer)
    */
  def structStreamFromFileToKafka(target:String): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getName)
      .getOrCreate()

    val csvDF = spark
      .readStream
      .option("header",true)
      .option("mode", "PERMISSIVE")
      .option("sep", ",")
      .schema(schemaSelection(target))      // Specify schema of the csv files
      .csv(s"$WORK_SPACE/src/main/resources/in_data/$target")    // Equivalent to format("csv").load("/path/to/directory")

    csvDF.printSchema()

    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }
      override def process(value: Row): Unit = {
        val json = convertRowToJSON(value)
        if(!"".equals(json)) processRowToKafka(json, target)
      }
      override def close(errorOrNull: Throwable): Unit = {
        logger.error(s"$errorOrNull")
      }
    }

    val writeStream = csvDF
      .writeStream
      .trigger(ProcessingTime("5 seconds"))
      .outputMode("append")
      .foreach(writer)
      .start()

    writeStream.awaitTermination()

  }

  def convertRowToJSON(row: Row): String = {

    val m = Option(row.getValuesMap(row.schema.fieldNames))
    if(m.isDefined) {
      try{
        JSONObject(m.get).toString()
      }catch {
        case _:Exception => ""
      }
    }else ""
  }

  /**
    * kafka producer
    */
  def processRowToKafka(json: String, target:String): Unit ={
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")  //kafka server
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)

    val topic = target match {
      case "listings" => "tp-listings"
      case "reviews" => "tp-reviews"
      case _ => "tp-error"
    }

    logger.debug("topic ==> " + topic)
    logger.debug("json ==> \n" + json)

    val record = new ProducerRecord[String, String](topic, json)
    producer.send(record)


  }


}
