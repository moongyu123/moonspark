package com.moongyu123.moonspark.app

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

class KstreamApp {


  def main(args: Array[String]): Unit = {
    kstream()
  }

  def kstream(): Unit = {

    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p
    }

    val builder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream[String, String]("tp-listings")

    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()
    wordCounts.toStream.to("listings-wordcount-output")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.cleanUp()

    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }

  }
}
