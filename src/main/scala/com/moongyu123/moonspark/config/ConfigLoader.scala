package com.moongyu123.moonspark.config

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

class ConfigLoader(resourceBasename: String) {
  import com.typesafe.config.Config

  private var _conf: Config = _
  val SPARK_MASTER = "spark.master"
  val SPARK_APP_NAME = "spark.app-name"

  def conf = _conf

  def conf_=(config: Config): Unit = _conf = config

  def this() = {
    this("config")
    init()
  }

  def init() = {
    _conf = ConfigFactory.load(resourceBasename)
  }

  def sparkConfig = {

    val sparkConf = new SparkConf()
    if (_conf.hasPath(SPARK_MASTER)) {
      sparkConf.setMaster(_conf.getString(SPARK_MASTER))
    }
    if (_conf.hasPath(SPARK_APP_NAME)) {
      sparkConf.setAppName(_conf.getString(SPARK_APP_NAME))
    }
    sparkConf
  }
}
