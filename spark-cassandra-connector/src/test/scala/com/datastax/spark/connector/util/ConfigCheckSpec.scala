package com.datastax.spark.connector.util

import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class ConfigCheckSpec extends FlatSpec with Matchers  {

  "ConfigCheck" should " throw an exception when the configuration contains a invalid spark.cassandra prop" in {
    val sparkConf = new SparkConf().set("spark.cassandra.foo.bar", "foobar")
    val exception = the [ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage() should include ("spark.cassandra.foo.bar")
  }

  it should " suggest alternatives if you have a slight misspelling " in {
    val sparkConf = new SparkConf()
      .set("spark.cassandra.output.batch.siz.bytez", "40")
      .set("spark.cassandra.output.batch.size.row","10")
      .set("spark.cassandra.connect.host", "123.231.123.231")

    val exception = the[ConfigurationException] thrownBy ConfigCheck.checkConfig(sparkConf)
    exception.getMessage() should include("spark.cassandra.output.batch.size.bytes")
    exception.getMessage() should include("spark.cassandra.output.batch.size.rows")
    exception.getMessage() should include("spark.cassandra.connection.host")
  }

}
