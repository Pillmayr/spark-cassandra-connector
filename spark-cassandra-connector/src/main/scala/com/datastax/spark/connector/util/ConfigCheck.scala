package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf

object ConfigCheck {

  val validVars = (WriteConf.envVars ++
    ReadConf.envVars ++
    CassandraConnectorConf.envVars)

  def checkConfig(conf: SparkConf): Unit = {
    val unknownVars = getUnknownVars(conf)
    if (unknownVars.nonEmpty) {
      val suggestions = unknownVars.flatMap(getSuggestedVars(_)).toSet
      throw new ConfigurationException(getErrorString(unknownVars, suggestions.toList))
    }
  }

  def getUnknownVars(conf: SparkConf): Seq[String] = {
    val scEnv = for ((key, value) <- conf.getAll if key.startsWith("spark.cassandra")) yield key
    for (key <- scEnv if !validVars.contains(key)) yield key
  }

  def getSuggestedVars(unknownVar: String): Seq[String] = {
    for (possibleVar <- validVars
         if (StringUtils.getJaroWinklerDistance(
           possibleVar.stripPrefix("spark.cassandra"), unknownVar.stripPrefix("spark.cassandra")) >= 0.85))
    yield possibleVar
  }

  def getErrorString(missingVars: Seq[String], suggestedVars: Seq[String]): String = {
    val errorString =
      s"""${missingVars.mkString("\n")} are not valid Spark Cassandra Connector environment variables.
                                         |The Spark Cassandra Connector only accepts known variables with the prefix spark.cassandra.  """.stripMargin

    val suggestionString = if (suggestedVars.isEmpty) ""
    else
      s"""
         |
         |There are some similar variables you may have meant:
         | ${suggestedVars.mkString("\n")}
       """.stripMargin

    errorString + suggestionString
  }

}
