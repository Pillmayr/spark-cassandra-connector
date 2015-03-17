package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf

/**
 * Helper class to throw exceptions if there are environment variables in the spark.cassandra
 * namespace which don't map to Spark Cassandra Connector known properties.
 */
object ConfigCheck {

  val MatchThreshold = 0.85
  val Prefix = "spark.cassandra."

  val validProps =
    WriteConf.Properties ++
    ReadConf.Properties ++
    CassandraConnectorConf.Properties


  /**
   * Checks the SparkConf Object for any unknown spark.cassandra.* properties and throws an exception
   * with suggestions if an unknown property is found.
   * @param conf SparkConf object to check
   */
  def checkConfig(conf: SparkConf): Unit = {
    val unknownProps = getUnknownProperties(conf)
    if (unknownProps.nonEmpty) {
      val suggestions = for (unknownVar <- unknownProps; s <- getSuggestedVars(unknownVar)) yield (unknownVar, s)
      throw new ConnectorConfigurationException(unknownProps, suggestions.toMap)
    }
  }

  def getUnknownProperties(conf: SparkConf): Seq[String] = {
    val scEnv = for ((key, value) <- conf.getAll if key.startsWith(Prefix)) yield key
    for (key <- scEnv if !validProps.contains(key)) yield key
  }

  /**
   * For a given unknown property determine if we have any guesses as
   * to what the property should be. Suggestions are found by 
   * breaking the unknown property into fragments.
   * spark.cassandra.foo.bar => [foo,bar]
   * Any known property which has some fragments which fuzzy match
   * all of the fragments of the unknown property are considered a
   * suggestion.
   *
   * Fuzziness is determined by MatchThreshold
   */
  def getSuggestedVars(unknownProp: String): Option[Seq[String]] = {
    val unknownFragments = unknownProp.stripPrefix(Prefix).split("\\.")
    val suggestions = validProps.filter { knownProp =>
      val knownFragments = knownProp.stripPrefix(Prefix).split("\\.")
      unknownFragments.forall { unknown =>
        knownFragments.exists { known =>
          val matchScore = StringUtils.getJaroWinklerDistance(unknown, known)
          matchScore >= MatchThreshold
        }
      }
    }
    if (suggestions.nonEmpty) Some(suggestions) else None
  }

  /**
   * Exception to be thrown when unknown properties are found in the SparkConf
   * @param unknownProps Properties that have no mapping to known Spark Cassandra Connector properties
   * @param suggestionMap A map possibly containing suggestions for each of of the unknown properties
   */
  class ConnectorConfigurationException(unknownProps: Seq[String], suggestionMap: Map[String, Seq[String]]) extends ConfigurationException {
    override
    def getMessage: String = {
      val intro = "Invalid Config Variables\nOnly known spark.cassandra.* variables are allowed when using the Spark Cassandra Connector.\n"
      val body = unknownProps.map(unknown => (unknown, suggestionMap.get(unknown))).map {
        case (unknown, None) =>
          s"""$unknown is not a valid Spark Cassandra Connector variable.
                        |No likely matches found.""".stripMargin
        case (unknown, Some(suggestions)) =>
          s"""$unknown is not a valid Spark Cassandra Connector variable.
                        |Possible matches:
                        |${suggestions.mkString("\n")}""".
            stripMargin
      }.mkString("\n")
      intro + body
    }
  }
}
