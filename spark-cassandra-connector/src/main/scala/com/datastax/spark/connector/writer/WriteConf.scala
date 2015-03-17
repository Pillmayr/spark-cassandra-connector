package com.datastax.spark.connector.writer

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.SparkConf

/** Write settings for RDD
  *
  * @param batchSize approx. number of bytes to be written in a single batch or
  *                  exact number of rows to be written in a single batch;
  * @param batchBufferSize the number of distinct batches that can be buffered before
  *                        they are written to Cassandra
  * @param batchLevel which rows can be grouped into a single batch
  * @param consistencyLevel consistency level for writes, default LOCAL_ONE
  * @param parallelismLevel number of batches to be written in parallel
  * @param ttl       the default TTL value which is used when it is defined (in seconds)
  * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
  */

case class WriteConf(batchSize: BatchSize = BatchSize.Automatic,
                     batchBufferSize: Int = WriteConf.DefaultBatchBufferSize,
                     batchLevel: BatchLevel = WriteConf.DefaultBatchLevel,
                     consistencyLevel: ConsistencyLevel = WriteConf.DefaultConsistencyLevel,
                     parallelismLevel: Int = WriteConf.DefaultParallelismLevel,
                     throughputMiBPS: Int = WriteConf.DefaultThroughputMiBPS,
                     ttl: TTLOption = TTLOption.defaultValue,
                     timestamp: TimestampOption = TimestampOption.defaultValue) {

  private[writer] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case WriteOption(PerRowWriteOptionValue(placeholder)) => placeholder
  }

  private[writer] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case WriteOption(PerRowWriteOptionValue(placeholder)) =>
        Some(ColumnDef(placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }

  val throttlingEnabled = throughputMiBPS < WriteConf.DefaultThroughputMiBPS
}


object WriteConf {

  val WriteBatchSizeInBytesProperty = "spark.cassandra.output.batch.size.bytes"
  val WriteConsistencyLevelProperty = "spark.cassandra.output.consistency.level"
  val WriteBatchSizeInRowsProperty = "spark.cassandra.output.batch.size.rows"
  val WriteBatchBufferSizeProperty = "spark.cassandra.output.batch.buffer.size"
  val WriteBatchLevelProperty = "spark.cassandra.output.batch.level"
  val WriteParallelismLevelProperty = "spark.cassandra.output.concurrent.writes"
  val WriteThroughputMiBPS = "spark.cassandra.output.throughput_mb_per_sec"

  //Whitelist for allowed Write environment variables
  val Properties = Seq(
    WriteBatchSizeInBytesProperty,
    WriteConsistencyLevelProperty,
    WriteBatchSizeInRowsProperty,
    WriteBatchBufferSizeProperty,
    WriteBatchLevelProperty,
    WriteParallelismLevelProperty,
    WriteThroughputMiBPS
  )

  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultBatchSizeInBytes = 16 * 1024
  val DefaultParallelismLevel = 8
  val DefaultBatchBufferSize = 1000
  val DefaultBatchLevel = BatchLevel.Partition
  val DefaultThroughputMiBPS = Int.MaxValue

  def fromSparkConf(conf: SparkConf): WriteConf = {

    val batchSizeInBytes = conf.getInt(
      WriteBatchSizeInBytesProperty, DefaultBatchSizeInBytes)

    val consistencyLevel = ConsistencyLevel.valueOf(
      conf.get(WriteConsistencyLevelProperty, DefaultConsistencyLevel.name()))

    val batchSizeInRowsStr = conf.get(
      WriteBatchSizeInRowsProperty, "auto")

    val batchSize = {
      val Number = "([0-9]+)".r
      batchSizeInRowsStr match {
        case "auto" => BytesInBatch(batchSizeInBytes)
        case Number(x) => RowsInBatch(x.toInt)
        case other =>
          throw new ConfigurationException(
            s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
      }
    }

    val batchBufferSize = conf.getInt(
      WriteBatchBufferSizeProperty, DefaultBatchBufferSize)

    val batchLevel = conf.getOption(
      WriteBatchLevelProperty).map(BatchLevel.apply).getOrElse(DefaultBatchLevel)

    val parallelismLevel = conf.getInt(
      WriteParallelismLevelProperty, DefaultParallelismLevel)

    val throughputMiBPS = conf.getInt(
      WriteThroughputMiBPS, DefaultThroughputMiBPS)

    WriteConf(
      batchSize = batchSize,
      consistencyLevel = consistencyLevel,
      parallelismLevel = parallelismLevel,
      throughputMiBPS = throughputMiBPS)

  }

}