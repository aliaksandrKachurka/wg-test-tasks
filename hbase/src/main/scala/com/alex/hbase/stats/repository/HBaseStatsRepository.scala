package com.alex.hbase.stats.repository

import com.alex.hbase.stats.to.Stats
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Mutation, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class HBaseStatsRepository(val zookeeperQuorum: String,
                           val table: String,
                           val columnFamily: String) extends Serializable {

  private val COLUMN_FAMILY = Bytes.toBytes(columnFamily)
  private val MAX = Bytes.toBytes("max")
  private val MIN = Bytes.toBytes("min")
  private val AVG = Bytes.toBytes("avg")
  private val COUNT = Bytes.toBytes("count")

  /**
    * Reads range of [[com.alex.hbase.stats.to.Stats]] for the given time interval.
    * @param firstMinute left time interval bound (inclusive)
    * @param lastMinute right time interval bound (inclusive)
    * @param sc spark context
    * @return a pairs of minute with
    *         the corresponding [[com.alex.hbase.stats.to.Stats]]
    */
  def readRange(firstMinute: Long, lastMinute: Long)(implicit sc: SparkContext) = {
    val config = createHBaseConfigForRangeScan(firstMinute, lastMinute)
    sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map {
        case (key, row) =>
          val minute = Bytes.toLong(key.get())
          val stats = convertFromResult(row)
          (minute, stats)
      }
  }

  private def createHBaseConfigForRangeScan(firstMinute: Long, lastMinute: Long) = {
    val firstMinuteRepresentation = Bytes.toString(Bytes.toBytes(firstMinute))
    val lastMinuteRepresentation = Bytes.toString(Bytes.toBytes(lastMinute + 1))

    val config = createHBaseConfig
    config.set(TableInputFormat.SCAN_ROW_START, firstMinuteRepresentation)
    config.set(TableInputFormat.SCAN_ROW_STOP, lastMinuteRepresentation)
    config
  }

  private def createHBaseConfig = {
    val config = HBaseConfiguration.create()
    config.clear()
    config.set(TableInputFormat.INPUT_TABLE, table)
    config.set(TableOutputFormat.OUTPUT_TABLE, table)
    config.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamily)
    config.set("hbase.zookeeper.quorum", zookeeperQuorum)
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.setClass("mapreduce.job.outputformat.class",
      classOf[TableOutputFormat[String]],
      classOf[OutputFormat[String, Mutation]])
    config
  }

  private def convertFromResult(row: Result): Stats = {
    new Stats(
      max = extract(row, MAX).toLong,
      min = extract(row, MIN).toLong,
      avg = extract(row, AVG).toDouble,
      count = extract(row, COUNT).toLong)
  }

  private def extract(row: Result, columnToExtract: Array[Byte]) = {
    Bytes.toString(row.getValue(COLUMN_FAMILY, columnToExtract))
  }

  /**
    * Stores the given RDD of [[com.alex.hbase.stats.to.Stats]].
    * @param statsToStore pairs of minute with
    *                     the corresponding [[com.alex.hbase.stats.to.Stats]]
    */
  def store(statsToStore: RDD[(Long, Stats)]) = {
    val config = createHBaseConfig
    statsToStore.map {
      case (key, stats) =>
        (new ImmutableBytesWritable, convertIntoPut(key, stats))
      }.saveAsNewAPIHadoopDataset(config)
  }

  private def convertIntoPut(key: Long, stats: Stats) = {
    val put = new Put(Bytes.toBytes(key))
    addColumnToPut(put, MAX, stats.max)
    addColumnToPut(put, MIN, stats.min)
    addColumnToPut(put, AVG, stats.avg)
    addColumnToPut(put, COUNT, stats.count)
    put
  }

  private def addColumnToPut(put: Put, columnName: Array[Byte], value: Any) = {
    put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(value.toString))
  }
}