package com.alex.hbase

import com.alex.hbase.stats.repository.HBaseStatsRepository
import com.alex.hbase.stats.util.StatsCSVParser
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Runner {
  def main(args: Array[String]) = {
    val config = ConfigFactory.load()
    val host = config.getString("input.stream.host")
    val port = config.getInt("input.stream.port")

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HBase Test Task")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Bad lines (i.e. not matching timestamp-value pairs) format are simply dropped.
    val statsStream = ssc
      .socketTextStream(host, port)
      .map(StatsCSVParser.parse)
      .filter(_.isDefined)
      .map(_.get)

    val zookeeperQuorum = config.getString("hbase.zookeeper.quorum")
    val table = config.getString("stats.table")
    val columnFamily = config.getString("stats.column.family")

    statsStream.foreachRDD { newStats =>
      if (!newStats.isEmpty()) {
        /*
        From the task one could infer that the received timestamp-value pairs (newStats)
        will likely belong to a range of N minutes where N << number of keys in HBase.
        Thus it makes sense to retrieve only those rows from HBase that could be affected
        by the new data.
         */
        val affectedMinutes = newStats.keys
        val minMinuteAffected = affectedMinutes.min()
        val maxMinuteAffected = affectedMinutes.max()

        implicit val sc = ssc.sparkContext
        val repository = new HBaseStatsRepository(zookeeperQuorum, table, columnFamily)
        val oldStats = repository.readRange(minMinuteAffected, maxMinuteAffected)
        val updatedStats = (newStats ++ oldStats).reduceByKey(_ merge _)
        repository.store(updatedStats)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
