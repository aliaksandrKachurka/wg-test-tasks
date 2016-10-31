package com.alex.hbase.stats.util

import com.alex.hbase.stats.to.Stats

object StatsCSVParser {
  private val PATTERN = "(\\d+),(\\d+)".r
  private val MILLIS_IN_MINUTE = 60000

  /**
    * Parses string of format "value,timestamp" into tuple, the first element
    * being a minute inferred from the timestamp and the second being
    * [[com.alex.hbase.stats.to.Stats]] instance corresponding to the value.
    */
  def parse(line: String) = {
    if (isValidStats(line)) {
      val PATTERN(value, timestamp) = line
      val minute = timestamp.toLong / MILLIS_IN_MINUTE
      val stats = new Stats(
        max = value.toLong,
        min = value.toLong,
        avg = value.toDouble,
        count = 1)
      Some(minute, stats)
    }
    else None
  }

  private def isValidStats(line: String) = {
    PATTERN.unapplySeq(line).isDefined
  }
}
