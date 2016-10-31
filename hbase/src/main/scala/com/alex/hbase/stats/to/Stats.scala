package com.alex.hbase.stats.to

class Stats(var max: Long, var min: Long, var avg: Double, var count: Long)
  extends Serializable {

  /**
    * Merges the given instance into this.
    * Note that the operation is associative and commutative.
    * @param other the given instance
    * @return this
    */
  def merge(other: Stats) = {
    max = math.max(max, other.max)
    min = math.min(min, other.min)
    avg = calculateAvg(other)
    count = count + other.count
    this
  }

  private def calculateAvg(other: Stats) = {
    val newCount = count + other.count
    (avg * count + other.avg * other.count) / newCount
  }
}