package com.hsbc.cia


import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

trait OffsetsStore {

  def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]]

  def saveOffsets(topic: String, rdd: RDD[_]): Unit

}
