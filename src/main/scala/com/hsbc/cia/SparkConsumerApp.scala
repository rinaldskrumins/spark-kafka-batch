package com.hsbc.cia

import java.util.Properties

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object SparkConsumerApp extends App {
//  val sparkConf = new SparkConf().setAppName("Kafka Message Consumer")
//  sparkConf.setIfMissing("spark.master", "local[5]")
//  //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
////  sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
//
//  val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//  val kafkaTopicRaw = "test3"
//  val kafkaBroker = "127.0.0.1:9092"
//  val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
//  val kafkaParams = Map[String, String]("metadata.broker.list", kafkaBroker)
//
//  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//    ssc, kafkaParams, topics)

  val LOG = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load
//
//  val curator = CuratorFrameworkFactory.newClient("localhost:2181",
//    new ExponentialBackoffRetry(1000, 3))
//  curator.start()

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("KafkaConsumer")
    .set("spark.streaming.kafka.maxRatePerPartition", "100")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
    .set("dfs.support.append", "true")

  val sc = new SparkContext(sparkConf)

  val hadoopConfig = sc.hadoopConfiguration
  hadoopConfig.set("dfs.support.append", "true")

  val hdfs = FileSystem.get(hadoopConfig)

  // hostname:port for Kafka brokers, not Zookeeper
  val topic = "test5"
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "localhost:9092",
    "enable.auto.commit" -> "false",
    "group_id" -> "CIA"
//    "auto.offset.reset" -> "none"
  )
  val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()


//  val kafkaOffsetManager = new KafkaOffsetManager(curator, s"/kafka/$topic")

  println("Offset manager")
//
  //  var offsetRangesTest = Array[OffsetRange]()
  val hadoopPrefix = "hdfs://localhost:9000/data"

  val brokers = "localhost:9092"
  val groupId = "CIA"
  val partition = 0

  val kafkaManager = new KafkaManager(brokers, groupId, topic)
  val hadoopWriter = new HadoopWriter()

  val metadata = kafkaManager.getTopicMetaData(topic, groupId)
  println(metadata)
  val latestOffset = kafkaManager.getLatestOffset(metadata, topic, partition, groupId)
  println(latestOffset)

  val offsetPath = "/tmp/offset.log"

  //  val beginningOffset = offsets.get(0)
  val beginningOffset = kafkaManager.readBeginningOffset(offsetPath)
//  beginningOffset match {
//    case Some(value) => {
      val offsetRanges = Array(
//        OffsetRange(topic, 0, value, 8))
        OffsetRange(topic, partition, beginningOffset, latestOffset))
//        println(value)
        if (beginningOffset < latestOffset){
          val rddStream = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, offsetRanges)
          val offsetRangesKafka = rddStream.asInstanceOf[HasOffsetRanges].offsetRanges
          println(offsetRangesKafka.foreach(x=>println(x)))
          //  val hdfsWriter = new HdfsWriter()
          //  hdfsWriter.runWriter(rddStream)
          //  hadoopWriter.writeToHadoop(rddStream)
          hadoopWriter.runHadoopWriter(sc, hadoopPrefix, rddStream)
          kafkaManager.updateBeginningOffset(offsetPath,latestOffset)
        }
//        kafkaOffsetManager.commitKafkaOffsets(Map(value -> 8).asInstanceOf[Map[Int, Long]])
//      sc.stop()
//    }
//    case None => throw new RuntimeException("blah")
//  }

  sc.stop()
}
