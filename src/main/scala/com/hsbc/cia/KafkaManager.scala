package com.hsbc.cia
import java.io.{BufferedWriter, File, FileWriter}
import java.util.{Collections, Properties}

import kafka.api._
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.Logging
import kafka.api.OffsetRequest
import kafka.common.OffsetAndMetadata
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.io.Source
import scala.util.Try


class KafkaManager(val brokers: String, val groupId: String, val topic: String) extends Logging{

  val simpleConsumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024, "offsetLookup")

//  val offsetCommitRequest = OffsetRequest(groupId, requestInfo, correlationId, clientName, 0.toShort)

  def renameFile(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)

  def readBeginningOffset(path: String): Long = {
    //    val offsetRdd = sc.textFile(path)
    //    println("Offset")
    //    println(offsetRdd.foreach(println))
    //    1L
    renameFile(path, path + ".old")
    val offset = Source.fromFile(path).getLines().toList.last.toLong
    offset
    //    val offsetFile = bufferedSource.getLines().toArray.take(bufferedSource.getLines().length - 1)
    //    for (line <- bufferedSource.getLines) {
    //      println(line.toUpperCase)
    //    }
    //    bufferedSource.close
    //    offsetFile(0).toLong
    //    offsetRdd.
    //    val beginningOffset = offsetRdd.zipWithIndex.filter(_._2==offsetRdd.count()).map(_._1).first()
    //    println(beginningOffset)
    //    beginningOffset.toLong
    //    val count = currentOffset.count()
    //    print(count)
    //    val result = currentOffset.zipWithIndex().collect {
    //      case (v, index) if index != 0 && index != count - 1 => v
    //    }
    //    result
  }

  def updateBeginningOffset(path: String, offset: Long): Unit ={
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.append(offset.toString)
    bw.close()
  }

  private def commitOffset(consumer: SimpleConsumer, offset: Long, clientName: String): Boolean = {
    val partition = 0
    val topicAndPartition = TopicAndPartition(topic, partition)
    val offsetAndMetadata = new OffsetAndMetadata(offset, "", -1L)
    val requestInfo = Map(topicAndPartition -> offsetAndMetadata)
    val req = OffsetCommitRequest(groupId, requestInfo)
    val resp: OffsetCommitResponse = consumer.commitOffsets(req)
    val commitStatus = resp.commitStatus
    println(s"Commit status: $commitStatus")
    println(resp.hasError)

    val testPartition0 = new TopicAndPartition("test", 0)
    val now = System.currentTimeMillis

    var offsets: Map[TopicAndPartition, OffsetAndMetadata] = Map(testPartition0 -> new OffsetAndMetadata(100L, "associated metadata", now))
    val commitRequest: OffsetCommitRequest = new OffsetCommitRequest(groupId, offsets, 0, 0, "offsetLookup")
    val resp2 = consumer.commitOffsets(commitRequest)
    println(resp2.commitStatus)
    println(resp2.commitStatusGroupedByTopic)
    resp2.hasError
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
//    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "org.apache.kafka.clients.consumer.RangeAssignor")
    props
  }

  def run() = {
    val timeout = 10000
    val bufferSize = 64 * 1024
    val consumer = new SimpleConsumer("localhost", 9092, timeout, bufferSize, "offsetLookup")
    val props = createConsumerConfig(brokers, groupId)
    val kafkaConsumer = new KafkaConsumer[String, String](props)
//    kafkaConsumer

    val metadata = getTopicMetaData(topic, "CIA")
    println(metadata)
    val latestOffset2 = getLatestOffset(metadata, "test", 0, "CIA")
    println(s"Latest offset2: $latestOffset2")
    commitOffset(consumer, latestOffset2, "offsetLookup")
    consumer.close()
  }

//    consumer.subscribe(Collections.singletonList(this.topic))
//    consumer.subscribe(this.topic)
//    val recordsFromConsumer = consumer.poll(0)
//
//    val partitions = new Nothing
//
//    val topicPartition = TopicAndPartition(record.topic, record.partition)
//
//    val currentOffset = consumer.position(topicPartition) - 1

  def shutdown(simpleConsumer: SimpleConsumer) = {
    if (simpleConsumer != null)
      simpleConsumer.close()
  }

  def getNumberOfMessages(topic: String, partition: Int) = {

    val clientId = "GetOffset"

    val topicsMetadata = getTopicMetaData(topic, clientId)
    getLatestOffset(topicsMetadata, topic, partition, clientId)
  }

  def getTopicMetaData(topic: String, clientId: String) = {
    val brokerList = "localhost:9092"
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val maxWaitMs = 1000
    val metadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs)
    val topicsMetadata = metadata.topicsMetadata
    if(topicsMetadata.size != 1 || !topicsMetadata.head.topic.equals(topic)) {
      logger.error(s"Error: no valid topic metadata for topic: $topic, probably the topic does not exist, run kafka-list-topic.sh to verify")
      sys.exit(1)
    }

    topicsMetadata
  }

  def getLatestOffset(topicsMetadata: Seq[TopicMetadata], topic: String, partition: Int, clientId: String) = {
    val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partition)
    val time = -1
    val nOffsets = 1

    partitionMetadataOpt match {
      case Some(metadata) =>
        metadata.leader match {
          case Some(leader) =>
            val timeout = 10000
            val bufferSize = 100000
            val consumer = new SimpleConsumer(leader.host, leader.port, timeout, bufferSize, clientId)
            val topicAndPartition = TopicAndPartition(topic, partition)
            val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
            val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
            offsets.last
          case None => logger.error(s"Error: partition $partition does not have a leader. Skip getting offsets"); sys.exit(1)
        }
      case None => logger.error(s"Error: partition $partition does not exist"); sys.exit(1)
    }
  }


}
//
//object KafkaManager extends App{
//  val brokers = "localhost:9092"
//  val groupId = "CIA"
//  val topic = "test"
//  val kafkaManager = new KafkaManager(brokers, groupId, topic)
////  kafkaManager.createConsumerConfig(brokers,groupId)
//  kafkaManager.run()
//}