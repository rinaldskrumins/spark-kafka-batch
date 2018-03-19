package com.hsbc.cia

import java.io.{ByteArrayInputStream, ObjectInputStream}

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.util.control.NonFatal

class KafkaOffsetManager(zkClient: CuratorFramework, path: String) {
  import KafkaOffset._
  val zkPath: String = path
  println(zkPath)
//  val zkPath = "/kafka/test6"

  // NB: retries are handled at the Curator level

  def init(): Unit = {
    try {
      zkClient.create()
        .creatingParentsIfNeeded()
        .forPath(zkPath)
    } catch {
      case _: NodeExistsException => println("Error when initializing ZkClient")
    }
  }

  def commitKafkaOffsets(offsets: PartitionsOffset): Unit = {
    try {
      zkClient.setData().forPath(zkPath, serialize(offsets))
    }
  }

  def getKafkaOffsets(): PartitionsOffset = {
    try {
      val bytes = zkClient.getData().forPath(zkPath)
      deserialize(bytes)
    }

  }


  object KafkaOffset {
    import play.api.libs.json._

    type PartitionsOffset = Map[Int, Long]

    implicit val reads: Reads[PartitionsOffset] =
      JsPath.read[Map[String, Long]].map(offsets => offsets.map(kv => kv._1.toInt -> kv._2))
    implicit val writes: Writes[PartitionsOffset] =
      Writes(offsets => Json.toJson(offsets.map(kv => kv._1.toString -> kv._2)))

    def serialize(offsets: PartitionsOffset): Array[Byte] = {
      println(Json.toJson(offsets).toString)
      println(Json.toJson(offsets).toString.getBytes("UTF-8"))
      Json.toJson(offsets).toString.getBytes("UTF-8")
    }

    def deserialize(bytes: Array[Byte]): PartitionsOffset = {
//      println(bytes)
//      Json.parse(bytes)[PartitionsOffset]
//      ErrorHelper.parseJsonBytes[PartitionsOffset](bytes)
      val jsonString = new String(bytes, "UTF-8")
      val json = Json.parse(jsonString)
      println(json.toString())
      json.as[Map[Int, Long]]
    }
  }


}

