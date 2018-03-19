package com.hsbc.cia

import java.io.{BufferedWriter, File, FileWriter}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.io.Source

class HadoopWriter {
  ////  def writeToHadoop(rdd: RDD[(String, String)], hadoopConfig: Configuration) = Unit {
//  def writeToHadoop(rdd: RDD[(String, String)]) = {
//    // current issues --> filename for each table stored in HDFS
////    rdd.foreach(println())
//    val fileMap: Map[String, Iterable[String]] = generateFileMapForEachSource(rdd)
//    var fileInsertionMap: Map[String, Boolean] = Map()
//
//    fileMap.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
//
//    // Perform insertions of records, one by one.
//    // If success, add true to map
//    // If failure, add false to map
//
//    fileMap.foreach(file => {
//
//    })
//    println("All done")
//
//  }

  private def getTimestamp: String = {
    val now = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.format(now)
  }

  def writeToSequenceFile(rdd: RDD[Array[Byte]]): Unit ={
    rdd.map(bytesArray => (NullWritable.get(), new BytesWritable(bytesArray)))
      .saveAsSequenceFile("/output/path")
  }

  def readFromSequenceFile(sc: SparkContext): RDD[Array[Byte]] ={
    val rdd: RDD[Array[Byte]] = sc.sequenceFile[NullWritable, BytesWritable]("/input/path")
      .map(_._2.copyBytes())
    rdd
  }

  def runHadoopWriter(sc: SparkContext, hadoopPath: String, rdd: RDD[(String, String)]): Unit ={
    // 1. get RDD from Kafka
    // 2. Create a map of RDD for each source table
    // 3. For each RDD, generate file as .avro --> still an issue
    // 4. Store in HDFS and done
    val hadoopConfig = sc.hadoopConfiguration
    val fs = FileSystem.get(URI.create(hadoopPath), hadoopConfig)
    val fileMapEntry: Map[String, Iterable[String]] = generateFileMapForEachSource(rdd) // Step 2
    println(fileMapEntry.size)
    if (fileMapEntry.size > 1) {
      fileMapEntry.foreach(fileMap => {
        val sourceTable: Array[String] = createDirectoryStructure(fileMap._1)
        val sourceSystemName = sourceTable(0)
        val tableName = sourceTable(1)
        val fullPath = s"$hadoopPath/$sourceSystemName/$tableName/$getTimestamp"
        println("fullPath: " + fullPath)
        val cleanedFullPath = fullPath.replaceAll("\"", "").replaceAll("\'", "")
        println("cleanedFullPath: " + cleanedFullPath)
        val destinationPath = new Path(cleanedFullPath)
        println("destinationPath: " + destinationPath)
        sc.makeRDD(fileMap._2.toList).coalesce(1).saveAsTextFile(destinationPath.toString) // Step 4
        // Save to hadoop in format of 'hdfs://' + $ROOT_DIR + $SOURCE + $TABLE + $TIMESTAMP'
      })
    }
  }

  private def createDirectoryStructure(key: String): Array[String] ={
    var directoryStructure = new Array[String](2)
    directoryStructure = key.replaceAll("\"", "").split('_')
    directoryStructure
  }

  private def generateFileMapForEachSource(rdd: RDD[(String, String)]): Map[String, Iterable[String]] = {
    var messagesMap: Map[String, ListBuffer[String]] = Map() // map for all messages with respective source_table
    var messagesListForSourceTables: ListBuffer[String] = new ListBuffer() // list of messages for each source_table
    rdd.foreach(x => {
      // iterate through all messages and construct map of messages for each.
      // Assumptions --> each message is .avro record where value is .avro entry and key is $source_table
      //        val sourceSystem = getSubFolders(x._1)
      //        val sourceTable = getSubFolders(x._2)
      if(!messagesMap.contains(x._1)){
        val avroMessage = messagesListForSourceTables += x._2
        messagesMap += (x._1 -> avroMessage)
      } else {
        messagesMap.get(x._1).map(_ += x._2) // append to existing list
      }
    })

    val mapOfMessages= rdd.groupBy(_._1).mapValues(_.map(_._2)).collect().toMap
    mapOfMessages
  }

}
