package com.hsbc.cia

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD

class HdfsWriter {

  private def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val destinationPath = new Path(dstPath)
    if (!hdfs.exists(destinationPath)) {
      hdfs.mkdirs(destinationPath)
    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath + "/" + fileName), false, hadoopConfig, null)
  }

  private def getTimestamp: String ={
    val now = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.format(now)
  }

  def runWriter(rddStream: RDD[(String, String)]) = {
    val directoryStructure = createDirectoryStructure("xds_table1")
    val sourceSystem = directoryStructure(0)
    val tableName = directoryStructure(1)
    val fileName = "test_file_spark.avro"
    val timestamp = getTimestamp
//    val sourcePath = s"/data/$sourceSystem/$tableName/tmp"
//    val destPath = s"/data/$sourceSystem/$tableName/test_merge"
//    println(s"hdfs:///data/$sourceSystem/$tableName")
    println(getTimestamp)
//    rddStream.saveAsTextFile(s"hdfs://localhost:9000/data/$sourceSystem/$tableName/$fileName")
//    val sourceFile = hdfsServer + "/tmp/"
//    rddStream.saveAsTextFile(s"hdfs://localhost:9000" + sourcePath)
//    merge(sourcePath, destPath, fileName)
    rddStream.coalesce(1)
    rddStream.saveAsTextFile(s"hdfs://localhost:9000/data/$sourceSystem/$tableName/$fileName/$timestamp")
  }


  def createDirectoryStructure(key: String): Array[String] ={
    var directoryStructure = new Array[String](2)
    directoryStructure = key.split("_")
    directoryStructure
  }

  def runWriter1(rdd: RDD[String]): Unit ={
      val directoryStructure = createDirectoryStructure("xds_table1")
      val sourceSystem = directoryStructure(0)
      val tableName = directoryStructure(1)
      rdd.saveAsTextFile(s"hdfs://$sourceSystem/$tableName")
  }
}
