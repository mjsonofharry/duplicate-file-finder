package com.mjsonofharry.fsutils

import atto._, Atto._
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object DuplicateFileFinder {

  case class FileInfo(path: String, size: Long, hash: String)

  object FileInfo {
    val delim = string(FileTreeIndexer.DELIM)
    val parser = for {
      path <- manyUntil(anyChar, delim) map (_.mkString)
      size <- long <~ delim
      hash <- manyUntil(anyChar, endOfInput) map (_.mkString)
    } yield FileInfo(path, size, hash)

    def apply(s: String): FileInfo = parser.parseOnly(s).option.get
  }

  case class GroupKey(hash: String, size: Long)

  case class GroupValue(paths: Set[String], count: Long)

  val DEFAULT_OUTPUT = "output"

  def main(args: Array[String]): Unit = {
    val (inputFilename: String, outputFilename: String) = args.toList match {
      case Nil         => (FileTreeIndexer.DEFAULT_INDEX, DEFAULT_OUTPUT)
      case head :: Nil => (head, DEFAULT_OUTPUT)
      case head :: tl  => (head, tl)
    }

    if (!new File(inputFilename).exists)
      throw new RuntimeException(
        s"Specified input file '$inputFilename' does not exist"
      )
    if (new File(outputFilename).exists)
      throw new RuntimeException(
        s"Specified output file '$outputFilename' already exists"
      )

    val kryoConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .registerKryoClasses(
        Array(classOf[FileInfo], classOf[GroupKey], classOf[GroupValue])
      )

    val spark = SparkSession.builder
      .appName("Duplicate File Finder")
      .master("local")
      .config(kryoConf)
      .getOrCreate
    import spark.implicits._

    try {
      val input: Dataset[FileInfo] =
        spark.read
          .textFile(inputFilename)
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
          .map(FileInfo(_))

      val groups: KeyValueGroupedDataset[GroupKey, GroupValue] = input
        .map(f => GroupKey(f.hash, f.size) -> GroupValue(Set(f.path), 1))
        .groupByKey(_._1)
        .mapValues(_._2)

      val aggregated: Dataset[(GroupKey, GroupValue)] = groups
        .reduceGroups(
          (a, b) => GroupValue(a.paths ++ b.paths, a.count + b.count)
        )
        .filter(_._2.count > 1)

      val output: Dataset[String] = aggregated
        .map(group => group._2.paths.mkString(FileTreeIndexer.DELIM))

      output.coalesce(1).write.text(outputFilename)
    } finally {
      spark.close
    }
  }
}
