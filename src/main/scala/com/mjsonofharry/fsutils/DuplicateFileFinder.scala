package com.mjsonofharry.fsutils

import java.io.File
import java.io.FileInputStream
import java.security.DigestInputStream
import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.SparkSession
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object DuplicateFileFinder {

  case class FileInfo(path: String, size: Long, hash: Option[String])

  case class GroupKey(hash: String, size: Long)

  case class GroupValue(paths: Set[String], count: Long)

  def getFileHash(f: File): Option[String] =
    Try(new FileInputStream(f)) match {
      case Success(input) => {
        val md5 = MessageDigest.getInstance("MD5")
        val digest = new DigestInputStream(input, md5)
        val buffer = new Array[Byte](8192)
        try {
          while (digest.read(buffer) != -1) {}
        } finally {
          digest.close
        }
        input.close
        Some(md5.digest.map("%02x".format(_)).mkString)
      }
      case Failure(exception) => None
    }

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
      .appName("File Search")
      .master("local")
      .config(kryoConf)
      .getOrCreate
    import spark.implicits._

    try {
      val input: Dataset[String] = spark.read
        .textFile(inputFilename)

      val data: Dataset[FileInfo] = input
        .map(path => {
          print(path + "\t")
          val f = new File(path)
          val hash = getFileHash(f)
          println(hash.map(_.toString).getOrElse("No hash"))
          FileInfo(path = path, size = f.length, hash = hash)
        })

      val groups: KeyValueGroupedDataset[GroupKey, GroupValue] = data
        .flatMap(
          part =>
            for {
              hash <- part.hash
            } yield GroupKey(hash, part.size) -> GroupValue(Set(part.path), 1)
        )
        .groupByKey(_._1)
        .mapValues(_._2)

      val aggregated: Dataset[(GroupKey, GroupValue)] = groups
        .reduceGroups(
          (left, right) =>
            GroupValue(
              left.paths ++ right.paths,
              left.count + right.count
            )
        )
        .filter(_._2.count > 1)

      val output: Dataset[String] = aggregated
        .map(group => group._2.paths.mkString(","))

      output.write.text(outputFilename)
    } finally {
      spark.close
    }
  }
}
