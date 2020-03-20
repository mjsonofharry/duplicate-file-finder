package com.mjsonofharry.fsutils

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.FileInputStream
import java.security.DigestInputStream
import java.security.MessageDigest
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object FileTreeIndexer {

  sealed trait Tree

  case class Node(children: Seq[File]) extends Tree

  case class Leaf(path: String) extends Tree

  object Tree {
    def apply(f: File): Tree =
      if (f.isDirectory) Node(Option(f.listFiles).map(_.toSeq).getOrElse(Nil))
      else Leaf(f.getAbsolutePath)
  }

  val DEFAULT_INDEX = "index.txt"

  def traverse(acc: Stream[Leaf])(f: File): Stream[Leaf] = Tree(f) match {
    case leaf: Leaf     => acc :+ leaf
    case Node(children) => children.toStream.flatMap(c => traverse(acc)(c))
  }

  def traverse(f: File): Stream[Leaf] = traverse(Stream.empty)(f)

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

  def getFileHashUnsafe(f: File): String = {
    val input = new FileInputStream(f)
    val md5 = MessageDigest.getInstance("MD5")
    val digest = new DigestInputStream(input, md5)
    val buffer = new Array[Byte](8192)
    try {
      while (digest.read(buffer) != -1) {}
    } finally {
      digest.close
    }
    input.close
    md5.digest.map("%02x".format(_)).mkString
  }

  def main(args: Array[String]): Unit = {
    val (outputFilename: String, roots: Stream[File]) = args.toList match {
      case Nil         => (DEFAULT_INDEX, File.listRoots.toStream)
      case head :: Nil => (head, File.listRoots.toStream)
      case head :: tl  => (head, tl.map(new File(_)).toStream)
    }

    val outputFile = new File(outputFilename)
    if (outputFile.exists) outputFile.delete
    val writer = new BufferedWriter(new FileWriter(outputFile, true))
    val delim  = "/\\/"
    roots
      .flatMap(traverse)
      .toIterator
      .foreach(file => {
        val f = new File(file.path)
        val hash = getFileHashUnsafe(f)
        println(file.path)
        writer.write(s"${file.path}${delim}${f.length}${delim}${hash}\n")
      })
    writer.close
  }
}
