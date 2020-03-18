package com.mjsonofharry.fsutils

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

object FileTreeIndexer {

  sealed trait Tree

  case class Node(children: Seq[File]) extends Tree

  case class Leaf(path: String) extends Tree

  object Tree {
    def apply(f: File): Tree =
      if (f.isDirectory) Node(Option(f.listFiles).map(_.toSeq).getOrElse(Nil))
      else Leaf(f.getAbsolutePath)
  }

  def traverse(acc: Stream[Leaf])(f: File): Stream[Leaf] = Tree(f) match {
    case leaf: Leaf     => acc :+ leaf
    case Node(children) => children.toStream.flatMap(c => traverse(acc)(c))
  }

  def traverse(f: File): Stream[Leaf] = traverse(Stream.empty)(f)

  val DEFAULT_INDEX = "index.txt"

  def main(args: Array[String]): Unit = {
    val (outputFilename: String, roots: Stream[File]) = args.toList match {
      case Nil         => (DEFAULT_INDEX, File.listRoots.toStream)
      case head :: Nil => (head, File.listRoots.toStream)
      case head :: tl  => (head, tl.map(new File(_)).toStream)
    }

    val outputFile = new File(outputFilename)
    if (outputFile.exists) outputFile.delete
    val writer = new BufferedWriter(new FileWriter(outputFile, true))
    roots
      .flatMap(traverse)
      .toIterator
      .foreach(f => {
        println(f.path)
        writer.write(f.path + "\n")
      })
    writer.close
  }
}
