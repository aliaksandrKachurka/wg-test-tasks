package com.alex.hdfs.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Ops {
  private val FILE_NAME_PATTERN = ".*/(\\d+)_([\\w])\\.log$".r
  private val LINE_SEPARATOR = "\n"

  /**
    * Checks whether the fileName similar to N_x.log.
    */
  def isLogFile(fileName: String) = {
    FILE_NAME_PATTERN.unapplySeq(fileName).isDefined
  }

  /**
    * Extracts N and x from N_x.log.
    */
  def extractFileNumberAndLetter(fileName: String) = {
    val FILE_NAME_PATTERN(fileNumber, letter) = fileName
    (fileNumber.toInt, letter)
  }

  /**
    * Splits the string into lines and collects them into indexed sequence
    * preserving order.
    */
  def splitOnIndexedLines(stringToSplit: String) = {
    stringToSplit.split(LINE_SEPARATOR).toIndexedSeq
  }

  /**
    * Concatenates columns in 2d sequence.
    *
    * @param sequence the 2d sequence, elements being rows
    * @return sequence of concatenated columns
    */
  def concatenateColumns(sequence: Seq[Seq[String]]) = {
    sequence.transpose.map(_.mkString)
  }

  /**
    * Creates a file for a given file number filled with the given lines
    * and stores it in the given directory.
    */
  def save(directory: String, fileNumber: Int, lines: Seq[String]) = {
    val absolutePath = s"$directory/${fileNumber}_abc.log"
    val fileContent = lines.mkString(LINE_SEPARATOR)
    writeFile(absolutePath, fileContent)
  }

  /*
  Writes the given content to a file under the given path
  using modified UTF-8 encoding in a machine-independent manner.
   */
  private def writeFile(absolutePath: String, content: String) = {
    val path = new Path(absolutePath)
    val fileSystem = FileSystem.get(new Configuration())
    val outputStream = fileSystem.create(path)
    outputStream.writeUTF(content)
    outputStream.close()
  }
}