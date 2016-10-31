package com.alex.hdfs

import com.alex.hdfs.util.Ops
import org.apache.spark.{SparkConf, SparkContext}

object Runner {
  def main(args: Array[String]) = {
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("HDFS Test Task")
    val sc = new SparkContext(conf)

    val files = sc.wholeTextFiles(s"$inputPath") filter {
      case (fileName, content) =>
        Ops.isLogFile(fileName)
    }

    /*
    In file name N_x.log N is assumed as a file number and x - as a letter.
     */
    val numberedLettersWithLines = files map {
      case (fileName, content) =>
        val (fileNumber, letter) = Ops.extractFileNumberAndLetter(fileName)
        val indexedLines = Ops.splitOnIndexedLines(content)
        (fileNumber, (letter, indexedLines))
    }

    val groupedByFileNumber = numberedLettersWithLines groupByKey()

    val linesSortedByLetterWithinGroup = groupedByFileNumber mapValues {
      lettersWithLines => lettersWithLines.toSeq sortBy {
        case (letter, lines) => letter
      } map {
        case (letter, lines) => lines
      }
    }

    val newFiles = linesSortedByLetterWithinGroup map {
      case (fileNumber, oldFilesLines) =>
        val newFileLines = Ops.concatenateColumns(oldFilesLines)
        (fileNumber, newFileLines)
    }

    newFiles.foreach {
      case (fileNumber, newFileLines) =>
        Ops.save(outputPath, fileNumber, newFileLines)
    }
    sc.stop()
  }
}