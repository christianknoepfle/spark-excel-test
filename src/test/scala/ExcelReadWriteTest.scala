import org.apache.spark.sql.SaveMode
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Paths}
import java.io.File

class ExcelReadWriteTest extends AnyWordSpec with SparkTestApp {

  def deleteDirectory(directoryToBeDeleted: File): Boolean = {
    val allContents = directoryToBeDeleted.listFiles
    if (allContents != null) for (file <- allContents) {
      deleteDirectory(file)
    }
    directoryToBeDeleted.delete
  }

  def withExistingCleanTempDir(name: String): (String => Unit) => Unit = {

    def fixture(testCode: String => Unit) = {

      val directory = new File(s"tmp/${name}")
      if (directory.exists)
        deleteDirectory(directory)

      if (!directory.exists)
        directory.mkdirs()

      try testCode(directory.getPath)
      finally deleteDirectory(directory)
    }

    fixture
  }

  "excel v1" can {
    "read a dataframe from xlsx" in {
      val dfExcel = spark.read
        .format("com.crealytics.spark.excel")
        .option("path", "src/main/resources/xlsx/test_simple.xlsx")
        .option("header", true)
        .load()

      // simple test that we read 2 lines from the xlsx
      assert(dfExcel.count() == 2)
    }

    "write a dataframe to xlsx" in withExistingCleanTempDir("v1") { targetDir =>
      // create a df from csv then write as xlsx
      val dfCsv = spark.read
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .option("path", "src/main/resources/csv/simple.csv")
        .load()

      val targetFile = s"${targetDir}/test_simple.xlsx"
      println(s"Trying to write to $targetFile using V1 API")
      dfCsv.write
        .format("com.crealytics.spark.excel")
        .option("path", targetFile)
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .save()

      assert(Files.exists(Paths.get(targetFile)))
    }
  }

  "excel v2" can {
    "read a dataframe from xlsx" in {
      // read any xlsx from given path
      val dfExcel = spark.read
        .format("excel")
        .option("path", "src/main/resources/xlsx")
        .option("header", true)
        .load()

      // simple test that we read 2 lines from the xlsx
      dfExcel.show()
      assert(dfExcel.count() == 2)
    }

    "write a dataframe to xlsx" in withExistingCleanTempDir("v2") { targetDir =>
      // create a df from csv then write as xlsx
      val dfCsv = spark.read
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .option("path", "src/main/resources/simple.csv")
        .load()

      dfCsv.write
        .format("excel")
        .option("path", targetDir)
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .save()

      val filesInTargetDir = Files.list(Paths.get(targetDir))
      assert(filesInTargetDir.count() == 1)
    }
  }
}
