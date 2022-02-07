/* SimpleApp.scala */
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMain {
  def main(args: Array[String]) {
    val spark =
      SparkSession
        .builder()
        .config("spark.master", "local")
        .appName("Simple Application")
        .getOrCreate()

    println("use v1")
    excelV1(spark)
    println("use v2")
    excelV2(spark)
    spark.stop()
  }

  private def excelV1(spark: SparkSession) = {
    // read the provided csv then write it using crealytics v1 api
    // then read it back and print content
    val dfCsv = spark.read
      .format("simple_csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("path", "src/main/resources/simple.csv")
      .load()

    dfCsv.write
      .format("com.crealytics.spark.excel")
      .option("path", "tmp/v1/test_simple.xlsx")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .save()

    val dfExcel = spark.read
      .format("com.crealytics.spark.excel")
      //.option("path", "src/main/resources/test_simple.xlsx")
      .option("path", "tmp/v1/test_simple.xlsx")
      .option("header", true)
      .load()

    dfExcel.show()
  }

  private def excelV2(spark: SparkSession) = {
    // read the provided csv then write it using crealytics v1 api
    // then read it back and print content
    val dfCsv = spark.read
      .format("simple_csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("path", "src/main/resources/simple.csv")
      .load()

    dfCsv.write
      .format("excel")
      .option("path", "tmp/v2/")
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .save()

    val dfExcel = spark.read
      .format("excel")
      .option("path", "tmp/v2/*.xlsx")
      .option("header", true)
      .load()

    dfExcel.show()
  }
}
