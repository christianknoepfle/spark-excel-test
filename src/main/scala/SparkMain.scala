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

    spark.stop()
  }

}
