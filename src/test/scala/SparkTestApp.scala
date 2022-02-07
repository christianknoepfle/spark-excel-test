import org.apache.spark.sql.SparkSession

trait SparkTestApp {

  val spark =
    SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Simple Application")
      .getOrCreate()

}
