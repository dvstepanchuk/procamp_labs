object DfTask {

  def main(args: Array[String]): Unit = {

    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path("popular_airport_statistics"), true)

    val flights_df = spark.read.option("sep", ",").option("header", "true").csv("flights.csv.gz")
    val airlines_df = spark.read.option("sep", ",").option("header", "true").csv("airlines.csv")
    val airports_df = spark.read.option("sep", ",").option("header", "true").csv("airports.csv")

    val dest_stat = flights_df.groupBy("MONTH", "DESTINATION_AIRPORT").agg(count("*").alias("dest_m_count")).select(col("MONTH"), col("DESTINATION_AIRPORT"), col("dest_m_count"))
    val max_month_count = dest_stat.groupBy("MONTH").agg(max("dest_m_count").alias("max_in_month")).select(col("MONTH"), col("max_in_month"))
    val max_dest_count = dest_stat.as("df1").join(max_month_count.as("df2"), $"df1.dest_m_count" === $"df2.max_in_month" && $"df1.MONTH" === $"df2.MONTH").select($"df1.MONTH", $"df1.DESTINATION_AIRPORT", $"df2.max_in_month")
    val popular_dest = max_dest_count.join(airports_df, col("IATA_CODE") === col("DESTINATION_AIRPORT"), joinType = "left_outer").select(col("MONTH").cast("int"), when(col("AIRPORT").isNull, col("DESTINATION_AIRPORT")).otherwise(col("AIRPORT")).alias("AIRPORT"), col("max_in_month")).sort("MONTH")

    popular_dest.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").save("popular_airport_statistics")

  }

}