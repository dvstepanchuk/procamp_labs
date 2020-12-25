object DfTask2 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SaveMode

    val flights_df = spark.read.option("sep", ",").option("header", "true").csv("flights.csv.gz")
    val airlines_df = spark.read.option("sep", ",").option("header", "true").csv("airlines.csv")
    val airports_df = spark.read.option("sep", ",").option("header", "true").csv("airports.csv")

    val percentage_df = flights_df.groupBy("AIRLINE", "ORIGIN_AIRPORT").agg(count("*").alias("flights_total_count"), sum("CANCELLED").alias("cancelled_count")).withColumn("percentage_of_cancel", bround(col("cancelled_count")*100/col("flights_total_count"), 2))

    val airline_flights_df = percentage_df.as("df1").join(airlines_df.as("df2"), $"df1.AIRLINE" === $"df2.IATA_CODE").select($"df2.AIRLINE", $"df1.ORIGIN_AIRPORT", $"df1.percentage_of_cancel", $"df1.cancelled_count", $"df1.flights_total_count", ($"df1.flights_total_count"-$"df1.cancelled_count").as("processed_flights"))
    val airline_airport_flights_df = airline_flights_df.join(airports_df, col("ORIGIN_AIRPORT") === col("IATA_CODE"), joinType = "left_outer").select(col("AIRLINE"), when(col("AIRPORT").isNull, col("ORIGIN_AIRPORT")).otherwise(col("AIRPORT")).alias("AIRPORT"), col("percentage_of_cancel"), col("cancelled_count"), col("processed_flights")).sort("AIRLINE", "percentage_of_cancel")

    airline_airport_flights_df.filter(airline_airport_flights_df("AIRPORT") !== "Waco Regional Airport").coalesce(1).write.mode(SaveMode.Overwrite).json("percentage_of_canceled_flights.json")
    airline_airport_flights_df.filter(airline_airport_flights_df("AIRPORT") === "Waco Regional Airport").coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save("wra_canceled_flights")

  }

}