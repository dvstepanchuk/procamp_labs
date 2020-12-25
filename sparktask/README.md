# Spark DataFrame task

- upload files (airlines, airports, flights) to the home directory
- upload files to hdfs
`hdfs dfs -copyFromLocal flights.csv.gz ./`
`hdfs dfs -copyFromLocal airlines.csv ./`
`hdfs dfs -copyFromLocal airports.csv ./`

Most Popular Airport task
- upload most_popular_airport.scala to the home directory
- start spark shell `spark-shell`
- run `:load most_popular_airport.scala`
- run `DfTask.main()`
