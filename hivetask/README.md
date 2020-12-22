# Hive Top 5 Airlines task

- Clone the repository
`git clone https://github.com/omklymenko/procamp_labs.git`
- Download flights.csv [source data](https://www.kaggle.com/usdot/flight-delays) and put into /procamp_labs/mrtask/data/
- Run `hdfs dfs -copyFromLocal /home/olena_mklymenko_gmail_com/procamp_labs/mrtask/data/ /procamp_labs/top_5_airlines/input`
- Start Hive 
`> hive`
- Run in Hive
 1. `CREATE TABLE Airlines (iata_code string, airline string) row format delimited fields terminated by ',' tblproperties ("skip.header.line.count"="1");`
 2. `LOAD DATA INPATH '/procamp_labs/top_5_airlines/input/airlines.csv' OVERWRITE INTO TABLE Airlines;`
 3. `CREATE TABLE Flights (YEAR STRING,
     MONTH STRING,
     DAY STRING,
     DAY_OF_WEEK STRING,
     AIRLINE STRING,
     FLIGHT_NUMBER STRING,
     TAIL_NUMBER STRING,
     ORIGIN_AIRPORT STRING,
     DESTINATION_AIRPORT STRING,
     SCHEDULED_DEPARTURE STRING,
     DEPARTURE_TIME STRING,
     DEPARTURE_DELAY INT,
     TAXI_OUT STRING,
     WHEELS_OFF STRING,
     SCHEDULED_TIME STRING,
     ELAPSED_TIME STRING,
     AIR_TIME STRING,
     DISTANCE STRING,
     WHEELS_ON STRING,
     TAXI_IN STRING,
     SCHEDULED_ARRIVAL STRING,
     ARRIVAL_TIME STRING,
     ARRIVAL_DELAY STRING,
     DIVERTED STRING,
     CANCELLED STRING,
     CANCELLATION_REASON STRING,
     AIR_SYSTEM_DELAY STRING,
     SECURITY_DELAY STRING,
     AIRLINE_DELAY STRING,
     LATE_AIRCRAFT_DELAY STRING,
     WEATHER_DELAY STRING) row format delimited fields terminated by ',' 
     tblproperties ("skip.header.line.count"="1");`
 4. `LOAD DATA INPATH '/procamp_labs/top_5_airlines/input/flights.csv.gz' OVERWRITE INTO TABLE Flights;`
 5. `select a.airline, d.average from (
     select airline, round(avg(departure_delay), 2) as average
     from flights f
     group by airline) as d
     join airlines a
     on d.airline = a.iata_code
     order by d.average desc
     limit 5;`
