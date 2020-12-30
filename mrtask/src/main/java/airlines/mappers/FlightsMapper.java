package airlines.mappers;

import airlines.AirlineIdentifierKey;
import airlines.Flight;
import airlines.JoinGenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<Object, Text, AirlineIdentifierKey, JoinGenericWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        if (recordFields[0].equals("YEAR")) {
            return;
        }
        String airlineId = recordFields[4];
        // GLC: It's better to use `int` here
        Integer departureDelay;
        if (recordFields[11].isEmpty()) {
            departureDelay = 0;
        } else {
            departureDelay = Integer.parseInt(recordFields[11]);
        }

        AirlineIdentifierKey recordKey = new AirlineIdentifierKey(airlineId, AirlineIdentifierKey.FLIGHT_RECORD);
        Flight record = new Flight(departureDelay);
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}
