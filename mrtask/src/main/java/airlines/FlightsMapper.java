package airlines;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<Object, Text, Top5Airlines.AirlineIdentifierKey, Top5Airlines.JoinGenericWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        if (recordFields[0].equals("YEAR")) {
            return;
        }
        String airlineId = recordFields[4];
        Integer departureDelay;
        if (recordFields[11].isEmpty()) {
            departureDelay = 0;
        } else {
            departureDelay = Integer.parseInt(recordFields[11]);
        }

        Top5Airlines.AirlineIdentifierKey recordKey = new Top5Airlines.AirlineIdentifierKey(airlineId, Top5Airlines.AirlineIdentifierKey.FLIGHT_RECORD);
        Top5Airlines.Flight record = new Top5Airlines.Flight(departureDelay);
        Top5Airlines.JoinGenericWritable genericRecord = new Top5Airlines.JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}
