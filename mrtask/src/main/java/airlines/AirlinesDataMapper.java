package airlines;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinesDataMapper extends Mapper<Object, Text, Top5Airlines.AirlineIdentifierKey, Top5Airlines.JoinGenericWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        String airlineId = recordFields[0];
        String airlineName = recordFields[1];

        if (airlineId.equals("IATA_CODE")) {
            return;
        }

        Top5Airlines.AirlineIdentifierKey recordKey = new Top5Airlines.AirlineIdentifierKey(airlineId, Top5Airlines.AirlineIdentifierKey.AIRLINE_RECORD);
        Top5Airlines.Airline record = new Top5Airlines.Airline(airlineName);

        Top5Airlines.JoinGenericWritable genericRecord = new Top5Airlines.JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}
