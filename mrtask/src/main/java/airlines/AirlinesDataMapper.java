package airlines;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinesDataMapper extends Mapper<Object, Text, AirlineIdentifierKey, JoinGenericWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        String airlineId = recordFields[0];
        String airlineName = recordFields[1];

        if (airlineId.equals("IATA_CODE")) {
            return;
        }

        AirlineIdentifierKey recordKey = new AirlineIdentifierKey(airlineId, AirlineIdentifierKey.AIRLINE_RECORD);
        Airline record = new Airline(airlineName);

        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}
