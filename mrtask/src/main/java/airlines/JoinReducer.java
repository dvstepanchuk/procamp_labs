package airlines;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<AirlineIdentifierKey, JoinGenericWritable, Text, DoubleWritable> {
    public void reduce(AirlineIdentifierKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{

        String airlineName = "";
        int delaySum = 0;
        int flightCount = 0;

        for (JoinGenericWritable v : values) {
            Writable record = v.get();
            if (key.recordType.equals(AirlineIdentifierKey.AIRLINE_RECORD)){
                Airline aRecord = (Airline) record;
                airlineName = aRecord.airlineName.toString();
            } else {
                Flight record2 = (Flight) record;
                delaySum += record2.departureDelay.get();
                flightCount++;
            }
        }

        context.write(new Text(airlineName), new DoubleWritable(1.0 * delaySum / flightCount));
    }
}
