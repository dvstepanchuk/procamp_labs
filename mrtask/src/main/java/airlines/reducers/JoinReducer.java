package airlines.reducers;

import airlines.Airline;
import airlines.AirlineIdentifierKey;
import airlines.Flight;
import airlines.JoinGenericWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<AirlineIdentifierKey, JoinGenericWritable, Text, DoubleWritable> {
    // GLC| if you set the only reducer for the job
    // GLC| you can implement TopN algorithm right in here and write on Reducer.cleanup
    // GLC| so there is no need for extra MR jobs
    public void reduce(AirlineIdentifierKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{

        String airlineName = "";
        int delaySum = 0;
        int flightCount = 0;
        // GLC: Interesting... You've got almost everything for reduce-side join but have not used it
        // GLC: The idea of RSJ is to have reference data coming first so you need to buffer it in memory (ie in Map)
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

        // GLC: It's better to reuse writables
        context.write(new Text(airlineName), new DoubleWritable(1.0 * delaySum / flightCount));
    }
}
