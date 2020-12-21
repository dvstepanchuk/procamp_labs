package airlines.comparators;

import airlines.AirlineIdentifierKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinGroupingComparator extends WritableComparator {
    public JoinGroupingComparator() {
        super (AirlineIdentifierKey.class, true);
    }

    @Override
    public int compare (WritableComparable a, WritableComparable b){
        AirlineIdentifierKey first = (AirlineIdentifierKey) a;
        AirlineIdentifierKey second = (AirlineIdentifierKey) b;

        return first.airlineId.compareTo(second.airlineId);
    }
}
