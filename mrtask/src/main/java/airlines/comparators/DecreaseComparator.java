package airlines.comparators;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

public class DecreaseComparator extends WritableComparator {
    public DecreaseComparator() {
        super(DoubleWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        double thisValue = readDouble(b1, s1);
        double thatValue = readDouble(b2, s2);
        return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
    }
}