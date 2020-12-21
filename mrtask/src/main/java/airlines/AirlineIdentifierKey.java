package airlines;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirlineIdentifierKey implements WritableComparable<AirlineIdentifierKey> {

    public static final IntWritable AIRLINE_RECORD = new IntWritable(0);
    public static final IntWritable FLIGHT_RECORD = new IntWritable(1);

    public Text airlineId = new Text();
    public IntWritable recordType = new IntWritable();

    public AirlineIdentifierKey(){}
    public AirlineIdentifierKey(String airlineId, IntWritable recordType) {
        this.airlineId.set(airlineId);
        this.recordType = recordType;
    }

    @Override
    public int compareTo(AirlineIdentifierKey o) {
        if (this.airlineId.equals(o.airlineId)) {
            return this.recordType.compareTo(o.recordType);
        } else {
            return this.airlineId.compareTo(o.airlineId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.airlineId.write(out);
        this.recordType.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.airlineId.readFields(in);
        this.recordType.readFields(in);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof AirlineIdentifierKey) {
            AirlineIdentifierKey other = (AirlineIdentifierKey) object;
            return this.airlineId.equals(other.airlineId) && this.recordType.equals(other.recordType);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.airlineId.hashCode();
    }
}
