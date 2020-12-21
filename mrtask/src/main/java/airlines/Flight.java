package airlines;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flight implements Writable {

    public IntWritable departureDelay = new IntWritable();

    public Flight(){}

    public Flight(int departureDelay){
        this.departureDelay.set(departureDelay);
    }

    public void write(DataOutput out) throws IOException {
        this.departureDelay.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.departureDelay.readFields(in);
    }
}
