package airlines;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Airline implements Writable {
    // GLC: There is no need to use Text -> out.write(); | in.readXXX
    public Text airlineName = new Text();

    public Airline(){}

    public Airline(String airlineName) {
        this.airlineName.set(airlineName);
    }

    public void write(DataOutput out) throws IOException {
        this.airlineName.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.airlineName.readFields(in);
    }
}
