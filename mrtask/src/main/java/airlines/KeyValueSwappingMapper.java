package airlines;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueSwappingMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\t");

        double delay = Double.parseDouble(split[1]);
        delay = Math.round(delay * 100d) / 100d;

        context.write(new DoubleWritable(delay), new Text(split[0]));
    }
}