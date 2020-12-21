package airlines;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueSwappingMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\t");

        context.write(new DoubleWritable(Double.parseDouble(split[1])), new Text(split[0]));
    }
}