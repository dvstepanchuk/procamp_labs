package airlines;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Top5Reducer extends Reducer<Object, Text, Text, DoubleWritable> {

    private int count = 0;

    public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        if (count < 5) {
            context.write(values.iterator().next(), new DoubleWritable(Double.parseDouble(key.toString())));
        }
        count++;


    }
}
