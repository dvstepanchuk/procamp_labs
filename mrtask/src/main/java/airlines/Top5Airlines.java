package airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Top5Airlines {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "top 5 airlines");
        job.setJarByClass(Top5Airlines.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(AirlineIdentifierKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirlinesDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FlightsMapper.class);

        job.setReducerClass(JoinReducer.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileOutputFormat.setOutputPath(job, new Path(args[2], "out1"));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "top 5 sort by delay");
        job2.setJarByClass(Top5Airlines.class);
        job2.setMapperClass(KeyValueSwappingMapper.class);
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(DoubleWritable.Comparator.class);
        job2.setReducerClass(Top5Reducer.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[2], "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2], "out2"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
