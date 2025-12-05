package org.bigdata.mapreduce.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for Task 2: Find month and year with highest total precipitation
 * 
 * Usage: hadoop jar mapreduce-task.jar
 * org.bigdata.mapreduce.task2.MaxPrecipitationDriver
 * <input_path> <output_path>
 */
public class MaxPrecipitationDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxPrecipitationDriver <input_path> <output_path>");
            System.err.println("  input_path: Path to weather.csv");
            System.err.println("  Example: /data/weather.csv /output/task2");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Maximum Precipitation Month Analysis");

        // Set JAR class
        job.setJarByClass(MaxPrecipitationDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MaxPrecipitationMapper.class);
        job.setReducerClass(MaxPrecipitationReducer.class);

        // Set output key and value types for mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set output key and value types for reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use single reducer to find global maximum
        job.setNumReduceTasks(1);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MaxPrecipitationDriver(), args);
        System.exit(exitCode);
    }
}
