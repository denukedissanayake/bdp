package org.bigdata.mapreduce.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for Task 1: District-level precipitation and temperature
 * analysis
 * 
 * Usage: hadoop jar mapreduce-task.jar
 * org.bigdata.mapreduce.task1.DistrictWeatherDriver
 * <input_path> <output_path>
 */
public class DistrictWeatherDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DistrictWeatherDriver <input_path> <output_path>");
            System.err.println("  input_path: Comma-separated paths to weather.csv and location.csv");
            System.err.println("  Example: /data/weather.csv,/data/location.csv /output/task1");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "District Weather Analysis");

        // Set JAR class
        job.setJarByClass(DistrictWeatherDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(DistrictWeatherMapper.class);
        job.setReducerClass(DistrictWeatherReducer.class);

        // Set Combiner (same as Reducer for efficiency)
        job.setCombinerClass(DistrictWeatherReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        String[] inputPaths = args[0].split(",");
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath.trim()));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DistrictWeatherDriver(), args);
        System.exit(exitCode);
    }
}
