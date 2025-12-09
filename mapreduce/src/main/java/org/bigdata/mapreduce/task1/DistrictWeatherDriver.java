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
 */
public class DistrictWeatherDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DistrictWeatherDriver <weather_data_path> <location_data_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Set location data path in configuration so mapper can load it
        conf.set("location.data.path", args[1]);

        Job job = Job.getInstance(conf, "District Weather Analysis");
        job.setJarByClass(DistrictWeatherDriver.class);

        // Mapper and Reducer classes
        job.setMapperClass(DistrictWeatherMapper.class);
        job.setReducerClass(DistrictWeatherReducer.class);

        // Output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths - only process weather data file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DistrictWeatherDriver(), args);
        System.exit(exitCode);
    }
}
