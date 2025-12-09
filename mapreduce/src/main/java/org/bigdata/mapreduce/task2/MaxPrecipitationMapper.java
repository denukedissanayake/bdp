package org.bigdata.mapreduce.task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;

public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length < 14) {
            return;
        }

        try {
            String date = fields[1].trim();

            int year = WeatherDataParser.getYear(date);
            int month = WeatherDataParser.getMonth(date);

            if (year == -1 || month == -1) {
                return;
            }

            double precipitationHours = WeatherDataParser.parseDouble(fields[13]);
            String yearMonth = year + "-" + month;
            outputKey.set(yearMonth);
            outputValue.set(precipitationHours);

            // key-value pair: year-month(2015-5) -> precipitation hours(12.5)
            context.write(outputKey, outputValue);

        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
