package org.bigdata.mapreduce.task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;

/**
 * Mapper for Task 2: Find month and year with highest total precipitation
 * Emits year-month as key and precipitation hours as value
 */
public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header lines
        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        String[] fields = line.split(",");

        // Weather data should have at least 13 fields
        if (fields.length < 13) {
            return;
        }

        try {
            String date = fields[1].trim();

            // Extract year and month
            int year = WeatherDataParser.getYear(date);
            int month = WeatherDataParser.getMonth(date);

            if (year == -1 || month == -1) {
                return; // Skip invalid dates
            }

            // Extract precipitation_hours (index 9)
            double precipitationHours = WeatherDataParser.parseDouble(fields[9]);

            // Create key: year-month
            String yearMonth = year + "-" + month;
            outputKey.set(yearMonth);
            outputValue.set(precipitationHours);

            context.write(outputKey, outputValue);

        } catch (Exception e) {
            // Skip records with parsing errors
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
