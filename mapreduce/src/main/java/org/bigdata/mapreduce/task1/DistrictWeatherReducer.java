package org.bigdata.mapreduce.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;

/**
 * Reducer for Task 1: Aggregates precipitation and temperature by
 * district-month
 */
public class DistrictWeatherReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecipitation = 0.0;
        double totalTemperature = 0.0;
        int count = 0;

        // Aggregate all values for this key
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                totalPrecipitation += Double.parseDouble(parts[0]);
                totalTemperature += Double.parseDouble(parts[1]);
                count += Integer.parseInt(parts[2]);
            }
        }

        // Calculate mean temperature
        double meanTemperature = count > 0 ? totalTemperature / count : 0.0;

        // Parse the key to extract district, year, and month
        String[] keyParts = key.toString().split("\\|");
        if (keyParts.length == 3) {
            String district = keyParts[0];
            String year = keyParts[1];
            int month = Integer.parseInt(keyParts[2]);

            // Format output as specified:
            // "Gampaha had a total precipitation of 30 hours with a mean temperature of 25
            // for 2nd month"
            String monthOrdinal = WeatherDataParser.getOrdinal(month);
            String output = String.format(
                    "%s had a total precipitation of %.0f hours with a mean temperature of %.0f for %s month (Year: %s)",
                    district, totalPrecipitation, meanTemperature, monthOrdinal, year);

            outputValue.set(output);
            context.write(null, outputValue);
        }
    }
}
