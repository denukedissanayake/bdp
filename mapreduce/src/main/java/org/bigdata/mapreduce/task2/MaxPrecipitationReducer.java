package org.bigdata.mapreduce.task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;

public class MaxPrecipitationReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {

    private String maxYearMonth = "";
    private double maxPrecipitation = 0.0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecipitation = 0.0;

        // Sum all precipitation hours for this year-month
        for (DoubleWritable value : values) {
            totalPrecipitation += value.get();
        }

        // Track the maximum
        if (totalPrecipitation > maxPrecipitation) {
            maxPrecipitation = totalPrecipitation;
            maxYearMonth = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!maxYearMonth.isEmpty()) {
            String[] parts = maxYearMonth.split("-");
            if (parts.length == 2) {
                String year = parts[0];
                int month = Integer.parseInt(parts[1]);
                String monthOrdinal = WeatherDataParser.getOrdinal(month);

                // Format output as follows:
                // "2nd month in 2019 had the highest total precipitation of 300 hr"
                String output = String.format("%s month in %s had the highest total precipitation of %.0f hr",
                        monthOrdinal, year, maxPrecipitation);

                context.write(NullWritable.get(), new Text(output));
            }
        }
    }
}
