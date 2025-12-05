package org.bigdata.mapreduce.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper for Task 1: District-level precipitation and temperature analysis
 * Processes both weather and location data to emit district-month aggregations
 */
public class DistrictWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Store location_id to city_name mapping
    private static Map<String, String> locationMap = new HashMap<>();
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim(); // Trim to handle \r\n line endings

        // Skip header lines
        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        String[] fields = line.split(",");

        // Determine if this is location data or weather data
        if (isLocationData(fields)) {
            // Process location data: location_id, latitude, longitude, elevation,
            // utc_offset_seconds, timezone, timezone_abbreviation, city_name
            if (fields.length >= 8) {
                String locationId = fields[0].trim();
                String cityName = fields[7].trim(); // city_name is at index 7
                locationMap.put(locationId, cityName);
            }
        } else {
            // Process weather data
            processWeatherData(fields, context);
        }
    }

    /**
     * Check if the data is location data (has latitude/longitude)
     */
    private boolean isLocationData(String[] fields) {
        // Location data has 8 fields, weather data has 21 fields
        // Also check if second field looks like a latitude (decimal number)
        if (fields.length == 8) {
            try {
                Double.parseDouble(fields[1]); // Try parsing latitude
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Process weather data and emit key-value pairs
     */
    private void processWeatherData(String[] fields, Context context)
            throws IOException, InterruptedException {

        if (fields.length < 13) {
            return; // Skip malformed records
        }

        try {
            String locationId = fields[0].trim();
            String date = fields[1].trim();

            // Get city name from location map
            String cityName = locationMap.get(locationId);
            if (cityName == null || cityName.isEmpty()) {
                return; // Skip if location not found
            }

            // Extract year and month
            int year = WeatherDataParser.getYear(date);
            int month = WeatherDataParser.getMonth(date);

            if (year == -1 || month == -1) {
                return; // Skip invalid dates
            }

            // Extract precipitation_hours (index 9) and temperature_2m_mean (index 5)
            double precipitationHours = WeatherDataParser.parseDouble(fields[9]);
            double temperatureMean = WeatherDataParser.parseDouble(fields[5]);

            // Create composite key: district|year|month
            String compositeKey = cityName + "|" + year + "|" + month;
            outputKey.set(compositeKey);

            // Create value: precipitation_hours,temperature_mean,count
            String compositeValue = precipitationHours + "," + temperatureMean + ",1";
            outputValue.set(compositeValue);

            context.write(outputKey, outputValue);

        } catch (Exception e) {
            // Skip records with parsing errors
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
