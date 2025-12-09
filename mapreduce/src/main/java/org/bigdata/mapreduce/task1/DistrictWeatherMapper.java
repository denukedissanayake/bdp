package org.bigdata.mapreduce.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bigdata.mapreduce.util.WeatherDataParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DistrictWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Store location_id to city_name mapping
    private Map<String, String> locationMap = new HashMap<>();
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // Load location data from HDFS
        // Get the location file path from job configuration
        org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
        String locationPath = conf.get("location.data.path", "/user/test/input/locationData.csv");

        // Read location data from HDFS
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(locationPath);
        org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);

        if (fs.exists(path)) {
            try (org.apache.hadoop.fs.FSDataInputStream inputStream = fs.open(path);
                    java.io.BufferedReader reader = new java.io.BufferedReader(
                            new java.io.InputStreamReader(inputStream))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();

                    // Skip header
                    if (WeatherDataParser.isHeader(line)) {
                        continue;
                    }

                    String[] fields = line.split(",");
                    if (fields.length >= 8) {
                        String locationId = fields[0].trim();
                        String cityName = fields[7].trim();
                        locationMap.put(locationId, cityName);
                    }
                }
            }
        }

        System.err.println("DEBUG: Loaded " + locationMap.size() + " locations in setup()");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length >= 14) {
            processWeatherData(fields, context);
        }
    }

    private void processWeatherData(String[] fields, Context context)
            throws IOException, InterruptedException {

        if (fields.length < 14) {
            return;
        }

        try {
            String locationId = fields[0].trim();
            String date = fields[1].trim();

            // Get city name from location map
            String cityName = locationMap.get(locationId);
            if (cityName == null || cityName.isEmpty()) {
                return;
            }

            // Extract year and month
            int year = WeatherDataParser.getYear(date);
            int month = WeatherDataParser.getMonth(date);

            if (year == -1 || month == -1) {
                return;
            }

            // Extract precipitation_hours (index 13) and temperature_2m_mean (index 5)
            double precipitationHours = WeatherDataParser.parseDouble(fields[13]);
            double temperatureMean = WeatherDataParser.parseDouble(fields[5]);

            // Create composite key: district|year|month
            String compositeKey = cityName + "|" + year + "|" + month;
            outputKey.set(compositeKey);

            // Create value: precipitation_hours,temperature_mean,count
            String compositeValue = precipitationHours + "," + temperatureMean + ",1";
            outputValue.set(compositeValue);

            // Return key-value pair
            // key -> district|year|month
            // value -> precipitation_hours,temperature_mean,count
            context.write(outputKey, outputValue);

        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
