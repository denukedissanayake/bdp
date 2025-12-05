package org.bigdata.mapreduce.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for parsing weather and location CSV data
 */
public class WeatherDataParser {

    private static final SimpleDateFormat DATE_FORMAT_1 = new SimpleDateFormat("M/d/yyyy");
    private static final SimpleDateFormat DATE_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATE_FORMAT_3 = new SimpleDateFormat("dd/MM/yyyy");

    /**
     * Parse a CSV line and return a map of column name to value
     */
    public static Map<String, String> parseCsvLine(String line, String[] headers) {
        Map<String, String> record = new HashMap<>();
        String[] values = line.split(",");

        for (int i = 0; i < headers.length && i < values.length; i++) {
            record.put(headers[i].trim(), values[i].trim());
        }

        return record;
    }

    /**
     * Extract year from date string (supports multiple formats: M/d/yyyy,
     * yyyy-MM-dd, dd/MM/yyyy)
     */
    public static int getYear(String dateStr) {
        SimpleDateFormat[] formats = { DATE_FORMAT_1, DATE_FORMAT_2, DATE_FORMAT_3 };

        for (SimpleDateFormat format : formats) {
            try {
                Date date = format.parse(dateStr);
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                return cal.get(Calendar.YEAR);
            } catch (ParseException e) {
                // Try next format
            }
        }
        return -1;
    }

    /**
     * Extract month from date string (supports multiple formats: M/d/yyyy,
     * yyyy-MM-dd, dd/MM/yyyy)
     * Returns 1-12
     */
    public static int getMonth(String dateStr) {
        SimpleDateFormat[] formats = { DATE_FORMAT_1, DATE_FORMAT_2, DATE_FORMAT_3 };

        for (SimpleDateFormat format : formats) {
            try {
                Date date = format.parse(dateStr);
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                return cal.get(Calendar.MONTH) + 1; // Calendar.MONTH is 0-based
            } catch (ParseException e) {
                // Try next format
            }
        }
        return -1;
    }

    /**
     * Parse double value safely
     */
    public static double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /**
     * Check if a line is a header line
     */
    public static boolean isHeader(String line) {
        return line.contains("location_id") || line.contains("date") ||
                line.contains("temperature") || line.contains("precipitation");
    }

    /**
     * Get ordinal suffix for month number (1st, 2nd, 3rd, etc.)
     */
    public static String getOrdinal(int number) {
        if (number >= 11 && number <= 13) {
            return number + "th";
        }
        switch (number % 10) {
            case 1:
                return number + "st";
            case 2:
                return number + "nd";
            case 3:
                return number + "rd";
            default:
                return number + "th";
        }
    }
}
