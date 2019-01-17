package io.datamass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapWordCount extends Mapper<LongWritable, Text, Text, Text> {

    public static final String TOTAL = "TOTAL";

    public enum DataType {
        BOOLEAN, DOMAIN_NAME, IP_ADDRESS, STRING, INTEGER, FILE_PATH
    }

    public static ArrayList headersList = new ArrayList<String>();

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        System.out.println("Key: " + key.toString() + " value: " + value.toString());

        String line = value.toString();
        String[] values = line.split("\t");

        //store header
        if (key.get() == 0) {

            for (String val : values) {
                headersList.add(val);
            }
            return;
        }

        for (int i = 0; i < values.length; i++) {
            String stringValue = values[i];
            DataType dataType = determineDataType(stringValue.toUpperCase().trim());
            String outputKey = headersList.get(i).toString();
            con.write(new Text(outputKey), new Text(dataType.toString()));
        }
        //int columnNumberToProcess = con.getConfiguration().getInt("columnNumberToProcess", 0);


        //TODO: Figure out how to pass this total to the reduce task?? Not working at the moment
        //System.out.println("NEW TOTAL: "+con.getConfiguration().getInt(TOTAL,1));
    }

    private DataType determineDataType(String input) {

        if (applyRegex("^(?i)(true|false)$", input)) {
            return DataType.BOOLEAN;
        }
        if (applyRegex("^((?!-)[A-Za-z0-9-_]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$", input)) {
            return DataType.DOMAIN_NAME;
        }
        if (applyRegex("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$", input)) {
            return DataType.IP_ADDRESS;
        }
        if (applyRegex("^\\d*$", input)) {
            return DataType.INTEGER;
        }
        if (applyRegex("^\\/.*$", input)) {
            return DataType.FILE_PATH;
        }

        System.out.println("Input '" + input + "' was not recognised in terms of data type, defaulting to string");
        return DataType.STRING;
    }

    private Boolean applyRegex(String regExPattern, String input) {
        final Pattern patternCompiled = Pattern.compile(regExPattern);
        return patternCompiled.matcher(input).matches();
    }

}
