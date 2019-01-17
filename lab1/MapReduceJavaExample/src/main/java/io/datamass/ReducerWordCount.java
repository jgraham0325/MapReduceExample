package io.datamass;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerWordCount extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text columnName, Iterable<Text> dataTypes, Context con) throws IOException, InterruptedException {

        HashMap<String, Integer> dataTypesMap = new HashMap();
        double count = 0;
        for (Text singleRowDataType : dataTypes) {
            count++;
            int existingCount = dataTypesMap.containsKey(singleRowDataType.toString()) ? dataTypesMap.get(singleRowDataType.toString()) : 0;
            dataTypesMap.put(singleRowDataType.toString(), existingCount + 1);
        }

        Map.Entry<String, Integer> maxEntry = findMaxEntry(dataTypesMap);

        for (Map.Entry<String, Integer> entry : dataTypesMap.entrySet()) {
            double percentage = (entry.getValue() / count) * 100;
            Text keyText = new Text("Column: " + StringUtils.rightPad("'" + columnName.toString() + "'", 15, ' ') +
                    " DataType: " + StringUtils.rightPad(entry.getKey().toString(), 10, ' '));
            Text valueText = new Text(" Percentage: " + new DecimalFormat("#.#").format(percentage) + " Prediction: " + maxEntry.getKey());
            con.write(keyText, valueText);
        }
    }

    private Map.Entry<String, Integer> findMaxEntry(HashMap<String, Integer> dataTypesMap) {
        Map.Entry<String, Integer> maxEntry = null;

        for (Map.Entry<String, Integer> entry : dataTypesMap.entrySet()) {
            if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                maxEntry = entry;
            }
        }
        return maxEntry;
    }
}

