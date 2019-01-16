package io.datamass;


import static io.datamass.MapWordCount.TOTAL;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class ReducerWordCount extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    public void reduce(Text word, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException
    {
        double sum = 0;
        for(DoubleWritable value : values)
        {
            sum += value.get();
        }
        con.write(word, new DoubleWritable(sum));
    }
}

