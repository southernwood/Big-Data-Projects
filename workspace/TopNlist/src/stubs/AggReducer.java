package stubs;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AggReducer  extends
    Reducer<Text, DoubleWritable, Text, DoubleWritable> {

      @Override
      public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
         throws IOException, InterruptedException {
         
         int sum = 0;
         for (DoubleWritable value : values) {
               sum += value.get();
         }

         context.write(key, new DoubleWritable(sum));
      }
}