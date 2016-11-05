package stubs;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class AggMapper extends
         Mapper<LongWritable, Text, Text, DoubleWritable> {

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(",");
      if (tokens.length < 3) {
         return;
      }

      double score = Double.parseDouble(tokens[2]);
      context.write(new Text(tokens[0]), new DoubleWritable(score));
   }

}