package stubs;


import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


public class TopNMapper extends
   Mapper<LongWritable, Text, NullWritable, Text> {

   private int N = 3; // default
   private SortedMap<Double, String> top = new TreeMap<Double, String>();

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String line = value.toString();
      String[] strs = line.split("\\W+");
      if (strs.length < 2) return;
      String keyAsString = strs[0];
      double score = Double.parseDouble(strs[1]);
      
      String compositeValue = keyAsString + "," + score;
      top.put(score, compositeValue);
      // keep only top N
      if (top.size() > N) {
         top.remove(top.firstKey());
      }
   }
   
   @Override
   protected void setup(Context context) throws IOException,
         InterruptedException {
      this.N = context.getConfiguration().getInt("N", 10); // default is top 10
   }
   

   @Override
   protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (String str : top.values()) {
         context.write(NullWritable.get(), new Text(str));
      }
   }

}