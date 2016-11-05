package stubs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] words = line.split("\\W+");
    if (words != null && words.length > 1) {
    	int i = 0;
    	while (i < words.length - 1) {
    		if (words[i].length() == 0) {
    			++i;
    			continue;
    		}
    		int j = i+1;
    		while (j < words.length && words[j].length() == 0) ++j;
    		String[] pair = new String[2];
    		pair[0] = words[i].toLowerCase();
    		pair[1] = words[j].toLowerCase();
    		Arrays.sort(pair);
    		context.write(new Text(pair[0]+","+pair[1]), new IntWritable(1));
    		i = j;
    	}
    }
    
          
  }
}
