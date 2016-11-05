package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
	  String file_name =((FileSplit)context.getInputSplit()).getPath().getName();
	  String line = value.toString();
	  String[] strs = line.split("\t");
	  if (strs != null && strs.length > 1) {
		  String index = strs[0];
		    for (String word : strs[1].split("\\W+")) {
			      if (word.length() > 0) {
			        context.write(new Text(word), new Text(file_name + "@" + index));
			      }
			 }
	  }
  }
}