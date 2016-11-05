package stubs;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class WordCount extends Configured implements Tool  {
	  public static void main(String[] args) throws Exception {
		  int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
		  System.exit(exitCode);
		  
	  }
	
	
	  public int run(String[] args) throws Exception{

    /*
     * The expected command-line arguments are the paths containing
     * input and output data. Terminate the job if the number of
     * command-line arguments is not exactly 2.
     */
    if (args.length != 2) {
      System.out.printf(
          "Usage: WordCount <input dir> <output dir>\n");
      return -1;
    }

    Job job=new Job(getConf());
    
    job.setJarByClass(WordCount.class);

    job.setJobName("Word Count with partition");

    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    
    job.setMapperClass(WordMapper.class);
    job.setReducerClass(SumReducer.class);


    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(3);
    job.setPartitionerClass(SentimentPartitioner.class);

    boolean success = job.waitForCompletion(true);
    return (success ? 0 : 1);
  }
}
