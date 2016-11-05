package stubs;


import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Agg  extends Configured implements Tool {

   private static Logger THE_LOGGER = Logger.getLogger(Agg.class);

   public int run(String[] args) throws Exception {
      Job job = new Job(getConf());
      job.setJobName("Agg");

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);

      job.setMapperClass(AggMapper.class);
      job.setReducerClass(AggReducer.class);
      job.setCombinerClass(AggReducer.class);


      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      boolean status = job.waitForCompletion(true);
      THE_LOGGER.info("run(): status="+status);
      return status ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
      // Make sure there are exactly 2 parameters
      if (args.length != 2) {
         THE_LOGGER.warn("usage Agg <input> <output>");
         System.exit(1);
      }

      THE_LOGGER.info("inputDir="+args[0]);
      THE_LOGGER.info("outputDir="+args[1]);
      int returnStatus = ToolRunner.run(new Agg(), args);
      System.exit(returnStatus);
   }

}