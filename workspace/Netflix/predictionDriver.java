package stubs;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* 
 * Prediction phase. Input: (MovieID,UserID,Rating) from training data
 * Load similarity matrix and testing data into memory 
 * Output: (PredictingMovieID, PredictingUserID, actualRating, predictedRating)
 * 
 */

public class predictionDriver extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(predictionDriver.class);
		job.setJobName("Prediction");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(predictionMapper.class);
		job.setReducerClass(predictionReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("usage predictionDriver <input> <output>");
			System.exit(1);
		}

		int returnStatus = ToolRunner.run(new predictionDriver(), args);
		System.exit(returnStatus);
	}

}