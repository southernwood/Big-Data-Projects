package stubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

public class TestAvgWordLength {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

	    /*
	     * Set up the mapper test harness.
	     */
	    LetterMapper mapper = new LetterMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);

	    /*
	     * Set up the reducer test harness.
	     */
	    AverageReducer reducer = new AverageReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
	    reduceDriver.setReducer(reducer);

	    /*
	     * Set up the mapper/reducer test harness.
	     */
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }

	@Test
	  public void testMapper() {

	    /*
	     * For this test, the mapper's input will be "cat cats doggy" 
	     * TODO: implement
	     */
	  //  fail("Please implement test.");
	    mapDriver.withInput(new LongWritable(1), new Text("cat cats doggy"));
	    mapDriver.withOutput(new Text("c"),new IntWritable(3));
	    mapDriver.withOutput(new Text("c"),new IntWritable(4));
	    mapDriver.withOutput(new Text("d"),new IntWritable(5));
	   
	    mapDriver.runTest(); 
	    System.out.println("The input of testMapper are 'cat cats doggy'");
	    System.out.println("The output of testMapper are '(c,3),(c,4),c(5)'");
	  }
	@Test
	  public void testReducer() {

	    /*
	     * For this test, the reducer's input will be "c 3 4".
	     * The expected output is "cat 3.5".
	     * TODO: implement
	     */
	  //  fail("Please implement test.");
	   List<IntWritable> values=new ArrayList<IntWritable>();
	   values.add(new IntWritable(3));
	   values.add(new IntWritable(4));
	   
	    reduceDriver.withInput(new Text("c"),values);
	    reduceDriver.withOutput(new Text("c"),new DoubleWritable(3.5));
	    
	    reduceDriver.runTest();
	    System.out.println("The input of testReducer are '(c,[3,4])'");
	    System.out.println("The output of testReducer are '(c,3.5)'");
	  }

	
	@Test
	  public void testMapReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cats doggy" 
	     * The expected output (from the reducer) is "c 3,5", "d 5". 
	     * TODO: implement
	     */
	  //  fail("Please implement test.");
		  mapReduceDriver.withInput(new LongWritable(1),new Text("cat cats doggy"));
		  mapReduceDriver.addOutput(new Text("c"),new DoubleWritable(3.5));
		  mapReduceDriver.addOutput(new Text("d"),new DoubleWritable(5.0));
		  
		  mapReduceDriver.runTest();
		  System.out.println("The input of testMapReduce are 'cat cats doggy'");
		    System.out.println("The output of testMapReduce are '(c,3.5),(d,4)'");

	  }
		
}
