package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopNReducer  extends
   Reducer<NullWritable, Text, IntWritable, Text> {

   private int N = 3; // default
   private HashMap<Integer, String> name_list = new HashMap<Integer, String>();
   private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

   @Override
   public void reduce(NullWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
	   
      for (Text value : values) {
         String valueAsString = value.toString().trim();
         String[] tokens = valueAsString.split(",");
         String url = tokens[0];
         int frequency =  Integer.parseInt(tokens[1]);
         top.put(frequency, url);
         // keep only top N
         if (top.size() > N) {
            top.remove(top.firstKey());
         }
      }
      
      // emit final top N
        List<Integer> keys = new ArrayList<Integer>(top.keySet());
        for(int i=keys.size()-1; i>=0; i--){
        	int idx = Integer.parseInt(top.get(keys.get(i)));
        	String name = name_list.get(idx);
        	context.write(new IntWritable(keys.get(i)), new Text(name));
      }
   }
   
   @Override
   protected void setup(Context context) 
      throws IOException, InterruptedException {
      this.N = context.getConfiguration().getInt("N", 3); // default is top 3
      File fs = new File("movie_titles.txt");
      readFile(fs);
   }
   

   private void readFile(File fs) {
 	          try{
 	              BufferedReader bufferedReader = new BufferedReader(new FileReader(fs));
 	              String word = null;
 	              while((word = bufferedReader.readLine()) != null) {
 	            	  String[] strs = word.split(",");
 	            	  if (strs.length < 3) continue;
 	            	  int index = Integer.parseInt(strs[0]);
 	            	  name_list.put(index,strs[2]);
 	              }
 	             bufferedReader.close();
 	          } catch(IOException ex) {
 	              System.err.println("Exception while reading stop words file: " + ex.getMessage());
 	          }
 	      }

 }

