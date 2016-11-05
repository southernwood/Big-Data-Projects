package stubs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() throws  URISyntaxException {

		mpart = new SentimentPartitioner();
		mpart.setConf(new Configuration());
		int result;
		String tests[] = {"love", "deadly", "zodiac"};
		for (int i = 0; i < tests.length; i++) {
			result = mpart.getPartition(new Text(tests[i]), new IntWritable(1), 3);
			assertEquals(result, i);
		}
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */          
		
	}

}
