package ucsc.hadoop.mapreduce.movie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Test;

public class MovieCountTest {
	
	private static final Logger LOGGER = Logger.getLogger(MovieCountTest.class);
	
	@Test
	public void mapperTest() throws Exception {
		LOGGER.info("..... inside mapper test");
		
		MapDriver<Object, Text, IntWritable, IntWritable> mapDriver = new MapDriver<Object, Text, IntWritable, IntWritable>();
		
		mapDriver.withMapper(new MovieCount.MovieTokenizerMapper())
		.withInput(new IntWritable(10), new Text("Cooper, Chris (I)	Seabiscuit	2003"))
		.withOutput(new IntWritable(2003), new IntWritable(1))
		.runTest();
			
		System.out.println("expected output:" + mapDriver.getExpectedOutputs());
	}
	
	@Test
	public void reducerTest() throws Exception {
		ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> reduceDriver =
					new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>();
		
		List<IntWritable> valueList = new ArrayList<IntWritable>();
		valueList.addAll(Arrays.asList(new IntWritable(2), new IntWritable(3), new IntWritable(4)));
		
		IntWritable year = new IntWritable(2003);
		reduceDriver.withReducer(new MovieCount.MovieYearReducer())
		.withInput(year, valueList)
		.withOutput(year, new IntWritable(9))
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
	

}
