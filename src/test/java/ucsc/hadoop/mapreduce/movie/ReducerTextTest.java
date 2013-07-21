package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ReducerTextTest {
	
	@Test
	public void reducerTest() throws Exception {
		ReduceDriver<Text, Text, Text, Text> reduceDriver =
					new ReduceDriver<Text, Text, Text, Text>();
		
		List<Text> valueList = new ArrayList<Text>();
		valueList.addAll(Arrays.asList(new Text("John"), new Text("Mary"), new Text("Kary")));
		
		Text movie = new Text("Once in a lifetime");
		reduceDriver.withReducer(new TextReducer())
		.withInput(movie, valueList)
		.withOutput(movie, new Text("John, Mary, Kary"))
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
	
	public static class TextReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		
		@Override
		public void reduce(Text movie, Iterable<Text> actors, Context context) 
				 throws IOException, InterruptedException {
				
			StringBuilder buf = new StringBuilder();
			for (Text actor : actors) {
				if (buf.length() > 0) {
					buf.append(", ");
				}
				buf.append(actor.toString());
			}
			result.set(buf.toString());
			context.write(movie, result);
		}
	}
}
