package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;


/**
 * Simple MapReduce application to count how many movies per year and only output years with
 * a configuration # of movies
 * 
 * @author hluu
 *
 */
public class MovieCountWithLimit extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(MovieCount.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		ConfigurationUtil.dumpConfigurations(conf, System.out);

		if (args.length != 2) {
			System.err.println("Usage: moviecountwithlimit <in> <out>");
			System.exit(2);
		}
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(MovieCountWithLimit.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieCountWithLimit(), args);
		System.exit(exitCode);
	}
	
	public static class MovieTokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private final static IntWritable YEAR = new IntWritable();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[2]);
				YEAR.set(year);
				context.write(YEAR, ONE);
			}
			
		}
	}
	
	public static class MovieYearReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static final Log LOG = LogFactory.getLog(MovieYearReducer.class);
		
		private IntWritable result = new IntWritable();
		
		private int minCount;
		
		@Override 
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	minCount = context.getConfiguration().getInt("minCount", 5);
	    	LOG.info("---- minCount: " + minCount);
	    }
		
		@Override
		public void reduce(IntWritable year, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException {
				
			int totalCnt = 0;
			for (IntWritable cnt : values) {
				totalCnt += cnt.get();
			}

			if (totalCnt >= minCount) {
				result.set(totalCnt);
				context.write(year, result);
			}
		}
	}
	
}