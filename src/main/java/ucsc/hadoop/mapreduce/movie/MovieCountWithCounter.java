package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;


/**
 * Simple MapReduce application to count how many movies per year and use
 * counter to track bad records
 * 
 * @author hluu
 *
 */
public class MovieCountWithCounter extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(MovieCount.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviecountwithcounter <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(MovieCount.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		if (result) {
			printCounter(job);
		}
		return (result) ? 0 : 1;
	}
	
	private void printCounter(Job job) throws IOException {
		System.out.println("============ displaying counters ============");
		Counters counters = job.getCounters();
		Counter badRecordCounter = counters.findCounter(MovieTokenizerMapper.Movie.BAD_RECORD);
		
        if (badRecordCounter != null)
        {
        	System.out.println(MovieTokenizerMapper.Movie.BAD_RECORD + " value: " + badRecordCounter.getValue());
        } else {
        	System.out.println("badRecordCounter is null");
        }
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieCountWithCounter(), args);
		System.exit(exitCode);
	}
	
	public static class MovieTokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final static Text MOVIE = new Text();
		private final static IntWritable YEAR = new IntWritable();
		
		public enum Movie {
			BAD_RECORD
		}
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[2]);
				YEAR.set(year);
				MOVIE.set(tokens[1]);
				context.write(YEAR, MOVIE);
			} else {
				context.getCounter(Movie.BAD_RECORD).increment(1);
			}
			
		}
	}
	
	public static class MovieYearReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(IntWritable year, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException {
				
			Set<String> movieSet = new HashSet<String>();
			for (Text movie : values) {
				movieSet.add(movie.toString());
			}
			result.set(movieSet.size());
			context.write(year, result);
			movieSet.clear();
		}
	}
	
}
