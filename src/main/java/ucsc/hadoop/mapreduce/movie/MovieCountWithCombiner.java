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
 * Simple MapReduce application to count how many movies each actor plays
 * 
 * @author hluu
 *
 */
public class MovieCountWithCombiner extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(MovieCount.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviecountcombiner <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count with combiner");
		job.setJarByClass(MovieCountWithCombiner.class);
		job.setMapperClass(ActorMapper.class);
		
		if (conf.getBoolean("useCombiner", false)) {
			LOG.info("****** running WITH combiner ********");
			job.setCombinerClass(ActorReducer.class);	
		} else {
			LOG.info("****** running WITHOUT combiner ********");
		}

		job.setReducerClass(ActorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieCountWithCombiner(), args);
		System.exit(exitCode);
	}
	
	/**
	 * Mapper 
	 * @author hluu
	 *
	 */
	public static class ActorMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private final static Text ACTOR = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				ACTOR.set(tokens[0]);
				context.write(ACTOR, ONE);
			}
		}
	}
	
	
	/**
	 * Reducer
	 * @author hluu
	 *
	 */
	public static class ActorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text actor, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException {
				
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			result.set(sum);
			context.write(actor, result);
		}
	}
	
}
