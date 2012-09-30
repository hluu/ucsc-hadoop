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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

/**
 * Sort movies by either year or weight.  This will work against the ibdb-weights.tsv
 * 
 * @author hluu
 *
 */
public class MovieSort extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(MovieSort.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieSort(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: movieSort <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie sort");
		job.setJarByClass(MovieCount.class);
		job.setMapperClass(MovieTokenizerMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static class MovieTokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final static IntWritable YEAR = new IntWritable();
		private final static Text MOVIE = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[1]);
				YEAR.set(year);
				MOVIE.set(tokens[0]);
				context.write(YEAR, MOVIE);
			}
			
		}
	}

}
