package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

/**
 * Demonstrating sorting movies by year and then by title within each year. 
 * Secondary sort
 * 
 * @author hluu
 *
 */
public class MovieSortByYearTitle extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(MovieSortByYearTitle.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieSortByYearTitle(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviesortbyyeartitle <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie secondary sort");
		job.setJarByClass(MovieSortByYearTitle.class);
		
    	job.setMapperClass(MovieMapper.class);
    	job.setReducerClass(MovieReducer.class);
    	
    	job.setMapOutputKeyClass(MovieWritable.class);
    	job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(MovieWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(MoviePartitioner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	private static class MovieMapper extends Mapper<Object, Text, MovieWritable, NullWritable> {
		private final static MovieWritable MOVIE = new MovieWritable();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[1]);
				MOVIE.setYear(year);
				MOVIE.setTitle(tokens[0]);
				context.write(MOVIE, NullWritable.get());
			}
		}
	}
	
	private static class MovieReducer extends Reducer<MovieWritable, NullWritable, MovieWritable, NullWritable> {
		
		@Override
		public void reduce(MovieWritable key, Iterable<NullWritable> values, Context context) 
				 throws IOException, InterruptedException {

			context.write(key, NullWritable.get());
		}
	}

	/**
	 * Partition movies by year
	 * 
	 * @author hluu
	 *
	 */
	private static class MoviePartitioner extends Partitioner<MovieWritable, NullWritable> {

		@Override
		public int getPartition(MovieWritable movie, NullWritable arg1, int partitionCount) {
			return Math.abs(movie.getYear() * 127) % partitionCount;
		}
		
	}
	
}
