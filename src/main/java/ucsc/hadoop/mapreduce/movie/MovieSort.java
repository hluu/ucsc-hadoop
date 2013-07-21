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
			System.err.println("Usage: moviesort [-D column=YEAR|MOVIE] <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie sort");
		job.setJarByClass(MovieCount.class);
		
		String sortColumnName = conf.get("column", "YEAR");
    	LOG.info("---- sortColumnName: " + sortColumnName);
    	
    	if ("YEAR".equals(sortColumnName)) {
    		job.setMapperClass(MovieSortByYearMapper.class);
    		job.setMapOutputKeyClass(IntWritable.class);
    		job.setMapOutputValueClass(Text.class);
    	} else {
    		job.setMapperClass(MovieSortByMovieTitleMapper.class);
    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(IntWritable.class);
    	}
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static class MovieSortByYearMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final static IntWritable YEAR = new IntWritable();
		private final static Text MOVIE = new Text();
		
		private String sortColumnName = null;
		
		@Override 
	    protected void setup(Context context) throws IOException, InterruptedException {
			sortColumnName = context.getConfiguration().get("column", "YEAR");
	    	LOG.info("---- sortColumnName: " + sortColumnName);
	    }
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[2]);
				YEAR.set(year);
				MOVIE.set(tokens[0]);
				context.write(YEAR, MOVIE);
			}
			
		}
	}

	public static class MovieSortByMovieTitleMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable YEAR = new IntWritable();
		private final static Text MOVIE = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[2]);
				YEAR.set(year);
				MOVIE.set(tokens[0]);
				context.write(MOVIE, YEAR);
			}
			
		}
	}
	
}
