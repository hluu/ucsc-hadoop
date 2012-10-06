package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class MovieSortByYearMaxWeight extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(MovieSortByYearMaxWeight.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieSortByYearMaxWeight(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviesortbyyearmaxweight <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie secondary sort");
		job.setJarByClass(MovieSortByYearMaxWeight.class);
		
    	job.setMapperClass(MovieMapper.class);
    	job.setReducerClass(MovieReducer.class);
    	
    	job.setMapOutputKeyClass(IntWritable.class);
    	job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	private static class MovieMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		private final static IntWritable YEAR = new IntWritable();
		private final static DoubleWritable WEIGHT = new DoubleWritable();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[1]);
				YEAR.set(year);
				WEIGHT.set(Double.parseDouble(tokens[2]));
				context.write(YEAR, WEIGHT);
			}
		}
	}
	
	private static class MovieReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
		@Override
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
				 throws IOException, InterruptedException {

			double maxWeightPerKey = Double.MIN_VALUE;
			for (DoubleWritable val : values) {
				if (val.get() > maxWeightPerKey) {
					maxWeightPerKey = val.get();
				}
		    }
			context.write(key, new DoubleWritable(maxWeightPerKey));
		}
	}

}
