package ucsc.hadoop.mapreduce.movie;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

public class MovieGrepDistributedCache extends Configured implements Tool {

	private static final String REGEX_CONFIG_NAME = "search.mappper.regex";
	
	private static final Log LOG = LogFactory.getLog(MovieGrepDistributedCache.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieGrepDistributedCache(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: moviesdistcache <in> <out> <regex>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1] + " regex: " + args[2]);
		
		Job job = new Job(conf, "movie distributed cache");
		job.getConfiguration().set(REGEX_CONFIG_NAME, args[2]);
		
		job.setJarByClass(MovieGrepDistributedCache.class);
		
    	job.setMapperClass(MovieMapper.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	private static class MovieMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Pattern regexPattern;
		private MovieWeightData movieWeightLookupService;
		private Text movieWithWeight = new Text();

		@Override
		public void setup(Context context) throws IOException {
			String pattern = context.getConfiguration().get(REGEX_CONFIG_NAME);
			if (LOG.isDebugEnabled()) {
				LOG.debug("pattern: " + pattern);
			}
			regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			LOG.info(paths);
			
			if (paths.length != 1) {
				LOG.info("expecting one file, but found: " + paths.length);
			} else {
				// side data file
				LOG.info("movie weights file: " + paths[0]);
				movieWeightLookupService = new MovieWeightData();
				movieWeightLookupService.initialize(new File(paths[0].toString()));
			}
		}
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] tokens = line.toString().split("\\t");
			if (tokens.length == 3) {
				Matcher matcher = regexPattern.matcher(line);
				if (matcher.find()) {
					String weight = movieWeightLookupService.getWeight(tokens[1], tokens[2]);
					if (weight != null) {
						movieWithWeight.set(value.toString() + "\t" + weight);
						context.write(movieWithWeight, NullWritable.get());
					} else {
						LOG.info("no weight for " + tokens[1] + " - " +  tokens[2]);
					}
				}
			}
		}
	}
	
}
