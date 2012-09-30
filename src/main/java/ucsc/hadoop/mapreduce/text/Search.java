package ucsc.hadoop.mapreduce.text;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

public class Search extends Configured implements Tool {
	private static final String REGEX_CONFIG_NAME = "search.mappper.regex";

	private static final Log LOG = LogFactory.getLog(Search.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Search(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: search <in> <out> <regex>");
			System.exit(-1);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1] + " regex: " + args[2]);
	
		Job job = new Job(conf, "search");
		job.getConfiguration().set(REGEX_CONFIG_NAME, args[2]);
		
		job.setJarByClass(Search.class);
		job.setMapperClass(SearchMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static class SearchMapper extends Mapper<Object, Text, Text, Text> {
		private Pattern regexPattern;
		private Text file = new Text();
		private Text matchedLine = new Text();
		
		@Override
		public void setup(Context context) {
			String pattern = context.getConfiguration().get(REGEX_CONFIG_NAME);
			if (LOG.isDebugEnabled()) {
				LOG.debug("pattern: " + pattern);
			}
			
			regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		  }
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			//ConfigurationUtil.dumpConfigurations(context.getConfiguration(), System.out);
			Path fileInputPath = ((FileSplit) context.getInputSplit()).getPath();
			String fileName = (fileInputPath != null) ? fileInputPath.getName() : "unknown";
			String line = value.toString();
			Matcher matcher = regexPattern.matcher(line);
			if (matcher.find()) {
				file.set(fileName);
				matchedLine.set(line);
				context.write(file, matchedLine);
			}
		}
	}
}
