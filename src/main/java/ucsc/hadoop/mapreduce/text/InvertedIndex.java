package ucsc.hadoop.mapreduce.text;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

public class InvertedIndex extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(InvertedIndex.class);
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new InvertedIndex(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: invertedIndex <in> <out>");
			System.exit(-1);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
	
		Job job = new Job(conf, "inverted index");
		
		job.setJarByClass(Search.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private Text wordHolder = new Text();
		private Text fileHolder = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			Path fileInputPath = ((FileSplit) context.getInputSplit()).getPath();
			String fileName = (fileInputPath != null) ? fileInputPath.getName() : "unknown";
			fileHolder.set(fileName);
			
			String line = value.toString();
			String[] words = line.split(" ");
			
			for (String word : words) {
				wordHolder.set(word);
				context.write(wordHolder, fileHolder);
			}
		}
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text word, Iterable<Text> fileNames, Context context) 
				 throws IOException, InterruptedException {
			
			for (Text fileName : fileNames) {
				context.write(word, fileName);
				break;
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		private Text fileNameList = new Text();
		
		@Override
		public void reduce(Text word, Iterable<Text> fileNames, Context context) 
				 throws IOException, InterruptedException {
			StringBuilder buf = new StringBuilder();
			
			for (Text fileName : fileNames) {
				if (buf.length() > 0) {
					buf.append(",");
				}
				buf.append(fileName.toString());
			}
			fileNameList.set(buf.toString());
			context.write(word, fileNameList);
		}
	}
}
