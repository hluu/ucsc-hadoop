package ucsc.hadoop.mapreduce.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieAvroCount extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(MovieAvroCount.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieAvroCount(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: avromoviecount <in> <out>");
			System.exit(2);
		}

		FileSystem fs = FileSystem.getLocal(getConf());
		Path path1 = new Path(args[0]);
		Schema movieSchema = getSchema(path1, fs);
		LOG.info("schema from data file: " + path1.getName() + ": "
				+ movieSchema.toString(true));

		Job job = new Job(getConf());
		job.setJobName("Movie count - reading data in Avro format");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MovieTokenizerMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MovieCountReducer.class);

		AvroJob.setInputKeySchema(job, movieSchema);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}

	protected Schema getSchema(Path path, FileSystem fs) throws IOException {

		/* read in file and obtain schema */
		GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
		InputStream hdfsInputStream = fs.open(path);
		DataFileStream<Object> avroDataStream = new DataFileStream<Object>(
				hdfsInputStream, avroReader);
		Schema ret = avroDataStream.getSchema();
		avroDataStream.close();

		return ret;
	}

	public static class MovieTokenizerMapper
			extends
			Mapper<AvroKey<GenericData.Record>, NullWritable, IntWritable, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private final static IntWritable YEAR = new IntWritable();

		@Override
		public void map(AvroKey<GenericData.Record> record, NullWritable value,
				Context context) throws IOException, InterruptedException {

			GenericData.Record genericRecord = record.datum();
			Integer year = (Integer) genericRecord.get("year");

			YEAR.set(year.intValue());
			context.write(YEAR, ONE);
		}
	}

	public static class MovieCountReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private final static IntWritable SUM = new IntWritable(1);

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			SUM.set(sum);
			context.write(key, SUM);

		}

		/*
		 * public void reduce(Integer year, Iterable<Integger> values,
		 * AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter)
		 * throws IOException {
		 * 
		 * collector.collect(new Pair<Integer, Integer>(year, ONE)); }
		 */
	}

}
