package ucsc.hadoop.mapreduce.algorithm;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import ucsc.hadoop.avro.AvroBytesRecord;
import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

/**
 * MapReduce application to generate a bloom filter of the movie data. The bloom
 * filter can be used to find out which movies in movie weight data set are in
 * the movies data set
 * 
 * @author hluu
 * 
 */
public class MovieBloomFilter extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(MovieBloomFilter.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieBloomFilter(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: bloomfilter <in> <out>");
			System.exit(2);
		}

		Configuration conf = getConf();
		ConfigurationUtil.dumpConfigurations(conf, System.out);

		LOG.info("input: " + args[0] + " output: " + args[1]);

		JobConf job = new JobConf();

		//AvroJob.setOutputSchema(job, AvroBytesRecord.SCHEMA);
		job.set(AvroJob.OUTPUT_SCHEMA, AvroBytesRecord.SCHEMA.toString());
		job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());

		job.setJarByClass(MovieBloomFilter.class);
		job.setMapperClass(MovieBlooFilterMapper.class);
		job.setReducerClass(MovieBlooFilterReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BloomFilter.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BloomFilter.class);

		job.setOutputFormat(AvroOutputFormat.class);

		// use only one reducer to combine all the partial bloom filters
		// together
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
		
		return 0;
		/*
		 * boolean result = job.waitForCompletion(true); return (result) ? 0 :
		 * 1;
		 */

	}

	private static BloomFilter createBloolFilter() {
		return new BloomFilter(500, 3, Hash.MURMUR_HASH);
	}

	public static class MovieBlooFilterMapper implements
			Mapper<Object, Text, NullWritable, BloomFilter> {
		private BloomFilter bFilter = createBloolFilter();

		private OutputCollector<NullWritable, BloomFilter> collector;
		
		@Override
		public void map(Object key, Text value,
				OutputCollector<NullWritable, BloomFilter> output,
				Reporter reporter) throws IOException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 3) {
				String title_year = tokens[1] + "_" + tokens[2];
				bFilter.add(new Key(title_year.getBytes()));
			}
			
			collector = output;
		}

		@Override
		public void close() throws IOException {
			collector.collect(NullWritable.get(), bFilter);
		}

		@Override
		public void configure(JobConf job) {
		}
	}

	public static class MovieBlooFilterReducer
			implements
			Reducer<NullWritable, BloomFilter, AvroWrapper<GenericRecord>, NullWritable> {

		private BloomFilter bFilter = createBloolFilter();
		private OutputCollector<AvroWrapper<GenericRecord>, NullWritable> collector;

		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void close() throws IOException {
			Record avroGenericRecord = AvroBytesRecord.toGenericRecord(bFilter);
			collector.collect(new AvroWrapper<GenericRecord>(avroGenericRecord),
					NullWritable.get());			
		}

		@Override
		public void reduce(
				NullWritable key,
				Iterator<BloomFilter> values,
				OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output,
				Reporter reporter) throws IOException {

			while (values.hasNext()) {
				bFilter.or(values.next());
			}
			collector = output;
		}

	}
}
