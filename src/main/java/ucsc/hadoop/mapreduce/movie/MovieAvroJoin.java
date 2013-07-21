package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.mapreduce.util.ConfigurationUtil;

public class MovieAvroJoin extends Configured implements Tool {
	private static final String MERGED_SCHEMA_JSON = "mergedSchema";

	private static Schema MERGED_SCHEMA = null;

	public static PathFilter PATH_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return !path.getName().startsWith("_")
					&& !path.getName().startsWith(".");
		}
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MovieAvroJoin(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.println("Usage: MovieAvroJoin <imdb> <imdb-weight> <output>");
			return -1;
		}

		FileSystem fs = FileSystem.getLocal(getConf());
		Path path1 = new Path(args[0]);
		Schema schema1 = getSchema(path1, fs);
		System.out.println("schema from data file: " + path1.getName() + ": "
				+ schema1.toString(true));

		getConf().setStrings(path1.getName(), schema1.toString(true));

		Path path2 = new Path(args[1]);
		Schema schema2 = getSchema(path2, fs);
		System.out.println("schema from data file: " + path2.getName() + ": "
				+ schema2.toString(true));

		getConf().setStrings(path2.getName(), schema2.toString(true));

		MERGED_SCHEMA = mergeSchema(schema1, schema2);
		System.out.println("merged schema : " + MERGED_SCHEMA.toString(true));

		getConf().setStrings(MERGED_SCHEMA_JSON, MERGED_SCHEMA.toString(true));

		Schema unionSchema = Schema.createUnion(Arrays.asList(schema1, schema2));

		Path output = new Path(args[2]);
		fs.delete(output, true);
		
		ConfigurationUtil.dumpConfigurations(getConf(), System.out);

		Job job = new Job(getConf());
		job.setJobName("Movie Avro");

		// mapper
		AvroJob.setInputKeySchema(job, unionSchema);
		job.setMapperClass(MovieAvroJoinMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(AvroValue.class);
		AvroJob.setMapOutputValueSchema(job, MERGED_SCHEMA);
		
		// reducer
		job.setReducerClass(MovieAvroJoinReducer.class);
		AvroJob.setOutputKeySchema(job, MERGED_SCHEMA);

        // input data sets
		FileInputFormat.addInputPath(job, path1);
		FileInputFormat.addInputPath(job, path2);

		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}

	}

	private Schema mergeSchema(Schema... schemas) {
		Set<Field> fieldSet = new HashSet<Field>();
		for (Schema schema : schemas) {
			fieldSet.addAll(schema.getFields());
		}

		List<Field> fieldList = new ArrayList<Field>(fieldSet.size());
		for (Field field : fieldSet) {
			Field f = new Field(field.name(), ReflectData.makeNullable(field.schema()), field.doc(),
					field.defaultValue());
			fieldList.add(f);
		}
		Schema result = Schema.createRecord("MovieJoin", null, null, false);
		result.setFields(fieldList);

		return result;
	}

	public static class MovieAvroJoinMapper
			extends
			Mapper<AvroKey<GenericData.Record>, NullWritable, IntWritable, AvroValue<GenericData.Record>> {

		private final static IntWritable YEAR = new IntWritable();
		private String fileName;
		private String schemaJSON;
		private String mergeSchemaJSON;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path fileInputPath = ((FileSplit) context.getInputSplit())
					.getPath();
			String fileName = (fileInputPath != null) ? fileInputPath.getName()
					: "unknown";
			
			schemaJSON = context.getConfiguration().get(fileName);
			mergeSchemaJSON = context.getConfiguration().get(MERGED_SCHEMA_JSON);
			
			System.out.println("schemaJSON for " + fileName + " is " + schemaJSON);
			
			System.out.println("mergeSchemaJSON  is " + mergeSchemaJSON);
		}

		@Override
		public void map(AvroKey<GenericData.Record> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			GenericData.Record genericRecord = key.datum();
			Integer year = (Integer) genericRecord.get("year");

			YEAR.set(year.intValue());
			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(mergeSchemaJSON);
			GenericData.Record mergedRecord = new GenericData.Record(schema);
			
			Schema partSchema = genericRecord.getSchema();
			for (Field field : partSchema.getFields())
			{
				Object obj = genericRecord.get(field.name());
				mergedRecord.put(field.name(), obj);
			}
			context.write(YEAR,
					new AvroValue<GenericData.Record>(mergedRecord));
		}
	}

	public static class MovieAvroJoinReducer
			extends
			Reducer<IntWritable, AvroValue<GenericData.Record>, AvroKey<GenericData.Record>, NullWritable> {

		private int year;
		
		@Override
		public void reduce(IntWritable key,
				Iterable<AvroValue<GenericData.Record>> values, Context context)
				throws IOException, InterruptedException {

			Map<String, List<GenericData.Record>> movieToGenericRecordList = new HashMap<String, List<GenericData.Record>>();
			if (key.get() != year)
			{
				year = key.get();
				System.out.println("***** year: " + year);
			}
			
			for (AvroValue<GenericData.Record> gr : values) {
				String title = (String) gr.datum().get("title");
				if (title != null)
				{
					List<GenericData.Record> genericRecordList = movieToGenericRecordList
							.get(title);
	
					if (genericRecordList == null) {
						genericRecordList = new ArrayList<GenericData.Record>();
					}
					GenericData.Record recordCopy = new GenericData.Record(gr.datum(), true);
					genericRecordList.add(recordCopy);
					movieToGenericRecordList.put(title, genericRecordList);
				} else {
					System.out.println("***** title is null ******");
				}
			}
			
			System.out.println("before looping year: " + key.get() + " movieToGenericRecordList: " + movieToGenericRecordList);

			for (Map.Entry<String, List<GenericData.Record>> entry : movieToGenericRecordList
					.entrySet()) {
				StringBuilder buf = new StringBuilder();
				GenericData.Record resultGR = null;
				for (GenericData.Record gr : entry.getValue()) {
					String actor = (String) gr.get("actor");
					if (actor != null) {
						if (buf.length() > 0) {
							buf.append(",");
						}
						buf.append(actor);
					} else {
						resultGR = gr;
					}
				}
				if (resultGR != null) {
					resultGR.put("actor", buf.toString());
					context.write(new AvroKey<GenericData.Record>(resultGR),
							NullWritable.get());
				} else {
					System.out
							.println("****** resultGR is null, NOT GOOD for year: "
									+ key);
				}

			}

		}
	}

	protected Schema getSchema(Path path, FileSystem fs) throws IOException {
		/* get path of the last file */
		Path lastFile = getLast(path, fs);

		/* read in file and obtain schema */
		GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
		InputStream hdfsInputStream = fs.open(lastFile);
		DataFileStream<Object> avroDataStream = new DataFileStream<Object>(
				hdfsInputStream, avroReader);
		Schema ret = avroDataStream.getSchema();
		avroDataStream.close();

		return ret;
	}

	public static Path getLast(String path, FileSystem fs) throws IOException {
		return getLast(new Path(path), fs);
	}

	/**
	 * get last file of a hdfs path if it is a directory; or return the file
	 * itself if path is a file
	 */
	public static Path getLast(Path path, FileSystem fs) throws IOException {

		FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

		if (statuses.length == 0) {
			return path;
		} else {
			Arrays.sort(statuses);
			return statuses[statuses.length - 1].getPath();
		}
	}
 //http://apache-avro.679487.n3.nabble.com/Multiple-input-schemas-in-MapReduce-td2928590.html
}
