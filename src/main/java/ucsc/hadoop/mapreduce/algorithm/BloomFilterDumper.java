package ucsc.hadoop.mapreduce.algorithm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;

import ucsc.hadoop.avro.AvroBytesRecord;

public class BloomFilterDumper {

	public static BloomFilter readFromAvro(InputStream is) throws IOException {
		DataFileStream<Object> reader = new DataFileStream<Object>(is,
				new GenericDatumReader<Object>());

		reader.hasNext();
		BloomFilter filter = new BloomFilter();
		AvroBytesRecord
				.fromGenericRecord((GenericRecord) reader.next(), filter);
		IOUtils.closeQuietly(is);
		//IOUtils.closeQuietly(reader);

		return filter;
	}

	public static BloomFilter fromFile(File f) throws IOException {
		return readFromAvro(FileUtils.openInputStream(f));
	}

	public static BloomFilter fromPath(Configuration config, Path path)
			throws IOException {
		FileSystem hdfs = path.getFileSystem(config);

		return readFromAvro(hdfs.open(path));
	}

	public static void main(String... args) throws Exception {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path destFile = new Path(args[0]);

		InputStream is = hdfs.open(destFile);
		System.out.println(readFromAvro(is));
	}

}
