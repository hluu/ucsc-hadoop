package ucsc.hadoop.sequence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class IMDBSequenceData {
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: IMDBSequenceData convert <input file> <output file>");
			System.out.println("Usage: IMDBSequenceData read <input file> <how many>");
			System.exit(-1);
		}
		
		String command = args[0];
		File inputFile = new File(args[1]);
		if (!inputFile.exists()) {
			System.out.println("Input file: " + inputFile + " doesn't exist");
			System.exit(-1);
		}
		
		if ("convert".equals(command)) {
			File outputFile = new File(args[2]);
			if (outputFile.exists()) {
				outputFile.delete();
			}
			writeSequenceFile(inputFile, outputFile);
		} else if ("read".equals(command)) {
			int howMany = Integer.parseInt(args[2]);
			readSequenceFile(inputFile, howMany);
		} else {
			System.out.println("Unsupported command: " + command);
		}
	}

	private static void readSequenceFile(File inputFile, int howMany) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(inputFile.getAbsolutePath()), conf);
		Path path = new Path(inputFile.getAbsolutePath());
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			long position = reader.getPosition();
			int count = 0;
			while (reader.next(key, value)) {
				String sync = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, sync, key, value);
				if (count == howMany) {
					break;
				}
				position = reader.getPosition();
				count++;
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	private static void writeSequenceFile(File inputFile, File outputFile) throws IOException {
		
		System.out.println("******** writing data in sequence format ***********");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(outputFile.getAbsolutePath()), conf);
		Path path = new Path(outputFile.getAbsolutePath());
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		BufferedReader reader = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, 
					key.getClass(), value.getClass(), CompressionType.BLOCK);
			reader = new BufferedReader(new FileReader(inputFile));
			String line  = null;
			int count = 1;
			while ((line = reader.readLine()) != null) {
				key.set(count);
				value.set(line);
				writer.append(key, value);
				count++;
				
				if ((count % 100) == 0) {
					System.out.println("writing: " + count);
					//break;
				}
			}
			
			System.out.println("******** finished writing " + count + " movies *******");
			System.out.println("******** finished writing data in sequence format to " + outputFile + " ***********");
		} finally {
			IOUtils.closeStream(writer);
			if (reader != null) {
				reader.close();
			}
		}
	}
}
