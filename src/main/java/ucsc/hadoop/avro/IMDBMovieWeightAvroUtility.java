package ucsc.hadoop.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;

/**
 * This utility class can convert the movie data set from text to Avro format according to the 
 * schema defined in MOVIE_WEIGHT_AVRO_SCHEMA.
 * 
 * @author hluu
 *
 */
public class IMDBMovieWeightAvroUtility {
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: IMDBMovieWeightAvroUtility convert <input file> <output file>");
			System.out.println("Usage: IMDBMovieWeightAvroUtility read <input file> <how many>");
			System.exit(-1);
		}
		
		String command = args[0];
		if ("convert".equals(command)) {
			File inputFile = new File(args[1]);
			if (!inputFile.exists()) {
				System.out.println("Input file: " + inputFile + " doesn't exist");
				System.exit(-1);
			}
			
			File outputFile = new File(args[2]);
			if (outputFile.exists()) {
				outputFile.delete();
			}
			
			writeAvro(inputFile, outputFile);
		} else if ("read".equals(command)) {
			File inputFile = new File(args[1]);
			if (!inputFile.exists()) {
				System.out.println("Input file: " + inputFile + " doesn't exist");
				System.exit(-1);
			}
			
			int howMany = Integer.parseInt(args[2]);
			readAvro(inputFile, howMany);
		}
		
	}

	private static void readAvro(File inputFile, int howMany) throws IOException {
		System.out.println("******** readAvro from " + inputFile + " for " + howMany + " ***********");
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inputFile, reader);
		
		Schema movieSchema = dataFileReader.getSchema();
		System.out.println("Movie schema from data file: " + movieSchema.toString(true));
		
		GenericData.Record record = new GenericData.Record(movieSchema);
		int counter = 0;
		while (dataFileReader.hasNext()) {
			dataFileReader.next(record);
			System.out.println(record.get("title").toString() +  "\t" + 
			                   record.get("year").toString() + "\t" + 
					           record.get("weight"));
			
			counter++;
			
			if (counter == howMany) {
				break;
			}
		}
		dataFileReader.close();
	}

	private static void writeAvro(File inputFile, File outputFile) throws IOException {
		
		InputStream schemaIS = MovieAvroDemo.class.getResourceAsStream(MovieSchemaConstant.MOVIE_WEIGHT_AVRO_SCHEMA);
		if (schemaIS == null) {
			throw new IllegalStateException("Unable to find " + MovieSchemaConstant.MOVIE_WEIGHT_AVRO_SCHEMA);
		}
		
		Schema.Parser parser = new Schema.Parser();
		Schema movieSchema = parser.parse(schemaIS);
		
		System.out.println("******** writing data in Avro format ***********");
		GenericData.Record record = new GenericData.Record(movieSchema);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(movieSchema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);

		dataFileWriter.setCodec(CodecFactory.deflateCodec(9));
		
		dataFileWriter.create(movieSchema, outputFile);
		
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line  = null;
		int count = 0;
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split("\\t");
			if (tokens.length == 3) {
				record.put("title", new Utf8(tokens[0]));
				record.put("year", Integer.parseInt(tokens[1]));
				record.put("weight", Float.parseFloat(tokens[2]));
				dataFileWriter.append(record);
				count++;
			}
			
			if ((count % 100) == 0) {
				System.out.println("writing: " + count);
				//break;
			}
		}
		
		reader.close();
		dataFileWriter.close();
		
		System.out.println("******** finished writing " + count + " movies *******");
		System.out.println("******** finished writing data in Avro format to " + outputFile + " ***********");
	}
}
