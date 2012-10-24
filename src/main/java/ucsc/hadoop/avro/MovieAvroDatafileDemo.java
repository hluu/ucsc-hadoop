package ucsc.hadoop.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
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
 * Example for writing data out to Avro data file that can consume by Hadoop
 * 
 * @author hluu
 *
 */
public class MovieAvroDatafileDemo {
	private static final String MOVIE_AVRO_SCHEMA = MovieSchemaConstant.MOVIE_AVRO_SCHEMA;
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		InputStream schemaIS = MovieAvroDemo.class.getResourceAsStream(MOVIE_AVRO_SCHEMA);
		if (schemaIS == null) {
			throw new IllegalStateException("Unable to find " + MOVIE_AVRO_SCHEMA);
		}
		Schema movieSchema = Schema.parse(schemaIS);
		
		File dataFile =  new File("/tmp/imbdb.avro");
		if (dataFile.exists()) {
			dataFile.delete();
		}
		
		writeAvro(movieSchema, dataFile);
		readAvro(dataFile);
	}
	
	private static void writeAvro(Schema movieSchema, File outputFile) throws IOException {
		System.out.println("******** writing data in Avro format ***********");
		GenericData.Record record = new GenericData.Record(movieSchema);
		
		record.put("title", new Utf8("Capote"));
		record.put("actor", new Utf8("Cooper, Chris (I)"));
		record.put("year", 2005);
		
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(movieSchema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		
		dataFileWriter.create(movieSchema, outputFile);
		
		dataFileWriter.append(record);
		dataFileWriter.close();
		
		System.out.println("******** finished writing data in Avro format to " + outputFile + " ***********");
	}
	
	private static void readAvro(File inputFile) throws IOException {
		System.out.println("******** reading data from Avro data file ***********");
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inputFile, reader);
		
		Schema movieSchema = dataFileReader.getSchema();
		System.out.println("Movie schema from data file: " + movieSchema.toString(true));
		
		GenericData.Record record = new GenericData.Record(movieSchema);
		while (dataFileReader.hasNext()) {
			dataFileReader.next(record);
			System.out.println("******** output from readAvro ***********");
			System.out.println("title: " + record.get("title"));
			System.out.println("actor: " + record.get("actor"));
			System.out.println("year: " + record.get("year"));
		}
		dataFileReader.close();
	}

}
