package ucsc.hadoop.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.util.Utf8;

/**
 * Simple example of writing and reading data in Avro format to byte array output stream
 * 
 * @author hluu
 *
 */
public class MovieAvroDemo {

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
		String avroData = writeAvro(movieSchema);
		readAvro(movieSchema, avroData);
	}
	
	private static void readAvro(Schema movieSchema, String avroData) throws IOException {
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(movieSchema);
		Decoder decoder = new JsonDecoder(movieSchema, avroData);
		
		GenericRecord record = reader.read(null, decoder);
		
		System.out.println("******** output from readAvro ***********");
		System.out.println("title: " + record.get("title"));
		System.out.println("actor: " + record.get("actor"));
		System.out.println("year: " + record.get("year"));
	}
	
	private static String writeAvro(Schema movieSchema) throws IOException {
		System.out.println("******** writing data in Avro format ***********");
		GenericData.Record record = new GenericData.Record(movieSchema);
		
		record.put("title", new Utf8("Capote"));
		record.put("actor", new Utf8("Cooper, Chris (I)"));
		record.put("year", 2005);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(movieSchema);
		Encoder encoder = new JsonEncoder(movieSchema, bos);
		//Encoder encoder = new BinaryEncoder(bos);
		writer.write(record, encoder);
		encoder.flush();
		bos.close();
		
		return new String(bos.toByteArray());
	}

}
