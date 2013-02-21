package ucsc.hadoop.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Writable;

public class AvroBytesRecord {

	public static final String BYTES_FIELD = "b";
	private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"SmallFilesTest\", "
			+ "\"fields\": ["
			+ "{\"name\":\""
			+ BYTES_FIELD
			+ "\", \"type\":\"bytes\"}]}";
	public static final Schema SCHEMA = new Parser().parse(SCHEMA_JSON); //Schema.parse(SCHEMA_JSON);

	public static Record toGenericRecord(byte[] bytes) {
		Record record = new GenericData.Record(SCHEMA);
		record.put(BYTES_FIELD, ByteBuffer.wrap(bytes));
		return record;
	}

	public static Record toGenericRecord(Writable writable)
			throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dao = new DataOutputStream(baos);
		writable.write(dao);
		dao.close();
		return toGenericRecord(baos.toByteArray());
	}

	public static void fromGenericRecord(GenericRecord r, Writable w)
			throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(
				fromGenericRecord(r));
		DataInputStream dis = new DataInputStream(bais);
		w.readFields(dis);
	}

	public static byte[] fromGenericRecord(GenericRecord record) {
		return ((ByteBuffer) record.get(BYTES_FIELD)).array();
	}

}
