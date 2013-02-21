package ucsc.hadoop.avro;

import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;

public class AvroBytesRecordTest {
	@Test
	public void basicTest() {
		Record record = AvroBytesRecord.toGenericRecord("Hello there".getBytes());
		
		byte rawBytes[] = AvroBytesRecord.fromGenericRecord(record);
		
		String result = new String(rawBytes);
		
		System.out.println("result: " + result);
	}

}
