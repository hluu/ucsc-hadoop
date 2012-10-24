package ucsc.hadoop.mapreduce.movie;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

public class MovieAvroCount extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
	
	public int run(String[] args) throws Exception {
		return 0;
	}
	
	public static class MovieTokenizerMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
		private final static IntWritable YEAR = new IntWritable();
		private final static Text MOVIE = new Text();
		
		@Override
		public void map(Utf8 line, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) 
				throws IOException {
			/*
			
			if (tokens.length == 3) {
				int year = Integer.parseInt(tokens[2]);
				YEAR.set(year);
				MOVIE.set(tokens[1]);
				context.write(YEAR, MOVIE);
			} */
			
		}
	}

}
