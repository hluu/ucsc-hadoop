package ucsc.hadoop.mapreduce.movie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Custom writable type example for composite type. This is used in secondary
 * sort example
 * 
 * @author hluu
 * 
 */
public class MovieWritable implements WritableComparable<MovieWritable> {

	private IntWritable year;
	private Text title;

	public MovieWritable() {
		year = new IntWritable();
		title = new Text();
	}
	
	public void setYear(int year) {
		this.year = new IntWritable(year);
	}
	
	public int getYear() {
		return year.get();
	}

	public void setTitle(String title) {
		this.title = new Text(title);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		year.write(out);
		title.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		title.readFields(in);
	}

	/**
	 * Compare by year first. If same year, then compare title.
	 */
	@Override
	public int compareTo(MovieWritable o) {
		int yearResult = year.compareTo(o.year);

		//System.out.println("w1: " + toString() + " w2: " + o.toString());
		if (yearResult != 0) {
			return yearResult;
		}

		return title.compareTo(o.title);
	}
	
	@Override
	public String toString() {
		return year.toString() + "\t" + title;
	}

}
