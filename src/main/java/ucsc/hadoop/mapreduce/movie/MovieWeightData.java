package ucsc.hadoop.mapreduce.movie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * Convenient look up service for getting movie weight
 * 
 * @author hluu
 *
 */
public class MovieWeightData {

	private static final Log LOG = LogFactory.getLog(MovieWeightData.class);
	
	private Map<String, String> movieWeightMap = new HashMap<String,String>();
	
	public void initialize(File file) throws IOException {
		LOG.info("initializing cache with file: " + file);
		
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line = null;
		
		try {
			while ((line = in.readLine()) != null) {
				String[] tokens = line.toString().split("\\t");
				if (tokens.length == 3) {
					movieWeightMap.put(tokens[0] + "_" + tokens[1], tokens[2]);
				}
			}
		} finally {
			LOG.info("cache has: " + movieWeightMap.size() + " entries");
			org.apache.commons.io.IOUtils.closeQuietly(in);
		}
	}
	
	public String getWeight(String movie, String year) {
		return movieWeightMap.get(movie + "_" + year);
	}
}
