// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
package ucsc.hadoop.mapreduce.weather;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxTemperature {

  public static void main(String[] args) throws IOException {
   
    
    JobConf conf = new JobConf(MaxTemperature.class);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length != 2) {
        System.err.println("Usage: MaxTemperature <input path> <output path>");
        System.exit(-1);
      }
    conf.setJobName("Max temperature");

    FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
  }
}
// ^^ MaxTemperature
