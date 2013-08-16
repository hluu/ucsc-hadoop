/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ucsc.hadoop.mapreduce;

import org.apache.hadoop.util.ProgramDriver;

import ucsc.hadoop.mapreduce.algorithm.MovieBloomFilter;
import ucsc.hadoop.mapreduce.apache.Grep;
import ucsc.hadoop.mapreduce.apache.WordCount;
import ucsc.hadoop.mapreduce.apache.WordCount2;
import ucsc.hadoop.mapreduce.avro.MovieAvroCount;
import ucsc.hadoop.mapreduce.movie.MovieCount;
import ucsc.hadoop.mapreduce.movie.MovieCountWithCombiner;
import ucsc.hadoop.mapreduce.movie.MovieCountWithCounter;
import ucsc.hadoop.mapreduce.movie.MovieCountWithLimit;
import ucsc.hadoop.mapreduce.sequence.MovieSequenceCount;
import ucsc.hadoop.mapreduce.weather.MaxTemperature;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class ExampleDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("bloomfilter", MovieBloomFilter.class, 
                "A map/reduce application that creates a bloom filter"); 
      pgd.addClass("moviecount", MovieCount.class, 
                "A map/reduce program that counts # of movies for each year");
      pgd.addClass("moviecountcombiner", MovieCountWithCombiner.class, 
              "A map/reduce program that counts # of movies for actor plays in. Using combiner");
      pgd.addClass("moviecountwithlimit", MovieCountWithLimit.class, 
              "A map/reduce program that counts # of movies for each year (example of using configuration)");	
      pgd.addClass("moviecountwithcounter", MovieCountWithCounter.class, 
              "A map/reduce program that counts # of movies for each year (example of using counter)");
      
      pgd.addClass("wordcount", WordCount.class, 
                   "A map/reduce program that counts the words in the input files.");
      pgd.addClass("wordcount2", WordCount2.class, 
              "A map/reduce program that counts the words in the input files with better delimiters and minimum count");
      pgd.addClass("maxtemperature", MaxTemperature.class, 
              "A map/reduce program that counts the words in the input files.");
      pgd.addClass("grep", Grep.class, 
              "A map/reduce program that counts the matches of a regex in the input.");
      
      // sequence
      pgd.addClass("seqmoviecount", MovieSequenceCount.class, 
              "A map/reduce example that counts # of movies from reading movie data in sequence format");
      
      // avro
      pgd.addClass("avromoviecount", MovieAvroCount.class, 
              "A map/reduce example that counts # of movies from reading movie data in Avro format");
      
      pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
