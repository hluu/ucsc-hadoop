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

import ucsc.hadoop.mapreduce.apache.Grep;
import ucsc.hadoop.mapreduce.apache.WordCount;
import ucsc.hadoop.mapreduce.apache.WordCount2;
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
      pgd.addClass("wordcount", WordCount.class, 
                   "A map/reduce program that counts the words in the input files.");
      pgd.addClass("wordcount2", WordCount2.class, 
              "A map/reduce program that counts the words in the input files with better delimiters and minimum count");
      pgd.addClass("maxtemperature", MaxTemperature.class, 
              "A map/reduce program that counts the words in the input files.");
      pgd.addClass("grep", Grep.class, 
              "A map/reduce program that counts the matches of a regex in the input.");
      
    /*  pgd.addClass("aggregatewordcount", AggregateWordCount.class, 
                   "An Aggregate based map/reduce program that counts the words in the input files.");
      pgd.addClass("aggregatewordhist", AggregateWordHistogram.class, 
                   "An Aggregate based map/reduce program that computes the histogram of the words in the input files.");
      pgd.addClass("grep", Grep.class, 
                   "A map/reduce program that counts the matches of a regex in the input.");
      pgd.addClass("randomwriter", RandomWriter.class, 
                   "A map/reduce program that writes 10GB of random data per node.");
      pgd.addClass("randomtextwriter", RandomTextWriter.class, 
      "A map/reduce program that writes 10GB of random textual data per node.");
      pgd.addClass("sort", Sort.class, "A map/reduce program that sorts the data written by the random writer.");

      pgd.addClass("pi", QuasiMonteCarlo.class, QuasiMonteCarlo.DESCRIPTION);
      pgd.addClass("bbp", BaileyBorweinPlouffe.class, BaileyBorweinPlouffe.DESCRIPTION);
      pgd.addClass("distbbp", DistBbp.class, DistBbp.DESCRIPTION);

      pgd.addClass("pentomino", DistributedPentomino.class,
      "A map/reduce tile laying program to find solutions to pentomino problems.");
      pgd.addClass("secondarysort", SecondarySort.class,
                   "An example defining a secondary sort to the reduce.");
      pgd.addClass("sudoku", Sudoku.class, "A sudoku solver.");
      pgd.addClass("join", Join.class, "A job that effects a join over sorted, equally partitioned datasets");
      pgd.addClass("multifilewc", MultiFileWordCount.class, "A job that counts words from several files.");
      pgd.addClass("dbcount", DBCountPageView.class, "An example job that count the pageview counts from a database.");
      pgd.addClass("teragen", TeraGen.class, "Generate data for the terasort");
      pgd.addClass("terasort", TeraSort.class, "Run the terasort");
      pgd.addClass("teravalidate", TeraValidate.class, "Checking results of terasort");*/
      pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
