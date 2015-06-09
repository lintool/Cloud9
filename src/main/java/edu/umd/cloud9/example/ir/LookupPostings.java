/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.example.ir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Int2IntFrequencyDistribution;
import tl.lin.data.fd.Int2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class LookupPostings {
  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(COLLECTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LookupPostings.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    if (collectionPath.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

    lookupTerm("starcross'd", reader, collectionPath, fs);
    lookupTerm("gold", reader, collectionPath, fs);
    lookupTerm("silver", reader, collectionPath, fs);
    lookupTerm("bronze", reader, collectionPath, fs);

    reader.close();
  }

  public static void lookupTerm(String term, MapFile.Reader reader, String collectionPath,
      FileSystem fs) throws IOException {
    FSDataInputStream collection = fs.open(new Path(collectionPath));

    Text key = new Text();
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

    key.set(term);
    Writable w = reader.get(key, value);

    if (w == null) {
      System.out.println("\nThe term '" + term + "' does not appear in the collection");
      return;
    }

    ArrayListWritable<PairOfInts> postings = value.getRightElement();
    System.out.println("\nComplete postings list for '" + term + "':");
    System.out.println("df = " + value.getLeftElement());

    Int2IntFrequencyDistribution hist = new Int2IntFrequencyDistributionEntry();
    for (PairOfInts pair : postings) {
      hist.increment(pair.getRightElement());
      System.out.print(pair);
      collection.seek(pair.getLeftElement());
      BufferedReader r = new BufferedReader(new InputStreamReader(collection));

      String d = r.readLine();
      d = d.length() > 80 ? d.substring(0, 80) + "..." : d;

      System.out.println(": " + d);
    }

    System.out.println("\nHistogram of tf values for '" + term + "'");
    for (PairOfInts pair : hist) {
      System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
    }

    collection.close();
  }
}
