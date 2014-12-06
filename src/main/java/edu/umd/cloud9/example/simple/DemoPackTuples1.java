/*
 * Cloud9: A MapReduce Library for Hadoop
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

package edu.umd.cloud9.example.simple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.TupleFactory;

/**
 * Demo that packs the sample collection into a {@code SequenceFile} of Pig Tuples. The key in each
 * record is a {@link LongWritable} indicating the record count (sequential numbering). The value in
 * each record is a Pig Tuple with a single field containing the text of the line. Designed to work
 * with {@link DemoWordCountTuple1}.
 *
 * @author Jimmy Lin
 */
public class DemoPackTuples1 {
  private static final Logger LOG = Logger.getLogger(DemoPackTuples1.class);

  private static final LongWritable LONG = new LongWritable();
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private DemoPackTuples1() {}

  /**
   * Runs the demo.
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("usage: [input] [output]");
      System.exit(-1);
    }

    String infile = args[0];
    String outfile = args[1];

    LOG.info("input: " + infile);
    LOG.info("output: " + outfile);

    Configuration conf = new Configuration();
    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(new Path(outfile)),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(BinSedesTuple.class));

    BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));

    long cnt = 0;
    String line;
    while ((line = data.readLine()) != null) {
      LONG.set(cnt);
      writer.append(LONG, TUPLE_FACTORY.newTuple(line));

      cnt++;
    }

    data.close();
    writer.close();

    LOG.info("Wrote " + cnt + " records.");
  }
}
