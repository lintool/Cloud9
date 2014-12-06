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

import edu.umd.cloud9.io.JsonWritable;

/**
 * Demo that packs the sample collection into a {@code SequenceFile} of {@link JsonWritable}s.
 * The key in each record is a {@link LongWritable} indicating the record count (sequential
 * numbering). The value in each record is a {@link JsonWritable}, where the raw text is
 * stored under the field name "text". Designed to work with {@link DemoWordCountJson}.
 *
 * @author Jimmy Lin
 */
public class DemoPackJson {
  private static final Logger LOG = Logger.getLogger(DemoPackJson.class);

  private DemoPackJson() {}

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
        SequenceFile.Writer.valueClass(JsonWritable.class));

    // Read in raw text records, line separated.
    BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));

    LongWritable key = new LongWritable();
    JsonWritable json = new JsonWritable();
    long cnt = 0;

    String line;
    while ((line = data.readLine()) != null) {
      json.getJsonObject().addProperty("text", line);
      key.set(cnt);
      writer.append(key, json);

      cnt++;
    }

    data.close();
    writer.close();

    LOG.info("Wrote " + cnt + " records.");
  }
}
