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

package edu.umd.cloud9.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;

/**
 * <p>
 * Demo that packs the sample collection into a SequenceFile as {@link Tuple}
 * objects with complex internal structure. The records are stored in a local
 * SequenceFile; this file can then be transfered over to HDFS to serve as the
 * the input to {@link DemoWordCountTuple2}.
 * </p>
 * 
 * <p>
 * Each value in the SequenceFile is a tuple with two fields:
 * </p>
 * 
 * <ul>
 * 
 * <li>the first field of the tuple is an Integer with the field name "length";
 * its value is the length of the record in number of characters.</li>
 * 
 * <li>the second field of the tuple is a ListWritable<Text> with the field
 * name "tokens"; its value is a list of tokens that comprise the text of the
 * record.</li>
 * 
 * </ul>
 * 
 * @see DemoPackTuples1
 * @see DemoPackJSON
 */
public class DemoPackTuples2 {
	private static final Logger sLogger = Logger.getLogger(DemoPackTuples2.class);

	private DemoPackTuples2() {
	}

	// define the tuple schema for the input record
	private static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("length", Integer.class);
		RECORD_SCHEMA.addField("tokens", ArrayListWritable.class, "");
	}

	// instantiate a single tuple
	private static Tuple tuple = RECORD_SCHEMA.instantiate();

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

		sLogger.info("input: " + infile);
		sLogger.info("output: " + outfile);

		Configuration conf = new JobConf();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(outfile),
				LongWritable.class, Tuple.class);

		// read in raw text records, line separated
		BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));

		LongWritable l = new LongWritable();
		long cnt = 0;

		String line;
		while ((line = data.readLine()) != null) {
			ArrayListWritable<Text> tokens = new ArrayListWritable<Text>();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				tokens.add(new Text(itr.nextToken()));
			}

			// write the record
			tuple.set("length", line.length());
			tuple.set("tokens", tokens);
			l.set(cnt);
			writer.append(l, tuple);

			cnt++;
		}

		data.close();
		writer.close();

		sLogger.info("Wrote " + cnt + " records.");
	}
}