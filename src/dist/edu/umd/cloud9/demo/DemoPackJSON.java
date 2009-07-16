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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.json.JSONException;

import edu.umd.cloud9.io.JSONObjectWritable;

/**
 * <p>
 * Demo that packs the sample collection into a SequenceFile as JSON objects.
 * The key in each record is a {@link LongWritable} indicating the record count
 * (sequential numbering). The value in each record is a
 * {@link JSONObjectWritable}, where the raw text is stored under the field
 * name "text".
 * </p>
 * 
 * @see DemoPackTuples1
 * @see DemoPackTuples2
 */
public class DemoPackJSON {
	private static final Logger sLogger = Logger.getLogger(DemoPackJSON.class);

	private DemoPackJSON() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException, JSONException {
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
				LongWritable.class, JSONObjectWritable.class);

		// read in raw text records, line separated
		BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));

		// the key
		LongWritable l = new LongWritable();
		JSONObjectWritable json = new JSONObjectWritable();
		long cnt = 0;

		String line;
		while ((line = data.readLine()) != null) {
			// write the record
			json.clear();
			json.put("text", line);
			l.set(cnt);
			writer.append(l, json);

			cnt++;
		}

		data.close();
		writer.close();

		sLogger.info("Wrote " + cnt + " records.");
	}
}
