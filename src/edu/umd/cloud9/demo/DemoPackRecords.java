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

import edu.umd.cloud9.tuple.Schema;
import edu.umd.cloud9.tuple.Tuple;
import edu.umd.cloud9.util.LocalTupleRecordWriter;

/**
 * Demo that packs the sample collection into records using the tuple library.
 * The records are stored in a local SequenceFile; this file can then be
 * transfered over to HDFS to serve as the starting point for a MapReduce
 * operation.
 */
public class DemoPackRecords {
	private DemoPackRecords() {
	}

	// define the tuple schema for the input record
	private static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("text", String.class, "");
	}

	// instantiate a single tuple
	private static Tuple tuple = RECORD_SCHEMA.instantiate();

	public static void main(String[] args) throws IOException {
		String infile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc";
		String outfile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";

		// create LocalTupleRecordWriter to write tuples to a local SequenceFile
		LocalTupleRecordWriter writer = new LocalTupleRecordWriter(outfile);

		// read in raw text records, line separated
		BufferedReader data = new BufferedReader(new InputStreamReader(
				new FileInputStream(infile)));

		String line;
		while ((line = data.readLine()) != null) {
			// write the record
			tuple.set(0, line);
			writer.add(tuple);
		}

		data.close();
		writer.close();

		System.out.println("Wrote " + writer.getRecordCount() + " records.");
	}
}
