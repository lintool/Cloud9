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

import java.io.IOException;

import edu.umd.cloud9.tuple.Tuple;
import edu.umd.cloud9.util.LocalTupleRecordReader;

/**
 * Demo that illustrates how to read records from a local SequenceFile.
 */
public class DemoReadPackedRecords {
	private DemoReadPackedRecords() {
	}

	private static final Tuple tuple = new Tuple();

	public static void main(String[] args) throws IOException {
		String file = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";

		// open local records file
		LocalTupleRecordReader reader = new LocalTupleRecordReader(file);
		// iterate over all tuples
		while (reader.read(tuple)) {
			// print out each tuple
			System.out.println(tuple.get(0));
		}
		reader.close();

		System.out.println("Read " + reader.getRecordCount() + " records.");
	}

}
