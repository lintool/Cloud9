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

package edu.umd.cloud9.example.ir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfWritables;
import edu.umd.cloud9.util.EntryFrequencyDistributionOfInts;
import edu.umd.cloud9.util.FrequencyDistributionOfInts;

public class LookupPostings {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		String indexPath = args[0];
		String collectionPath = args[1];

		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		MapFile.Reader reader = new MapFile.Reader(fs, indexPath + "/part-00000", config);

		FSDataInputStream collection = fs.open(new Path(collectionPath));
		BufferedReader d = new BufferedReader(new InputStreamReader(collection));

		Text key = new Text();
		PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

		System.out.println("Looking up postings for the term \"starcross'd\"");
		key.set("starcross'd");

		reader.get(key, value);

		ArrayListWritable<PairOfInts> postings = value.getRightElement();
		for (PairOfInts pair : postings) {
			System.out.println(pair);
			collection.seek(pair.getLeftElement());
			System.out.println(d.readLine());
		}

		key.set("gold");
		reader.get(key, value);
		System.out.println("Complete postings list for 'gold': " + value);

		FrequencyDistributionOfInts goldHist = new EntryFrequencyDistributionOfInts();
		postings = value.getRightElement();
		for (PairOfInts pair : postings) {
			goldHist.increment(pair.getRightElement());
		}

		System.out.println("histogram of tf values for gold");
		for ( PairOfInts pair : goldHist ) {
			System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
		}

		key.set("silver");
		reader.get(key, value);
		System.out.println("Complete postings list for 'silver': " + value);

		FrequencyDistributionOfInts silverHist = new EntryFrequencyDistributionOfInts();
		postings = value.getRightElement();
		for (PairOfInts pair : postings) {
			silverHist.increment(pair.getRightElement());
		}

		System.out.println("histogram of tf values for silver");
		for ( PairOfInts pair : silverHist ) {
			System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
		}

		key.set("bronze");
		Writable w = reader.get(key, value);

		if (w == null) {
			System.out.println("the term bronze does not appear in the collection");
		}

		collection.close();
		reader.close();
	}
}
