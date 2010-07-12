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
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;

public class BooleanRetrieval {

	MapFile.Reader mIndex;
	FSDataInputStream mCollection;
	Stack<Set<Integer>> mStack;

	public BooleanRetrieval(String indexPath, String collectionPath, Configuration config)
			throws IOException {
		FileSystem fs = FileSystem.get(config);
		mIndex = new MapFile.Reader(fs, indexPath + "/part-00000", config);

		mCollection = fs.open(new Path(collectionPath));
		mStack = new Stack<Set<Integer>>();
	}

	public void runQuery(String q) throws IOException {

		String[] terms = q.split("\\s+");

		for (String t : terms) {

			if (t.equals("AND")) {
				performAND();
			} else if (t.equals("OR")) {
				performOR();
			} else {
				pushTerm(t);
			}

		}

		Set<Integer> set = mStack.pop();

		for (Integer i : set) {
			String line = fetchLine(i);
			System.out.println(i + "\t" + line);
		}

	}

	public void pushTerm(String term) throws IOException {
		mStack.push(fetchDocumentSet(term));
	}

	public void performAND() {
		Set<Integer> s1 = mStack.pop();
		Set<Integer> s2 = mStack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			if (s2.contains(n)) {
				sn.add(n);
			}
		}

		mStack.push(sn);
	}

	public void performOR() {
		Set<Integer> s1 = mStack.pop();
		Set<Integer> s2 = mStack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			sn.add(n);
		}

		for (int n : s2) {
			sn.add(n);
		}

		mStack.push(sn);
	}

	public Set<Integer> fetchDocumentSet(String term) throws IOException {

		Set<Integer> set = new TreeSet<Integer>();

		for (PairOfInts pair : fetchPostings(term)) {
			set.add(pair.getLeftElement());
		}

		return set;
	}

	public ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
		Text key = new Text();
		ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

		key.set(term);

		mIndex.get(key, postings);

		return postings;
	}

	public String fetchLine(long offset) throws IOException {
		mCollection.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(mCollection));

		return reader.readLine();
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		String indexPath = args[0];
		String collectionPath = args[1];

		Configuration config = new Configuration();

		BooleanRetrieval s = new BooleanRetrieval(indexPath, collectionPath, config);

		String[] queries = { "outrageous fortune AND", "white rose AND", "means deceit AND",
				"white red OR rose AND pluck AND",
				"unhappy outrageous OR good your AND OR fortune AND" };

		for (String q : queries) {
			System.out.println("Query: " + q);

			s.runQuery(q);

			System.out.println("\n");
		}

	}
}
