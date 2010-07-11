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

package edu.umd.cloud9.example.bigram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.util.KeyValuePair;
import edu.umd.cloud9.util.SequenceFileUtils;

public class AnalyzeBigramRelativeFrequency {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<KeyValuePair<PairOfStrings, FloatWritable>> pairs = SequenceFileUtils
				.readDirectory(args[0]);

		List<KeyValuePair<PairOfStrings, FloatWritable>> list1 = new ArrayList<KeyValuePair<PairOfStrings, FloatWritable>>();
		List<KeyValuePair<PairOfStrings, FloatWritable>> list2 = new ArrayList<KeyValuePair<PairOfStrings, FloatWritable>>();

		for (KeyValuePair<PairOfStrings, FloatWritable> p : pairs) {
			PairOfStrings bigram = p.getKey();

			if (bigram.getLeftElement().equals("light")) {
				list1.add(p);
			}

			if (bigram.getLeftElement().equals("contain")) {
				list2.add(p);
			}
		}

		Collections.sort(list1, new Comparator<KeyValuePair<PairOfStrings, FloatWritable>>() {
			public int compare(KeyValuePair<PairOfStrings, FloatWritable> e1,
					KeyValuePair<PairOfStrings, FloatWritable> e2) {
				if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
					return e1.getKey().compareTo(e2.getKey());
				}

				return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
			}
		});

		int i = 0;
		for (KeyValuePair<PairOfStrings, FloatWritable> p : list1) {
			PairOfStrings bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

		Collections.sort(list2, new Comparator<KeyValuePair<PairOfStrings, FloatWritable>>() {
			public int compare(KeyValuePair<PairOfStrings, FloatWritable> e1,
					KeyValuePair<PairOfStrings, FloatWritable> e2) {
				if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
					return e1.getKey().compareTo(e2.getKey());
				}

				return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
			}
		});

		i = 0;
		for (KeyValuePair<PairOfStrings, FloatWritable> p : list2) {
			PairOfStrings bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

	}
}
