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

package edu.umd.cloud9.bigram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;

import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.util.KeyValuePair;
import edu.umd.cloud9.util.SequenceFileUtils;

public class AnalyzeBigramRelativeFrequencyTuple {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<KeyValuePair<Tuple, FloatWritable>> pairs = SequenceFileUtils
				.readDirectory(args[0]);

		List<KeyValuePair<Tuple, FloatWritable>> list1 = new ArrayList<KeyValuePair<Tuple, FloatWritable>>();
		List<KeyValuePair<Tuple, FloatWritable>> list2 = new ArrayList<KeyValuePair<Tuple, FloatWritable>>();

		for (KeyValuePair<Tuple, FloatWritable> p : pairs) {
			Tuple bigram = p.getKey();

			if (bigram.get("Left").equals("light")) {
				list1.add(p);
			}

			if (bigram.get("Left").equals("contain")) {
				list2.add(p);
			}
		}

		Collections.sort(list1, new Comparator<KeyValuePair<Tuple, FloatWritable>>() {
			public int compare(KeyValuePair<Tuple, FloatWritable> e1,
					KeyValuePair<Tuple, FloatWritable> e2) {
				if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
					return e1.getKey().compareTo(e2.getKey());
				}

				return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
			}
		});

		int i = 0;
		for (KeyValuePair<Tuple, FloatWritable> p : list1) {
			Tuple bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

		Collections.sort(list2, new Comparator<KeyValuePair<Tuple, FloatWritable>>() {
			public int compare(KeyValuePair<Tuple, FloatWritable> e1,
					KeyValuePair<Tuple, FloatWritable> e2) {
				if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
					return e1.getKey().compareTo(e2.getKey());
				}

				return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
			}
		});

		i = 0;
		for (KeyValuePair<Tuple, FloatWritable> p : list2) {
			Tuple bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

	}
}
