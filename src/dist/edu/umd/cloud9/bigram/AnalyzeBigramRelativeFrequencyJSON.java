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
import org.json.JSONException;

import edu.umd.cloud9.util.KeyValuePair;
import edu.umd.cloud9.util.SequenceFileUtils;

public class AnalyzeBigramRelativeFrequencyJSON {

	public static void main(String[] args) throws JSONException {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>> pairs = SequenceFileUtils
				.readDirectory(args[0]);

		List<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>> list1 = new ArrayList<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>>();
		List<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>> list2 = new ArrayList<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>>();

		for (KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> p : pairs) {
			BigramRelativeFrequencyJSON.MyTuple bigram = p.getKey();

			if (bigram.getStringUnchecked("Left").equals("light")) {
				list1.add(p);
			}

			if (bigram.getStringUnchecked("Left").equals("contain")) {
				list2.add(p);
			}
		}

		Collections.sort(list1,
				new Comparator<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>>() {
					public int compare(
							KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> e1,
							KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> e2) {
						if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
							return e1.getKey().compareTo(e2.getKey());
						}

						return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
					}
				});

		int i = 0;
		for (KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> p : list1) {
			BigramRelativeFrequencyJSON.MyTuple bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

		Collections.sort(list2,
				new Comparator<KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable>>() {
					public int compare(
							KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> e1,
							KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> e2) {
						if (((FloatWritable) e1.getValue()).compareTo(e2.getValue()) == 0) {
							return e1.getKey().compareTo(e2.getKey());
						}

						return ((FloatWritable) e2.getValue()).compareTo(e1.getValue());
					}
				});

		i = 0;
		for (KeyValuePair<BigramRelativeFrequencyJSON.MyTuple, FloatWritable> p : list2) {
			BigramRelativeFrequencyJSON.MyTuple bigram = p.getKey();
			System.out.println(bigram + "\t" + p.getValue());
			i++;

			if (i > 10)
				break;
		}

	}
}
