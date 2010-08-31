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

package edu.umd.cloud9.io.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONException;

import edu.umd.cloud9.io.JSONObjectWritable;

/**
 * Benchmark for {@link JSONObjectWritable}. See {@link BenchmarkPairOfInts} for
 * more details.
 */
public class BenchmarkJSON {

	private BenchmarkJSON() {
	}

	/**
	 * Runs this benchmark.
	 */
	private static class MyJSONTuple extends JSONObjectWritable implements
			WritableComparable<MyJSONTuple> {
		public int compareTo(MyJSONTuple that) {
			try {
				int thisLeft = this.getIntUnchecked("left");
				int thisRight = this.getIntUnchecked("right");

				int thatLeft = that.getIntUnchecked("left");
				int thatRight = that.getIntUnchecked("right");

				if (thisLeft == thatLeft) {
					if (thisRight < thatRight)
						return -1;

					if (thisRight > thatRight)
						return 1;

					return 0;
				}

				if (thisLeft < thatLeft)
					return -1;

				if (thisLeft > thatLeft)
					return 1;

				return 0;
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error comparing JSON objects!");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();
		List<MyJSONTuple> listJSONObjects1 = new ArrayList<MyJSONTuple>();
		for (int i = 0; i < 2000000; i++) {
			MyJSONTuple j = new MyJSONTuple();
			j.put("left", r.nextInt(1000));
			j.put("right", r.nextInt(1000));
			listJSONObjects1.add(j);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m JSON Objects in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<MyJSONTuple> listJSONObjects2 = new ArrayList<MyJSONTuple>();
		for (MyJSONTuple t : listJSONObjects1) {
			MyJSONTuple n = new MyJSONTuple();
			n.put("left", t.getInt("left"));
			n.put("right", t.getInt("right"));
			listJSONObjects2.add(n);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m JSON Objects in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listJSONObjects2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m JSON Objects in " + duration + " seconds");
	}
}
