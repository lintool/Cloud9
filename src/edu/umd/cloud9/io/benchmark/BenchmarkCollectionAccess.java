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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Benchmark comparing HashMaps, ArrayLists, and raw arrays. Tests one trillion
 * accesses to a simple three element collection. Sample output:
 * </p>
 * 
 * <pre>
 * 1000000000 accesses to a 3-element collection:
 *  for HashMap: 17.609 seconds
 *  for ArrayList: 12.313 seconds
 *  for array: 1.219 seconds
 * </pre>
 */
public class BenchmarkCollectionAccess {

	private BenchmarkCollectionAccess() {
	}

	/**
	 * Runs this benchmark.
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		long startTime;
		double duration;

		int trials = 1000000000;

		System.out.println(trials + " accesses to a 3-element collection:");
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("field1", 1);
		map.put("field2", 2);
		map.put("field3", 2);

		startTime = System.currentTimeMillis();
		for (int i = 0; i < trials; i++) {
			int tmp = map.get("field1");
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" for HashMap: " + duration + " seconds");

		List<Integer> arrayList = new ArrayList<Integer>();
		arrayList.add(1);
		arrayList.add(2);
		arrayList.add(2);

		startTime = System.currentTimeMillis();
		for (int i = 0; i < trials; i++) {
			int tmp = arrayList.get(1);
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" for ArrayList: " + duration + " seconds");

		int[] array = { 1, 2, 3 };

		startTime = System.currentTimeMillis();
		for (int i = 0; i < trials; i++) {
			int tmp = array[1];
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" for array: " + duration + " seconds");
	}
}
