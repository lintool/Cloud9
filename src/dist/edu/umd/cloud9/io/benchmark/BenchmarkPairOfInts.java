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

import edu.umd.cloud9.io.PairOfInts;

/**
 * <p>
 * Benchmark for {@link PairOfInts}. Does the following:
 * </p>
 * 
 * <ul>
 * <li>Creating 2 million new objects. Each is populated with two random
 * numbers between 0 and 1000. All objects are added to an ArrayList.</li>
 * 
 * <li>Cloning all 2 million objects. All new objects are added to another
 * ArrayList.</li>
 * 
 * <li>Sorting the second ArrayList</li>
 * 
 * </ul>
 * 
 * <p>
 * See below for results comparing this benchmark to {@link BenchmarkTuple} and
 * {@link BenchmarkJSON} (on the equivalent task). All times measured in
 * seconds.
 * </p>
 * 
 * <table cellpadding="5" border="1">
 * <tr>
 * <td></td>
 * <td width="90"><b>PairOfInts</b></td>
 * <td width="90"><b>Tuple</b></td>
 * <td width="90"><b>JSON</b></td>
 * </tr>
 * 
 * <tr>
 * <td>Creating objects</td>
 * <td>0.609</td>
 * <td>3.319</td>
 * <td>4.472</td>
 * </tr>
 * 
 * <tr>
 * <td>Cloning objects</td>
 * <td>0.576</td>
 * <td>2.303</td>
 * <td>4.972</td>
 * </tr>
 * 
 * <tr>
 * <td>Sorting list</td>
 * <td>1.681</td>
 * <td>7.591</td>
 * <td>11.644</td>
 * </tr>
 * 
 * </table>
 * 
 * <p>
 * Times were arrived at by taking the average of 10 trials. Experiments were
 * conducted on Aug 6, 2008 on a 2.6GHz MacBook Pro running Windows XP and
 * Cygwin.
 * </p>
 * 
 */
public class BenchmarkPairOfInts {

	private BenchmarkPairOfInts() {
	}

	/**
	 * Runs this benchmark.
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		List<PairOfInts> listPairOfInts1 = new ArrayList<PairOfInts>();
		for (int i = 0; i < 2000000; i++) {
			listPairOfInts1.add(new PairOfInts(r.nextInt(1000), r.nextInt(1000)));
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m PairOfInts in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<PairOfInts> listPairOfInts2 = new ArrayList<PairOfInts>();
		for (PairOfInts p : listPairOfInts1) {
			listPairOfInts2.add(p.clone());
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m PairOfInts in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listPairOfInts2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m PairOfInts in " + duration + " seconds");
	}
}
