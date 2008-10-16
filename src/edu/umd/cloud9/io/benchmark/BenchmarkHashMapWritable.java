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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import edu.umd.cloud9.io.HashMapWritable;

/**
 * <p>
 * Benchmark comparing <code>HashMapWritable</code> to Hadoop's native
 * <code>MapWritable</code>. Sample output:
 * </p>
 * 
 * <pre>
 * Generating and serializing 100000 random HashMapWritables: 4.672 seconds
 * Generating and serializing 100000 random MapWritables: 5.546 seconds
 * Average size of each HashMapWritable: 664.77783
 * Average size of each MapWritable: 747.4403
 * Deserializing 100000 random MapWritables: 3.954 seconds
 * Deserializing 100000 random MapWritables: 4.968 seconds
 * </pre>
 * 
 */
public class BenchmarkHashMapWritable {

	private BenchmarkHashMapWritable() {
	}

	/**
	 * Runs this benchmark.
	 */
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int numTrials = 100000;

		Random rand = new Random();

		ByteArrayOutputStream[] storageHashMapWritable = new ByteArrayOutputStream[numTrials];
		for (int i = 0; i < numTrials; i++) {
			HashMapWritable<IntWritable, IntWritable> map = new HashMapWritable<IntWritable, IntWritable>();

			int size = rand.nextInt(50) + 50;

			for (int j = 0; j < size; j++) {
				map.put(new IntWritable(rand.nextInt(10000)), new IntWritable(rand.nextInt(10)));
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			map.write(dataOut);
			storageHashMapWritable[i] = bytesOut;
		}

		System.out.println("Generating and serializing " + numTrials + " random HashMapWritables: "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		startTime = System.currentTimeMillis();

		ByteArrayOutputStream[] storageMapWritable = new ByteArrayOutputStream[numTrials];
		for (int i = 0; i < numTrials; i++) {
			MapWritable map = new MapWritable();

			int size = rand.nextInt(50) + 50;

			for (int j = 0; j < size; j++) {
				map.put(new IntWritable(rand.nextInt(10000)), new IntWritable(rand.nextInt(10)));
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			map.write(dataOut);
			storageMapWritable[i] = bytesOut;
		}

		System.out.println("Generating and serializing " + numTrials + " random MapWritables: "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		float cntA = 0.0f;
		float cntB = 0.0f;
		for (int i = 0; i < numTrials; i++) {
			cntA += storageHashMapWritable[i].size();
			cntB += storageMapWritable[i].size();
		}

		System.out.println("Average size of each HashMapWritable: " + cntA / numTrials);
		System.out.println("Average size of each MapWritable: " + cntB / numTrials);

		startTime = System.currentTimeMillis();

		for (int i = 0; i < numTrials; i++) {
			HashMapWritable<IntWritable, IntWritable> map = new HashMapWritable<IntWritable, IntWritable>();

			map.readFields(new DataInputStream(new ByteArrayInputStream(storageHashMapWritable[i]
					.toByteArray())));
		}

		System.out.println("Deserializing " + numTrials + " random MapWritables: "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		startTime = System.currentTimeMillis();

		for (int i = 0; i < numTrials; i++) {
			MapWritable map = new MapWritable();

			map.readFields(new DataInputStream(new ByteArrayInputStream(storageMapWritable[i]
					.toByteArray())));
		}

		System.out.println("Deserializing " + numTrials + " random MapWritables: "
				+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	}
}
