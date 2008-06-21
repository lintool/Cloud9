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

package edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * HashMap that is serializable in Hadoop framework. There are a number of key
 * differences between this class and Hadoop's {@link MapWritable}:
 * </p>
 * 
 * <ul>
 * 
 * <li><code>MapWritable</code> is more flexible in that it supports
 * heterogeneous elements. In this class, all keys must be of the same type and
 * all values must be of the same type. This assumption allows a simpler
 * serialization protocol and thus is more efficient. Run <code>main</code> in
 * this class for a simple efficiency test.</li>
 * 
 * <li>This class is generic, whereas <code>MapWritable</code> isn't.</li>
 * 
 * </ul>
 * 
 * @param <K>
 *            type of the key
 * @param <V>
 *            type of the value
 */
public class HashMapWritable<K extends Writable, V extends Writable> extends HashMap<K, V>
		implements Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a HashMapWritable object.
	 */
	public HashMapWritable() {
		super();
	}

	/**
	 * Creates a HashMapWritable object from a regular HashMap.
	 */
	public HashMapWritable(HashMap<K, V> map) {
		super(map);
	}

	/**
	 * Deserializes the array.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {

		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0)
			return;

		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();

		K objK;
		V objV;
		try {
			Class keyClass = Class.forName(keyClassName);
			Class valueClass = Class.forName(valueClassName);
			for (int i = 0; i < numEntries; i++) {
				objK = (K) keyClass.newInstance();
				objK.readFields(in);
				objV = (V) valueClass.newInstance();
				objV.readFields(in);
				put(objK, objV);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Serializes this array.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the map
		out.writeInt(size());
		if (size() == 0)
			return;

		// Write out the class names for keys and values
		// assuming that data is homogeneuos (i.e., all entries have same types)
		Set<Map.Entry<K, V>> entries = entrySet();
		Map.Entry<K, V> first = entries.iterator().next();
		K objK = first.getKey();
		V objV = first.getValue();
		out.writeUTF(objK.getClass().getCanonicalName());
		out.writeUTF(objV.getClass().getCanonicalName());

		// Then write out each key/value pair
		for (Map.Entry<K, V> e : entrySet()) {
			e.getKey().write(out);
			e.getValue().write(out);
		}
	}

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
