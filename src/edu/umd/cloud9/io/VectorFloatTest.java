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

package edu.umd.cloud9.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class VectorFloatTest {

	@Test
	public void testBasic() throws IOException {
		VectorFloat<Text> map = new VectorFloat<Text>();

		map.set(new Text("hi"), 5.0f);
		map.set(new Text("there"), 22.0f);

		Text key;
		float value;

		assertEquals(map.size(), 2);

		key = new Text("hi");
		value = map.get(key);
		assertTrue(value == 5.0f);

		value = map.remove(key);
		assertEquals(map.size(), 1);

		key = new Text("there");
		value = map.get(key);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerialize1() throws IOException {
		VectorFloat<Text> origMap = new VectorFloat<Text>();

		origMap.set(new Text("hi"), 5.0f);
		origMap.set(new Text("there"), 22.0f);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		VectorFloat<Text> map = new VectorFloat<Text>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		Text key;
		float value;

		assertEquals(map.size(), 2);

		key = new Text("hi");
		value = map.get(key);
		assertTrue(value == 5.0f);

		value = map.remove(key);
		assertEquals(map.size(), 1);

		key = new Text("there");
		value = map.get(key);
		assertTrue(value == 22.0f);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		VectorFloat<WritableComparable> origMap = new VectorFloat<WritableComparable>();

		origMap.set(new Text("hi"), 4.0f);
		origMap.set(new IntWritable(0), 76.0f);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		VectorFloat<WritableComparable> map = new VectorFloat<WritableComparable>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

	}

	@Test
	public void testSerializeEmpty() throws IOException {
		VectorFloat<WritableComparable> map = new VectorFloat<WritableComparable>();

		assertTrue(map.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		map.write(dataOut);

		VectorFloat<WritableComparable> newList = new VectorFloat<WritableComparable>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
		assertTrue(newList.size() == 0);
	}

	@Test
	public void testMerge() throws IOException {
		VectorFloat<Text> map1 = new VectorFloat<Text>();

		map1.set(new Text("hi"), 5.0f);
		map1.set(new Text("there"), 22.0f);

		VectorFloat<Text> map2 = new VectorFloat<Text>();

		map2.set(new Text("hi"), 4.0f);
		map2.set(new Text("test"), 5.0f);

		map1.plus(map2);

		assertEquals(map1.size(), 3);
		assertTrue(map1.get(new Text("hi")) == 9);
		assertTrue(map1.get(new Text("there")) == 22);
		assertTrue(map1.get(new Text("test")) == 5);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(VectorFloatTest.class);
	}

}
