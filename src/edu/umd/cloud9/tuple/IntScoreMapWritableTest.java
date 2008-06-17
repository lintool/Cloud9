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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class IntScoreMapWritableTest {

	@Test
	public void testBasic() throws IOException {
		IntScoreMapWritable<Text> map = new IntScoreMapWritable<Text>();

		map.put(new Text("hi"), 5);
		map.put(new Text("there"), 22);

		Text key;
		int value;

		assertEquals(map.size(), 2);

		key = new Text("hi");
		value = map.get(key);
		assertEquals(value, 5);

		value = map.remove(key);
		assertEquals(map.size(), 1);

		key = new Text("there");
		value = map.get(key);
		assertEquals(value, 22);
	}

	@Test
	public void testSerialize1() throws IOException {
		IntScoreMapWritable<Text> origMap = new IntScoreMapWritable<Text>();

		origMap.put(new Text("hi"), 5);
		origMap.put(new Text("there"), 22);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		IntScoreMapWritable<Text> map = new IntScoreMapWritable<Text>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		Text key;
		int value;

		assertEquals(map.size(), 2);

		key = new Text("hi");
		value = map.get(key);
		assertEquals(value, 5);

		value = map.remove(key);
		assertEquals(map.size(), 1);

		key = new Text("there");
		value = map.get(key);
		assertEquals(value, 22);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		IntScoreMapWritable<WritableComparable> origMap = new IntScoreMapWritable<WritableComparable>();

		origMap.put(new Text("hi"), 4);
		origMap.put(new IntWritable(0), 76);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		IntScoreMapWritable<WritableComparable> map = new IntScoreMapWritable<WritableComparable>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

	}

	@Test
	public void testSerializeEmpty() throws IOException {
		IntScoreMapWritable<WritableComparable> map = new IntScoreMapWritable<WritableComparable>();

		assertTrue(map.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		map.write(dataOut);

		IntScoreMapWritable<WritableComparable> newList = new IntScoreMapWritable<WritableComparable>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));
		assertTrue(newList.size() == 0);
	}

	@Test
	public void testMerge() throws IOException {
		IntScoreMapWritable<Text> map1 = new IntScoreMapWritable<Text>();

		map1.put(new Text("hi"), 5);
		map1.put(new Text("there"), 22);

		IntScoreMapWritable<Text> map2 = new IntScoreMapWritable<Text>();

		map2.put(new Text("hi"), 4);
		map2.put(new Text("test"), 5);

		map1.merge(map2);

		assertEquals(map1.size(), 3);
		assertTrue(map1.get(new Text("hi")) == 9);
		assertTrue(map1.get(new Text("there")) == 22);
		assertTrue(map1.get(new Text("test")) == 5);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(IntScoreMapWritableTest.class);
	}

}
