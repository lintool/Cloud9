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
import java.util.Iterator;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class VectorIntTest {

	@Test
	public void testBasic() throws IOException {
		VectorInt<Text> v = new VectorInt<Text>();

		v.set(new Text("hi"), 5);
		v.set(new Text("there"), 22);

		Text key;
		int value;

		assertEquals(v.size(), 2);

		key = new Text("hi");
		value = v.get(key);
		assertEquals(value, 5);

		value = v.remove(key);
		assertEquals(v.size(), 1);

		key = new Text("there");
		value = v.get(key);
		assertEquals(value, 22);
	}

	@Test
	public void testSerialize1() throws IOException {
		VectorInt<Text> v1 = new VectorInt<Text>();

		v1.set(new Text("hi"), 5);
		v1.set(new Text("there"), 22);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorInt<Text> v2 = new VectorInt<Text>();

		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		Text key;
		int value;

		assertEquals(v2.size(), 2);

		key = new Text("hi");
		value = v2.get(key);
		assertEquals(value, 5);

		value = v2.remove(key);
		assertEquals(v2.size(), 1);

		key = new Text("there");
		value = v2.get(key);
		assertEquals(value, 22);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		VectorInt<WritableComparable> v1 = new VectorInt<WritableComparable>();

		v1.set(new Text("hi"), 4);
		v1.set(new IntWritable(0), 76);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorInt<WritableComparable> v2 = new VectorInt<WritableComparable>();

		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

	}

	@Test
	public void testSerializeEmpty() throws IOException {
		VectorInt<WritableComparable> v1 = new VectorInt<WritableComparable>();

		assertTrue(v1.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorInt<WritableComparable> v2 = new VectorInt<WritableComparable>();
		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
		assertTrue(v2.size() == 0);
	}

	@Test
	public void testPlus() throws IOException {
		VectorInt<Text> v1 = new VectorInt<Text>();

		v1.set(new Text("hi"), 5);
		v1.set(new Text("there"), 22);

		VectorInt<Text> v2 = new VectorInt<Text>();

		v2.set(new Text("hi"), 4);
		v2.set(new Text("test"), 5);

		v1.plus(v2);

		assertEquals(v1.size(), 3);
		assertTrue(v1.get(new Text("hi")) == 9);
		assertTrue(v1.get(new Text("there")) == 22);
		assertTrue(v1.get(new Text("test")) == 5);
	}

	@Test
	public void testDot() throws IOException {
		VectorInt<Text> v1 = new VectorInt<Text>();

		v1.set(new Text("hi"), 5);
		v1.set(new Text("there"), 2);
		v1.set(new Text("empty"), 3);

		VectorInt<Text> v2 = new VectorInt<Text>();

		v2.set(new Text("hi"), 4);
		v2.set(new Text("there"), 4);
		v2.set(new Text("test"), 5);

		int s = v1.dot(v2);

		assertEquals(s, 28);
	}

	@Test
	public void testSortedEntries1() {

		VectorInt<Text> v = new VectorInt<Text>();

		v.set(new Text("a"), 5);
		v.set(new Text("b"), 2);
		v.set(new Text("c"), 3);
		v.set(new Text("d"), 3);
		v.set(new Text("e"), 1);

		Iterator<Map.Entry<Text, Integer>> iter = v.getSortedEntries().iterator();

		Map.Entry<Text, Integer> m = iter.next();
		assertEquals(new Text("a"), m.getKey());
		assertEquals(5, (int) m.getValue());

		m = iter.next();
		assertEquals(new Text("c"), m.getKey());
		assertEquals(3, (int) m.getValue());

		m = iter.next();
		assertEquals(new Text("d"), m.getKey());
		assertEquals(3, (int) m.getValue());

		m = iter.next();
		assertEquals(new Text("b"), m.getKey());
		assertEquals(2, (int) m.getValue());

		m = iter.next();
		assertEquals(new Text("e"), m.getKey());
		assertEquals(1, (int) m.getValue());

		assertEquals(false, iter.hasNext());		
	}

	@Test
	public void testSortedEntries2() {

		VectorInt<Text> v = new VectorInt<Text>();

		v.set(new Text("a"), 5);
		v.set(new Text("b"), 2);
		v.set(new Text("c"), 3);
		v.set(new Text("d"), 3);
		v.set(new Text("e"), 1);

		Iterator<Map.Entry<Text, Integer>> iter = v.getSortedEntries(2).iterator();

		Map.Entry<Text, Integer> m = iter.next();
		assertEquals(new Text("a"), m.getKey());
		assertEquals(5, (int) m.getValue());

		m = iter.next();
		assertEquals(new Text("c"), m.getKey());
		assertEquals(3, (int) m.getValue());

		assertEquals(false, iter.hasNext());		
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(VectorIntTest.class);
	}

}
