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

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import edu.umd.cloud9.util.MapInt;

public class MapKeyToIntWritableTest {

	@Test
	public void testBasic() throws IOException {
		MapKeyToIntWritable<Text> m = new MapKeyToIntWritable<Text>();

		m.put(new Text("hi"), 5);
		m.put(new Text("there"), 22);

		Text key;
		int value;

		assertEquals(m.size(), 2);

		key = new Text("hi");
		value = m.get(key);
		assertEquals(value, 5);

		value = m.remove(key);
		assertEquals(m.size(), 1);

		key = new Text("there");
		value = m.get(key);
		assertEquals(value, 22);
	}

	@Test
	public void testSerialize1() throws IOException {
		MapKeyToIntWritable<Text> m1 = new MapKeyToIntWritable<Text>();

		m1.put(new Text("hi"), 5);
		m1.put(new Text("there"), 22);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		MapKeyToIntWritable<Text> m2 = MapKeyToIntWritable.<Text> create(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		Text key;
		int value;

		assertEquals(m2.size(), 2);

		key = new Text("hi");
		value = m2.get(key);
		assertEquals(value, 5);

		value = m2.remove(key);
		assertEquals(m2.size(), 1);

		key = new Text("there");
		value = m2.get(key);
		assertEquals(value, 22);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		MapKeyToIntWritable<WritableComparable> m1 = new MapKeyToIntWritable<WritableComparable>();

		m1.put(new Text("hi"), 4);
		m1.put(new IntWritable(0), 76);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		MapKeyToIntWritable<Text> m2 = MapKeyToIntWritable.<Text> create(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));
		
		m2.size();
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		MapKeyToIntWritable<WritableComparable> m1 = new MapKeyToIntWritable<WritableComparable>();

		assertTrue(m1.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		MapKeyToIntWritable<Text> m2 = MapKeyToIntWritable.<Text> create(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(MapKeyToIntWritableTest.class);
	}

}
