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
import java.util.ArrayList;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class ArrayListWritableTest {

	@Test
	public void testBasic() throws IOException {
		ArrayListWritable<Text> list = new ArrayListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));

		assertEquals(list.get(0).toString(), "hi");
		assertEquals(list.get(1).toString(), "there");
	}

	@Test
	public void testSerialize1() throws IOException {
		//ArrayListWritable<Text> list = new ArrayListWritable<Text>();
		ArrayListWritable<Writable> list = new ArrayListWritable<Writable>();
		list.add(new Text("hi"));
		list.add(new Text("there"));

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ArrayListWritable<Text> newList = new ArrayListWritable<Text>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		assertEquals(newList.get(0).toString(), "hi");
		assertEquals(newList.get(1).toString(), "there");
	}

	@Test
	public void testSerialize2() throws IOException {
		ArrayListWritable<FloatWritable> list = new ArrayListWritable<FloatWritable>();

		list.add(new FloatWritable(0.3f));
		list.add(new FloatWritable(3244.2f));

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ArrayListWritable<FloatWritable> newList = new ArrayListWritable<FloatWritable>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		assertTrue(newList.get(0).get() == 0.3f);
		assertTrue(newList.get(1).get() == 3244.2f);
	}

	@Test
	public void testSerialize3() throws IOException {
		//ArrayListWritable<Text> list = new ArrayListWritable<Text>();
		ArrayListWritable<Writable> list = new ArrayListWritable<Writable>();
		list.add(new Text("hi"));
		list.add(new IntWritable(1));

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ArrayListWritable<Writable> newList = new ArrayListWritable<Writable>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		assertEquals(newList.get(0).toString(), "hi");
		IntWritable i=(IntWritable)(newList.get(1));
		assertEquals(i.get(), 1);
	}

	@Test
	public void testToString() {
		ArrayListWritable<Text> list = new ArrayListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));

		assertEquals(list.toString(), "[hi, there]");
	}

	@Test
	public void testClear() {
		ArrayListWritable<Text> list = new ArrayListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));
		list.clear();
		
		assertEquals(list.size(), 0);
	}

	@Test
	public void testEmpty() throws IOException {
		ArrayListWritable<Text> list = new ArrayListWritable<Text>();
		
		assertTrue(list.size() == 0);
		
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ArrayListWritable<Text> newList = new ArrayListWritable<Text>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));
		assertTrue(newList.size() == 0);
		
		newList.add(new Text("Hey"));
		assertEquals(newList.get(0),new Text("Hey"));

	}
	
	/*@Test
	public void testTypeSafety() {
		ArrayListWritable<WritableComparable> list = new ArrayListWritable<WritableComparable> ();
		list.add(new Text("Hello"));
		list.add(new Text("Are you there"));
		
		try {
			list.add(new IntWritable(5));
			assertTrue(false); // should throw an exception before reaching this line.
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
		
		ArrayList<WritableComparable> otherList = new ArrayList<WritableComparable>();
		otherList.add(new Text("Test"));
		otherList.add(new Text("Test 2"));
		
		assertTrue(list.addAll(otherList));
		
		otherList.add(new IntWritable(6));
		try {
			list.addAll(otherList);
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}*/
	
	@Test 
	public void testListMethods() {
		IntWritable a = new IntWritable(1);
		IntWritable b = new IntWritable(2);
		IntWritable c = new IntWritable(3);
		IntWritable d = new IntWritable(4);
		IntWritable e = new IntWritable(5);
		
		ArrayListWritable<IntWritable> list = new ArrayListWritable<IntWritable>();
		assertTrue(list.isEmpty());
		list.add(a);
		list.add(b);
		list.add(c);
		list.add(d);
		list.add(e);
		
		int pos = 0;
		for (IntWritable i : list) {
			assertEquals(i, list.get(pos));
			++pos;
		}
		
		assertTrue(list.indexOf(d) == 3);
		list.add(2, a);
		assertTrue(list.lastIndexOf(a) == 2);
		assertEquals(list.get(2), list.get(0));
		assertTrue(list.size() == 6);
		
		assertTrue(list.contains(c));
		assertTrue(!list.contains(new IntWritable(123)));
		
		ArrayList<IntWritable> otherList = new ArrayList<IntWritable>();
		otherList.add(a);
		otherList.add(b);
		otherList.add(c);
		
		assertTrue(list.containsAll(otherList));
		
		otherList.add(new IntWritable(200));
		assertTrue(!list.containsAll(otherList));
		
		assertEquals(a, otherList.remove(0));
		assertTrue(list.remove(d));
		
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListWritableTest.class);
	}

}
