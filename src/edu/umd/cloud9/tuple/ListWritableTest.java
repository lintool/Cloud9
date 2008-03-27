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

package src.edu.umd.cloud9.tuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ListWritableTest {

	@Test
	public void testBasic() throws IOException {
		ListWritable<Text> list = new ListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));

		assertEquals(list.get(0).toString(), "hi");
		assertEquals(list.get(1).toString(), "there");
	}

	@Test
	public void testSerialize1() throws IOException {
		ListWritable<Text> list = new ListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ListWritable<Text> newList = new ListWritable<Text>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		assertEquals(newList.get(0).toString(), "hi");
		assertEquals(newList.get(1).toString(), "there");
	}

	@Test
	public void testSerialize2() throws IOException {
		ListWritable<FloatWritable> list = new ListWritable<FloatWritable>();

		list.add(new FloatWritable(0.3f));
		list.add(new FloatWritable(3244.2f));

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ListWritable<FloatWritable> newList = new ListWritable<FloatWritable>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		assertTrue(newList.get(0).get() == 0.3f);
		assertTrue(newList.get(1).get() == 3244.2f);
	}

	@Test
	public void testToString() {
		ListWritable<Text> list = new ListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));

		assertEquals(list.toString(), "[hi, there]");
	}

	@Test
	public void testClear() {
		ListWritable<Text> list = new ListWritable<Text>();

		list.add(new Text("hi"));
		list.add(new Text("there"));
		list.clear();
		
		assertEquals(list.size(), 0);
	}

	@Test
	public void testSorting1() {
		ListWritable<Text> list1 = new ListWritable<Text>();
		ListWritable<Text> list2 = new ListWritable<Text>();

		list1.add(new Text("a"));

		assertTrue(list1.compareTo(list2) > 0);
	}
	
	@Test
	public void testSorting2() {
		ListWritable<Text> list1 = new ListWritable<Text>();
		ListWritable<Text> list2 = new ListWritable<Text>();

		list1.add(new Text("a"));
		list2.add(new Text("b"));

		assertTrue(list1.compareTo(list2) < 0);
		assertTrue(list2.compareTo(list1) > 0);
		
		list2.clear();
		list2.add(new Text("a"));
		
		assertTrue(list1.compareTo(list2) == 0);
		
		list1.add(new Text("a"));
		list2.add(new Text("b"));
		
		// list 1 is now [a, a]
		// list 2 is now [a, b]
		assertTrue(list1.compareTo(list2) < 0);
		assertTrue(list2.compareTo(list1) > 0);

		// list 1 is now [a, a, a]
		list1.add(new Text("a"));
		
		assertTrue(list1.compareTo(list2) < 0);
	}

	@Test
	public void testSorting3() {
		ListWritable<Text> list1 = new ListWritable<Text>();
		ListWritable<Text> list2 = new ListWritable<Text>();
		ListWritable<Text> list3 = new ListWritable<Text>();

		list1.add(new Text("a"));
		
		list2.add(new Text("a"));
		list2.add(new Text("a"));
		
		list3.add(new Text("a"));
		list3.add(new Text("a"));
		
		assertTrue(list2.compareTo(list3) == 0);

		list3.add(new Text("a"));
		
		// list 1 is [a]
		// list 2 is [a, a]
		// list 3 is [a, a, a]
		
		assertTrue(list1.compareTo(list2) < 0);
		assertTrue(list1.compareTo(list3) < 0);
		assertTrue(list2.compareTo(list1) > 0);
		assertTrue(list2.compareTo(list3) < 0);
		assertTrue(list3.compareTo(list1) > 0);
		assertTrue(list3.compareTo(list2) > 0);
	}
	
	@Test
	public void testEmpty() throws IOException {
		ListWritable<Text> list = new ListWritable<Text>();
		
		assertTrue(list.size() == 0);
		
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		list.write(dataOut);

		ListWritable<Text> newList = new ListWritable<Text>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));
		assertTrue(newList.size() == 0);
		
		newList.add(new Text("Hey"));
		assertEquals(newList.get(0),new Text("Hey"));

	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ListWritableTest.class);
	}

}
