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
		newList.readFields(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));
		
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
		newList.readFields(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));
		
		assertTrue(newList.get(0).get() == 0.3f);
		assertTrue(newList.get(1).get() == 3244.2f);
	}


	@Test
	public void testToString() throws IOException {
		ListWritable<Text> list = new ListWritable<Text>();
		
		list.add(new Text("hi"));
		list.add(new Text("there"));

		assertEquals(list.toString(), "[hi, there]");
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ListWritableTest.class);
	}

}
