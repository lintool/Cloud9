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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

public class HashMapWritableTest {

	@Test
	public void testBasic() throws IOException {
		HashMapWritable<Text, IntWritable> map = new HashMapWritable<Text, IntWritable>();

		map.put(new Text("hi"), new IntWritable(5));
		map.put(new Text("there"), new IntWritable(22));

		Text key;
		IntWritable value;

		assertEquals(map.size(), 2);
		
		key=new Text("hi");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 5);
		
		value=map.remove(key);
		assertEquals(map.size(), 1);
		
		key=new Text("there");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 22);
	}

	@Test
	public void testSerialize1() throws IOException {
		HashMapWritable<Text, IntWritable> origMap = new HashMapWritable<Text, IntWritable>();

		origMap.put(new Text("hi"), new IntWritable(5));
		origMap.put(new Text("there"), new IntWritable(22));
	
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		HashMapWritable<Text, IntWritable> map = new HashMapWritable<Text, IntWritable>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		Text key;
		IntWritable value;

		assertEquals(map.size(), 2);
		
		key=new Text("hi");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 5);
		
		value=map.remove(key);
		assertEquals(map.size(), 1);
		
		key=new Text("there");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 22);
	}

	@Test
	public void testSerialize2() throws IOException {
		HashMapWritable<Text, LongWritable> origMap = new HashMapWritable<Text, LongWritable>();

		origMap.put(new Text("hi"), new LongWritable(52));
		origMap.put(new Text("there"), new LongWritable(77));
	
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		HashMapWritable<Text, LongWritable> map = new HashMapWritable<Text, LongWritable>();

		map.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));

		Text key;
		LongWritable value;

		assertEquals(map.size(), 2);
		
		key=new Text("hi");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 52);
		
		value=map.remove(key);
		assertEquals(map.size(), 1);
		
		key=new Text("there");
		value=map.get(key);
		assertTrue(value!=null);
		assertEquals(value.get(), 77);
	}


	@Test
	public void testTypeSafety() throws IOException {
		HashMapWritable<Writable, Writable> origMap = new HashMapWritable<Writable, Writable>();

		origMap.put(new Text("hi"), new FloatWritable(5.3f));
		origMap.put(new Text("there"), new Text("bbb"));
	
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origMap.write(dataOut);

		HashMapWritable<Writable, Writable> map = new HashMapWritable<Writable, Writable>();

		try {
	        map.readFields(new DataInputStream(new ByteArrayInputStream(
	        		bytesOut.toByteArray())));
	        assertTrue(false);
        } catch (Exception e) {
        }
	}



	@Test
	public void testSerializeEmpty() throws IOException {
		HashMapWritable<IntWritable, Text> map = new HashMapWritable<IntWritable, Text>();
		
		assertTrue(map.size() == 0);
		
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		map.write(dataOut);

		HashMapWritable<IntWritable, Text> newList = new HashMapWritable<IntWritable, Text>();
		newList.readFields(new DataInputStream(new ByteArrayInputStream(
				bytesOut.toByteArray())));
		assertTrue(newList.size() == 0);
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HashMapWritableTest.class);
	}

}
