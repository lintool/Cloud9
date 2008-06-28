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
		VectorFloat<Text> v = new VectorFloat<Text>();

		v.set(new Text("hi"), 5.0f);
		v.set(new Text("there"), 22.0f);

		Text key;
		float value;

		assertEquals(v.size(), 2);

		key = new Text("hi");
		value = v.get(key);
		assertTrue(value == 5.0f);

		value = v.remove(key);
		assertEquals(v.size(), 1);

		key = new Text("there");
		value = v.get(key);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerialize1() throws IOException {
		VectorFloat<Text> v1 = new VectorFloat<Text>();

		v1.set(new Text("hi"), 5.0f);
		v1.set(new Text("there"), 22.0f);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorFloat<Text> v2 = new VectorFloat<Text>();

		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		Text key;
		float value;

		assertEquals(v2.size(), 2);

		key = new Text("hi");
		value = v2.get(key);
		assertTrue(value == 5.0f);

		value = v2.remove(key);
		assertEquals(v2.size(), 1);

		key = new Text("there");
		value = v2.get(key);
		assertTrue(value == 22.0f);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		VectorFloat<WritableComparable> v1 = new VectorFloat<WritableComparable>();

		v1.set(new Text("hi"), 4.0f);
		v1.set(new IntWritable(0), 76.0f);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorFloat<WritableComparable> v2 = new VectorFloat<WritableComparable>();

		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

	}

	@Test
	public void testSerializeEmpty() throws IOException {
		VectorFloat<WritableComparable> v1 = new VectorFloat<WritableComparable>();

		assertTrue(v1.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		v1.write(dataOut);

		VectorFloat<WritableComparable> v2 = new VectorFloat<WritableComparable>();
		v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
		assertTrue(v2.size() == 0);
	}

	@Test
	public void testPlus() throws IOException {
		VectorFloat<Text> v1 = new VectorFloat<Text>();

		v1.set(new Text("hi"), 5.0f);
		v1.set(new Text("there"), 22.0f);

		VectorFloat<Text> v2 = new VectorFloat<Text>();

		v2.set(new Text("hi"), 4.0f);
		v2.set(new Text("test"), 5.0f);

		v1.plus(v2);

		assertEquals(v1.size(), 3);
		assertTrue(v1.get(new Text("hi")) == 9);
		assertTrue(v1.get(new Text("there")) == 22);
		assertTrue(v1.get(new Text("test")) == 5);
	}

	@Test
	public void testDot() throws IOException {
		VectorFloat<Text> v1 = new VectorFloat<Text>();

		v1.set(new Text("hi"), 2.3f);
		v1.set(new Text("there"), 1.9f);
		v1.set(new Text("empty"), 3.0f);

		VectorFloat<Text> v2 = new VectorFloat<Text>();

		v2.set(new Text("hi"), 1.2f);
		v2.set(new Text("there"), 4.3f);
		v2.set(new Text("test"), 5.0f);

		float s = v1.dot(v2);

		assertTrue(s == 10.93f);
	}

	@Test
	public void testLengthAndNormalize() throws IOException {
		VectorFloat<Text> v1 = new VectorFloat<Text>();

		v1.set(new Text("hi"), 2.3f);
		v1.set(new Text("there"), 1.9f);
		v1.set(new Text("empty"), 3.0f);

		assertEquals(v1.length(), 4.2308393, 10E-6);

		v1.normalize();

		assertEquals(v1.get(new Text("hi")), 0.5436274, 10E-6);
		assertEquals(v1.get(new Text("there")), 0.44908348, 10E-6);
		assertEquals(v1.get(new Text("empty")), 0.70907915, 10E-6);
		assertEquals(v1.length(), 1, 10E-6);

		VectorFloat<Text> v2 = new VectorFloat<Text>();

		v2.set(new Text("hi"), 1.2f);
		v2.set(new Text("there"), 4.3f);
		v2.set(new Text("test"), 5.0f);

		assertEquals(v2.length(), 6.7029843, 10E-6);

		v2.normalize();

		assertEquals(v2.get(new Text("hi")), 0.17902474, 10E-6);
		assertEquals(v2.get(new Text("there")), 0.64150536, 10E-6);
		assertEquals(v2.get(new Text("test")), 0.7459364, 10E-6);
		assertEquals(v2.length(), 1, 10E-6);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(VectorFloatTest.class);
	}

}
