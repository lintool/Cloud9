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

package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrderedHashMapFloatTest {

	@Test
	public void testBasic() throws IOException {
		OrderedHashMapFloat<Text> m = new OrderedHashMapFloat<Text>();

		m.put(new Text("hi"), 5.0f);
		m.put(new Text("there"), 22.0f);

		Text key;
		float value;

		assertEquals(m.size(), 2);

		key = new Text("hi");
		value = m.get(key);
		assertTrue(value == 5.0f);

		value = m.remove(key);
		assertEquals(m.size(), 1);

		key = new Text("there");
		value = m.get(key);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testPlus() throws IOException {
		OrderedHashMapFloat<Text> m1 = new OrderedHashMapFloat<Text>();

		m1.put(new Text("hi"), 5.0f);
		m1.put(new Text("there"), 22.0f);

		OrderedHashMapFloat<Text> m2 = new OrderedHashMapFloat<Text>();

		m2.put(new Text("hi"), 4.0f);
		m2.put(new Text("test"), 5.0f);

		m1.plus(m2);

		assertEquals(m1.size(), 3);
		assertTrue(m1.get(new Text("hi")) == 9);
		assertTrue(m1.get(new Text("there")) == 22);
		assertTrue(m1.get(new Text("test")) == 5);
	}

	@Test
	public void testDot() throws IOException {
		OrderedHashMapFloat<Text> m1 = new OrderedHashMapFloat<Text>();

		m1.put(new Text("hi"), 2.3f);
		m1.put(new Text("there"), 1.9f);
		m1.put(new Text("empty"), 3.0f);

		OrderedHashMapFloat<Text> m2 = new OrderedHashMapFloat<Text>();

		m2.put(new Text("hi"), 1.2f);
		m2.put(new Text("there"), 4.3f);
		m2.put(new Text("test"), 5.0f);

		float s = m1.dot(m2);

		assertTrue(s == 10.93f);
	}

	@Test
	public void testLengthAndNormalize() throws IOException {
		OrderedHashMapFloat<Text> m1 = new OrderedHashMapFloat<Text>();

		m1.put(new Text("hi"), 2.3f);
		m1.put(new Text("there"), 1.9f);
		m1.put(new Text("empty"), 3.0f);

		assertEquals(m1.length(), 4.2308393, 10E-6);

		m1.normalize();

		assertEquals(m1.get(new Text("hi")), 0.5436274, 10E-6);
		assertEquals(m1.get(new Text("there")), 0.44908348, 10E-6);
		assertEquals(m1.get(new Text("empty")), 0.70907915, 10E-6);
		assertEquals(m1.length(), 1, 10E-6);

		OrderedHashMapFloat<Text> m2 = new OrderedHashMapFloat<Text>();

		m2.put(new Text("hi"), 1.2f);
		m2.put(new Text("there"), 4.3f);
		m2.put(new Text("test"), 5.0f);

		assertEquals(m2.length(), 6.7029843, 10E-6);

		m2.normalize();

		assertEquals(m2.get(new Text("hi")), 0.17902474, 10E-6);
		assertEquals(m2.get(new Text("there")), 0.64150536, 10E-6);
		assertEquals(m2.get(new Text("test")), 0.7459364, 10E-6);
		assertEquals(m2.length(), 1, 10E-6);
	}

	@Test
	public void testSortedEntries1() {
		OrderedHashMapFloat<Text> m = new OrderedHashMapFloat<Text>();

		m.put(new Text("a"), 5.0f);
		m.put(new Text("b"), 2.0f);
		m.put(new Text("c"), 3.0f);
		m.put(new Text("d"), 3.0f);
		m.put(new Text("e"), 1.0f);

		Iterator<MapFloat.Entry<Text>> iter = m.getEntriesSortedByValue().iterator();

		MapFloat.Entry<Text> e = iter.next();
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5.0f, (float) e.getValue(), 10E-6);

		e = iter.next();
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3.0f, (float) e.getValue(), 10E-6);

		e = iter.next();
		assertEquals(new Text("d"), e.getKey());
		assertEquals(3.0f, (float) e.getValue(), 10E-6);

		e = iter.next();
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2.0f, (float) e.getValue(), 10E-6);

		e = iter.next();
		assertEquals(new Text("e"), e.getKey());
		assertEquals(1.0f, (float) e.getValue(), 10E-6);

		assertEquals(false, iter.hasNext());
	}

	@Test
	public void testSortedEntries2() {
		OrderedHashMapFloat<Text> m = new OrderedHashMapFloat<Text>();

		m.put(new Text("a"), 5.0f);
		m.put(new Text("b"), 2.0f);
		m.put(new Text("c"), 3.0f);
		m.put(new Text("d"), 3.0f);
		m.put(new Text("e"), 1.0f);

		Iterator<MapFloat.Entry<Text>> iter = m.getEntriesSortedByValue(2).iterator();

		MapFloat.Entry<Text> e = iter.next();
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5.0f, (float) e.getValue(), 10E-6);

		e = iter.next();
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3.0f, (float) e.getValue(), 10E-6);

		assertEquals(false, iter.hasNext());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OrderedHashMapFloatTest.class);
	}

}
