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

public class OrderedHashMapIntTest {

	@Test
	public void testBasic() throws IOException {
		OrderedHashMapInt<Text> m = new OrderedHashMapInt<Text>();

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
	public void testPlus() throws IOException {
		OrderedHashMapInt<Text> m1 = new OrderedHashMapInt<Text>();

		m1.put(new Text("hi"), 5);
		m1.put(new Text("there"), 22);

		OrderedHashMapInt<Text> m2 = new OrderedHashMapInt<Text>();

		m2.put(new Text("hi"), 4);
		m2.put(new Text("test"), 5);

		m1.plus(m2);

		assertEquals(3, m1.size());
		assertTrue(m1.get(new Text("hi")) == 9);
		assertTrue(m1.get(new Text("there")) == 22);
		assertTrue(m1.get(new Text("test")) == 5);
	}

	@Test
	public void testDot() throws IOException {
		OrderedHashMapInt<Text> m1 = new OrderedHashMapInt<Text>();

		m1.put(new Text("hi"), 5);
		m1.put(new Text("there"), 2);
		m1.put(new Text("empty"), 3);

		OrderedHashMapInt<Text> m2 = new OrderedHashMapInt<Text>();

		m2.put(new Text("hi"), 4);
		m2.put(new Text("there"), 4);
		m2.put(new Text("test"), 5);

		int s = m1.dot(m2);

		assertEquals(s, 28);
	}

	@Test
	public void testSortedEntries1() {

		OrderedHashMapInt<Text> m = new OrderedHashMapInt<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		Iterator<MapInt.Entry<Text>> iter = m.getEntriesSortedByValue().iterator();

		MapInt.Entry<Text> e = iter.next();
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = iter.next();
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = iter.next();
		assertEquals(new Text("d"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = iter.next();
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2, (int) e.getValue());

		e = iter.next();
		assertEquals(new Text("e"), e.getKey());
		assertEquals(1, (int) e.getValue());

		assertEquals(false, iter.hasNext());
	}

	@Test
	public void testSortedEntries2() {

		OrderedHashMapInt<Text> m = new OrderedHashMapInt<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		Iterator<MapInt.Entry<Text>> iter = m.getEntriesSortedByValue(2).iterator();

		MapInt.Entry<Text> e = iter.next();
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = iter.next();
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3, (int) e.getValue());

		assertEquals(false, iter.hasNext());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OrderedHashMapIntTest.class);
	}

}
