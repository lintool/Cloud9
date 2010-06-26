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
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class HMapKITest {

	@Test
	public void testBasic1() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		MapKI<Integer> map = new HMapKI<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			assertEquals(ints[i], v);
			assertTrue(map.containsKey(i));
		}

	}

	@Test
	public void testBasic2() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];
		String[] strings = new String[size];

		MapKI<String> map = new HMapKI<String>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			String s = new Integer(k).toString();
			map.put(s, k);
			ints[i] = k;
			strings[i] = s;
		}

		for (int i = 0; i < size; i++) {
			int v = map.get(strings[i]);

			assertEquals(ints[i], v);
			assertTrue(map.containsKey(strings[i]));
		}

	}

	@Test
	public void testUpdate() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		MapKI<Integer> map = new HMapKI<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, ints[i] + 1);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			assertEquals(ints[i] + 1, v);
			assertTrue(map.containsKey(i));
		}

	}

	@Test
	public void testToString() throws IOException {
		HMapKI<String> m1 = new HMapKI<String>();

		m1.put("hi", 5);
		m1.put("there", 22);

		assertEquals("{there=22, hi=5}", m1.toString());
	}

	@Test
	public void testBasic() throws IOException {
		HMapKI<Text> m = new HMapKI<Text>();

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
		HMapKI<Text> m1 = new HMapKI<Text>();

		m1.put(new Text("hi"), 5);
		m1.put(new Text("there"), 22);

		HMapKI<Text> m2 = new HMapKI<Text>();

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
		HMapKI<Text> m1 = new HMapKI<Text>();

		m1.put(new Text("hi"), 5);
		m1.put(new Text("there"), 2);
		m1.put(new Text("empty"), 3);

		HMapKI<Text> m2 = new HMapKI<Text>();

		m2.put(new Text("hi"), 4);
		m2.put(new Text("there"), 4);
		m2.put(new Text("test"), 5);

		int s = m1.dot(m2);

		assertEquals(s, 28);
	}

	@Test
	public void testSortedEntriesValue1() {
		HMapKI<Text> m = new HMapKI<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		MapKI.Entry<Text>[] entries = m.getEntriesSortedByValue();
		MapKI.Entry<Text> e = null;

		assertEquals(5, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = entries[1];
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = entries[2];
		assertEquals(new Text("d"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = entries[3];
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2, (int) e.getValue());

		e = entries[4];
		assertEquals(new Text("e"), e.getKey());
		assertEquals(1, (int) e.getValue());
	}

	@Test
	public void testSortedEntriesValue2() {
		HMapKI<Text> m = new HMapKI<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		MapKI.Entry<Text>[] entries = m.getEntriesSortedByValue(2);
		MapKI.Entry<Text> e = null;

		assertEquals(2, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = entries[1];
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3, (int) e.getValue());
	}

	@Test
	public void testSortedEntriesKey1() {
		HMapKI<Text> m = new HMapKI<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		MapKI.Entry<Text>[] entries = m.getEntriesSortedByKey();
		MapKI.Entry<Text> e = null;

		assertEquals(5, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = entries[1];
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2, (int) e.getValue());

		e = entries[2];
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = entries[3];
		assertEquals(new Text("d"), e.getKey());
		assertEquals(3, (int) e.getValue());

		e = entries[4];
		assertEquals(new Text("e"), e.getKey());
		assertEquals(1, (int) e.getValue());
	}

	@Test
	public void testSortedEntriesKey2() {
		HMapKI<Text> m = new HMapKI<Text>();

		m.put(new Text("a"), 5);
		m.put(new Text("b"), 2);
		m.put(new Text("c"), 3);
		m.put(new Text("d"), 3);
		m.put(new Text("e"), 1);

		MapKI.Entry<Text>[] entries = m.getEntriesSortedByKey(2);
		MapKI.Entry<Text> e = null;

		assertEquals(2, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5, (int) e.getValue());

		e = entries[1];
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2, (int) e.getValue());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapKITest.class);
	}

}