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

package edu.umd.cloud9.util.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;
import edu.umd.cloud9.util.map.MapIV.Entry;

public class HMapIVTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		String[] strings = new String[size];

		MapIV<String> map = new HMapIV<String>();
		for (int i = 0; i < size; i++) {
			String s = new Integer(r.nextInt(size)).toString();
			map.put(i, s);
			strings[i] = s;
		}

		for (int i = 0; i < size; i++) {
			String v = map.get(i);

			assertEquals(strings[i], v);
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		String[] strings = new String[size];

		MapIV<String> map = new HMapIV<String>();
		for (int i = 0; i < size; i++) {
			String s = new Integer(r.nextInt(size)).toString();
			map.put(i, s);
			strings[i] = s;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, new Integer(Integer.parseInt(strings[i]) + 1).toString());
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			String v = map.get(i);

			assertEquals(new Integer(Integer.parseInt(strings[i]) + 1).toString(), v);
			assertTrue(map.containsKey(i));
		}

	}
	
	@Test
	public void testBasic() throws IOException {
		HMapIV<String> m = new HMapIV<String>();

		m.put(1, "5");
		m.put(2, "22");

		String value;

		assertEquals(2, m.size());

		value = m.get(1);
		assertEquals("5", m.get(1));

		value = m.remove(1);
		assertEquals(1, m.size());

		value = m.get(2);
		assertEquals("22", value);
	}

	@Test
	public void testSortedEntries1() {
		HMapIV<String> m = new HMapIV<String>();

		m.put(1, "5");
		m.put(2, "2");
		m.put(3, "3");
		m.put(4, "3");
		m.put(5, "1");

		Entry<String>[] e = m.getEntriesSortedByValue();
		assertEquals(5, e.length);

		assertEquals(5, e[0].getKey());
		assertEquals("1", e[0].getValue());

		assertEquals(2, e[1].getKey());
		assertEquals("2", e[1].getValue());

		assertEquals(3, e[2].getKey());
		assertEquals("3", e[2].getValue());

		assertEquals(4, e[3].getKey());
		assertEquals("3", e[3].getValue());

		assertEquals(1, e[4].getKey());
		assertEquals("5", e[4].getValue());
	}

	@Test
	public void testSortedEntries2() {
		HMapIV<String> m = new HMapIV<String>();

		m.put(1, "5");
		m.put(2, "2");
		m.put(3, "3");
		m.put(4, "3");
		m.put(5, "1");

		Entry<String>[] e = m.getEntriesSortedByValue(2);

		assertEquals(2, e.length);

		assertEquals(5, e[0].getKey());
		assertEquals("1", e[0].getValue());

		assertEquals(2, e[1].getKey());
		assertEquals("2", e[1].getValue());
	}

	@Test
	public void testSortedEntries3() {
		HMapIV<String> m = new HMapIV<String>();

		m.put(1, "5");
		m.put(2, "2");

		Entry<String>[] e = m.getEntriesSortedByValue(5);

		assertEquals(2, e.length);

		assertEquals(2, e[0].getKey());
		assertEquals("2", e[0].getValue());

		assertEquals(1, e[1].getKey());
		assertEquals("5", e[1].getValue());
	}

	@Test
	public void testSortedEntries4() {
		HMapIV<String> m = new HMapIV<String>();

		Entry<String>[] e = m.getEntriesSortedByValue();
		assertTrue(e == null);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapIVTest.class);
	}

}