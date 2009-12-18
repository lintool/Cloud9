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

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.MapIV.Entry;

public class OHMapIVTest {

	@Test
	public void testBasic() throws IOException {
		OHMapIV<String> m = new OHMapIV<String>();

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
		OHMapIV<String> m = new OHMapIV<String>();

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
		OHMapIV<String> m = new OHMapIV<String>();

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
		OHMapIV<String> m = new OHMapIV<String>();

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
		OHMapIV<String> m = new OHMapIV<String>();

		Entry<String>[] e = m.getEntriesSortedByValue();
		assertTrue(e == null);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OHMapIVTest.class);
	}

}
