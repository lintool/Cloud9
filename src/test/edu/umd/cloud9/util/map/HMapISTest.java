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

import edu.umd.cloud9.util.map.HMapIS;
import edu.umd.cloud9.util.map.MapIS;
import edu.umd.cloud9.util.map.MapIS.Entry;

public class HMapISTest {

	@Test
	public void testBasic1() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		MapIS map = new HMapIS();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, (short) (k * 2));
			shorts[i] = (short) (k * 2);
		}

		for (int i = 0; i < size; i++) {
			short v = map.get(i);

			assertEquals(shorts[i], v);
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testUpdate() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		MapIS map = new HMapIS();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, (short) (k + 10));
			shorts[i] = (short) (k + 10);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, (short) (shorts[i] + 10));
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			short v = map.get(i);

			assertEquals(shorts[i] + 10, v);
			assertTrue(map.containsKey(i));
		}

	}

	@Test
	public void testBasic() throws IOException {
		HMapIS m = new HMapIS();

		m.put(1, (short) 5);
		m.put(2, (short) 22);

		short value;

		assertEquals(2, m.size());

		value = m.get(1);
		assertEquals(5, value);

		value = m.remove(1);
		assertEquals(m.size(), 1);

		value = m.get(2);
		assertEquals(22L, value);
	}

	@Test
	public void testPlus() throws IOException {
		HMapIS m1 = new HMapIS();

		m1.put(1, (short) 5);
		m1.put(2, (short) 22);

		HMapIS m2 = new HMapIS();

		m2.put(1, (short) 4);
		m2.put(3, (short) 5);

		m1.plus(m2);

		assertEquals(m1.size(), 3);
		assertTrue(m1.get(1) == 9);
		assertTrue(m1.get(2) == 22);
		assertTrue(m1.get(3) == 5);
	}

	@Test
	public void testDot() throws IOException {
		HMapIS m1 = new HMapIS();

		m1.put(1, (short) 2);
		m1.put(2, (short) 1);
		m1.put(3, (short) 3);

		HMapIS m2 = new HMapIS();

		m2.put(1, (short) 1);
		m2.put(2, (short) 4);
		m2.put(4, (short) 5);

		int s = m1.dot(m2);

		assertEquals(6, s);
	}

	@Test
	public void testSortedEntries1() {
		HMapIS m = new HMapIS();

		m.put(1, (short)5);
		m.put(2, (short)2);
		m.put(3, (short)3);
		m.put(4, (short) 3);
		m.put(5, (short) 1);

		Entry[] e = m.getEntriesSortedByValue();
		assertEquals(5, e.length);

		assertEquals(1, e[0].getKey());
		assertEquals(5L, e[0].getValue());

		assertEquals(3, e[1].getKey());
		assertEquals(3L, e[1].getValue());

		assertEquals(4, e[2].getKey());
		assertEquals(3L, e[2].getValue());

		assertEquals(2, e[3].getKey());
		assertEquals(2L, e[3].getValue());

		assertEquals(5, e[4].getKey());
		assertEquals(1L, e[4].getValue());
	}

	@Test
	public void testSortedEntries2() {
		HMapIS m = new HMapIS();

		m.put(1, (short) 5);
		m.put(2, (short) 2);
		m.put(3, (short) 3);
		m.put(4, (short) 3);
		m.put(5, (short) 1);

		Entry[] e = m.getEntriesSortedByValue(2);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getKey());
		assertEquals(5L, e[0].getValue());

		assertEquals(3, e[1].getKey());
		assertEquals(3L, e[1].getValue());
	}
	
	@Test
	public void testSortedEntries3() {
		HMapIS m = new HMapIS();

		m.put(1, (short) 5);
		m.put(2, (short) 2);

		Entry[] e = m.getEntriesSortedByValue(5);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getKey());
		assertEquals(5L, e[0].getValue());

		assertEquals(2, e[1].getKey());
		assertEquals(2L, e[1].getValue());
	}
	
	@Test
	public void testSortedEntries4() {
		HMapIS m = new HMapIS();

		Entry[] e = m.getEntriesSortedByValue();
		assertTrue(e == null);
	}

  @Test
  public void testIncrement() {
    HMapIS m = new HMapIS();
    assertEquals(0, m.get(1));

    m.increment(1, (short)1);
    assertEquals(1, m.get(1));

    m.increment(1, (short)1);
    m.increment(2, (short)0);
    m.increment(3, (short)-1);
    
    assertEquals(2, m.get(1));
    assertEquals(0, m.get(2));
    assertEquals(-1, m.get(3));
  }

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapISTest.class);
	}

}