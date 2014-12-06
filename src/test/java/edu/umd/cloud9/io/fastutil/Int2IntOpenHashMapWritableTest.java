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

package edu.umd.cloud9.io.fastutil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.IOException;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.map.Int2IntOpenHashMapWritable;

public class Int2IntOpenHashMapWritableTest {

	@Test
	public void testBasic() throws IOException {
		Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

		m.put(2, 5);
		m.put(1, 22);

		int value;

		assertEquals(2, m.size());

		value = m.get(2);
		assertEquals(5, value);

		value = m.remove(2);
		assertEquals(1, m.size());

		value = m.get(1);
		assertEquals(22, value);
	}

  @Test
  public void testIncrement() throws IOException {
    Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

    m.put(2, 7);
    m.put(1, 29);

    assertEquals(7, m.get(2));
    assertEquals(29, m.get(1));

    m.increment(2);
    m.increment(1);
    m.increment(3);

    assertEquals(8, m.get(2));
    assertEquals(30, m.get(1));
    assertEquals(1, m.get(3));

    m.increment(1, 3);
    m.increment(3, 5);

    assertEquals(8, m.get(2));
    assertEquals(33, m.get(1));
    assertEquals(6, m.get(3));
  }

	@Test
	public void testSerialize1() throws IOException {
		Int2IntOpenHashMapWritable.setLazyDecodeFlag(false);
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(3, 5);
		m1.put(4, 22);

		Int2IntOpenHashMapWritable n2 = Int2IntOpenHashMapWritable.create(m1.serialize());

		int value;

		assertEquals(2, n2.size());

		value = n2.get(3);
		assertEquals(5, value);

		value = n2.remove(3);
		assertEquals(1, n2.size());

		value = n2.get(4);
		assertEquals(22, value);
	}

	@Test
	public void testSerializeLazy1() throws IOException {
		Int2IntOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(3, 5);
		m1.put(4, 22);

		Int2IntOpenHashMapWritable m2 = Int2IntOpenHashMapWritable.create(m1.serialize());

		assertEquals(0, m2.size());
		assertFalse(m2.hasBeenDecoded());

		int[] keys = m2.getKeys();
		int[] values = m2.getValues();

		assertEquals(3, keys[0]);
		assertEquals(4, keys[1]);

		assertEquals(5, values[0]);
		assertEquals(22, values[1]);

		m2.decode();
		assertTrue(m2.hasBeenDecoded());

		int value;
		assertEquals(2, m2.size());

		value = m2.get(3);
		assertEquals(5, value);

		value = m2.remove(3);
		assertEquals(1, m2.size());

		value = m2.get(4);
		assertEquals(22, value);
	}

	@Test
	public void testSerializeLazy2() throws IOException {
		Int2IntOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(3, 5);
		m1.put(4, 22);

		// Object m2 should not have been decoded, size lazy decode flag is
		// true.
		Int2IntOpenHashMapWritable m2 = Int2IntOpenHashMapWritable.create(m1.serialize());

		// Even though m2 hasn't be decoded, we should be able to properly
		// serialize it.
		Int2IntOpenHashMapWritable m3 = Int2IntOpenHashMapWritable.create(m2.serialize());

		assertEquals(0, m3.size());
		assertFalse(m3.hasBeenDecoded());

		int[] keys = m3.getKeys();
		int[] values = m3.getValues();

		assertEquals(3, keys[0]);
		assertEquals(4, keys[1]);

		assertEquals(5, values[0]);
		assertEquals(22, values[1]);

		m3.decode();
		assertTrue(m3.hasBeenDecoded());

		int value;
		assertEquals(2, m3.size());

		value = m3.get(3);
		assertEquals(5, value);

		value = m3.remove(3);
		assertEquals(1, m3.size());

		value = m3.get(4);
		assertEquals(22, value);
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		// make sure this does nothing
		m1.decode();

		assertEquals(0, m1.size());

		Int2IntMap m2 = Int2IntOpenHashMapWritable.create(m1.serialize());

		assertEquals(0, m2.size());
	}

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		Int2IntMap map = new Int2IntOpenHashMapWritable();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(ints[i], map.get(i));
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		Int2IntMap map = new Int2IntOpenHashMapWritable();
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
			assertEquals(ints[i] + 1, map.get(i));
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testPlus() throws IOException {
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(1, 5);
		m1.put(2, 22);

		Int2IntOpenHashMapWritable m2 = new Int2IntOpenHashMapWritable();

		m2.put(1, 4);
		m2.put(3, 5);

		m1.plus(m2);

		assertEquals(3, m1.size());
		assertEquals(9, m1.get(1));
		assertEquals(22, m1.get(2));
		assertEquals(5, m1.get(3));
	}

	@Test
	public void testLazyPlus() throws IOException {
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(1, 5);
		m1.put(2, 22);

		Int2IntOpenHashMapWritable m2 = new Int2IntOpenHashMapWritable();

		m2.put(1, 4);
		m2.put(3, 5);

		Int2IntOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2IntOpenHashMapWritable m3 = Int2IntOpenHashMapWritable.create(m2.serialize());

		assertEquals(0, m3.size());

		m1.lazyplus(m3);

		assertEquals(3, m1.size());
		assertEquals(9, m1.get(1));
		assertEquals(22, m1.get(2));
		assertEquals(5, m1.get(3));
	}

	@Test
	public void testDot() throws IOException {
		Int2IntOpenHashMapWritable m1 = new Int2IntOpenHashMapWritable();

		m1.put(1, 2);
		m1.put(2, 1);
		m1.put(3, 3);

		Int2IntOpenHashMapWritable m2 = new Int2IntOpenHashMapWritable();

		m2.put(1, 1);
		m2.put(2, 4);
		m2.put(4, 5);

		int s = m1.dot(m2);

		assertEquals(6, s);
	}

	@Test
	public void testSortedEntries1() {
		Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

		m.put(1, 5);
		m.put(2, 2);
		m.put(3, 3);
		m.put(4, 3);
		m.put(5, 1);

		Int2IntMap.Entry[] e = m.getEntriesSortedByValue();
		assertEquals(5, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5, e[0].getIntValue());

		assertEquals(3, e[1].getIntKey());
		assertEquals(3, e[1].getIntValue());

		assertEquals(4, e[2].getIntKey());
		assertEquals(3, e[2].getIntValue());

		assertEquals(2, e[3].getIntKey());
		assertEquals(2, e[3].getIntValue());

		assertEquals(5, e[4].getIntKey());
		assertEquals(1, e[4].getIntValue());
	}

	@Test
	public void testSortedEntries2() {
		Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

		m.put(1, 5);
		m.put(2, 2);
		m.put(3, 3);
		m.put(4, 3);
		m.put(5, 1);

		Int2IntMap.Entry[] e = m.getEntriesSortedByValue(2);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5, e[0].getIntValue());

		assertEquals(3, e[1].getIntKey());
		assertEquals(3, e[1].getIntValue());
	}

	@Test
	public void testSortedEntries3() {
		Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

		m.put(1, 5);
		m.put(2, 2);

		Int2IntMap.Entry[] e = m.getEntriesSortedByValue(5);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5, e[0].getIntValue());

		assertEquals(2, e[1].getIntKey());
		assertEquals(2, e[1].getIntValue());
	}

	@Test
	public void testSortedEntries4() {
		Int2IntOpenHashMapWritable m = new Int2IntOpenHashMapWritable();

		Int2IntMap.Entry[] e = m.getEntriesSortedByValue();
		assertTrue(e == null);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Int2IntOpenHashMapWritableTest.class);
	}
}
