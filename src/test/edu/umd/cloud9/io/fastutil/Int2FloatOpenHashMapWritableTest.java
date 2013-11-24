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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;

import java.io.IOException;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.map.Int2FloatOpenHashMapWritable;

public class Int2FloatOpenHashMapWritableTest {

	@Test
	public void testBasic() throws IOException {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

		m.put(2, 5.0f);
		m.put(1, 22.0f);

		float value;

		assertEquals(2, m.size());

		value = m.get(2);
		assertEquals(5.0f, value, 10e-6);

		value = m.remove(2);
		assertEquals(1, m.size());

		value = m.get(1);
		assertEquals(22.0f, value, 10e-6);
	}

  @Test
  public void testIncrement() throws IOException {
    Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

    m.put(2, 7.0f);
    m.put(1, 29.0f);

    assertEquals(7, m.get(2), 10e-6);
    assertEquals(29, m.get(1), 10e-6);

    m.increment(2);
    m.increment(1);
    m.increment(3);

    assertEquals(8, m.get(2), 10e-6);
    assertEquals(30, m.get(1), 10e-6);
    assertEquals(1, m.get(3), 10e-6);

    m.increment(1, 3.0f);
    m.increment(3, 5.0f);

    assertEquals(8, m.get(2), 10e-6);
    assertEquals(33, m.get(1), 10e-6);
    assertEquals(6, m.get(3), 10e-6);
  }

	@Test
	public void testSerialize1() throws IOException {
		Int2FloatOpenHashMapWritable.setLazyDecodeFlag(false);
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		Int2FloatOpenHashMapWritable n2 = Int2FloatOpenHashMapWritable.create(m1.serialize());

		float value;

		assertEquals(2, n2.size());

		value = n2.get(3);
		assertEquals(5.0f, value, 10e-6);

		value = n2.remove(3);
		assertEquals(1, n2.size());

		value = n2.get(4);
		assertEquals(22.0f, value, 10e-6);
	}

	@Test
	public void testSerializeLazy1() throws IOException {
		Int2FloatOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(3, 5);
		m1.put(4, 22);

		Int2FloatOpenHashMapWritable m2 = Int2FloatOpenHashMapWritable.create(m1.serialize());

		assertEquals(0, m2.size());
		assertFalse(m2.hasBeenDecoded());

		int[] keys = m2.getKeys();
		float[] values = m2.getValues();

		assertEquals(3, keys[0]);
		assertEquals(4, keys[1]);

		assertEquals(5.0f, values[0], 10e-6);
		assertEquals(22.0f, values[1], 10e-6);

		m2.decode();
		assertTrue(m2.hasBeenDecoded());

		float value;
		assertEquals(2, m2.size());

		value = m2.get(3);
		assertEquals(5.0f, value, 10e-6);

		value = m2.remove(3);
		assertEquals(1, m2.size());

		value = m2.get(4);
		assertEquals(22.0f, value, 10e-6);
	}

	@Test
	public void testSerializeLazy2() throws IOException {
		Int2FloatOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(3, 5);
		m1.put(4, 22);

		// Object m2 should not have been decoded, size lazy decode flag is
		// true.
		Int2FloatOpenHashMapWritable m2 = Int2FloatOpenHashMapWritable.create(m1.serialize());

		// Even though m2 hasn't be decoded, we should be able to properly
		// serialize it.
		Int2FloatOpenHashMapWritable m3 = Int2FloatOpenHashMapWritable.create(m2.serialize());

		assertEquals(0, m3.size());
		assertFalse(m3.hasBeenDecoded());

		int[] keys = m3.getKeys();
		float[] values = m3.getValues();

		assertEquals(3, keys[0]);
		assertEquals(4, keys[1]);

		assertEquals(5.0f, values[0], 10e-6);
		assertEquals(22.0f, values[1], 10e-6);

		m3.decode();
		assertTrue(m3.hasBeenDecoded());

		float value;
		assertEquals(2, m3.size());

		value = m3.get(3);
		assertEquals(5.0f, value, 10e-6);

		value = m3.remove(3);
		assertEquals(1, m3.size());

		value = m3.get(4);
		assertEquals(22.0f, value, 10e-6);
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		// Make sure this does nothing.
		m1.decode();

		assertEquals(0, m1.size());

		Int2FloatMap m2 = Int2FloatOpenHashMapWritable.create(m1.serialize());

		assertEquals(0, m2.size());
	}

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		Int2FloatMap map = new Int2FloatOpenHashMapWritable();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat() * size;
			map.put(i, k);
			floats[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(floats[i], map.get(i), 10e-6);
			assertTrue(map.containsKey(i));
		}

	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		Int2FloatMap map = new Int2FloatOpenHashMapWritable();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat() * size;
			map.put(i, k);
			floats[i] = k;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, floats[i] + 1.0f);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			assertEquals(floats[i] + 1.0f, map.get(i), 10e-6);
			assertTrue(map.containsKey(i));
		}

	}

	@Test
	public void testPlus() throws IOException {
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(1, 5.0f);
		m1.put(2, 22.0f);

		Int2FloatOpenHashMapWritable m2 = new Int2FloatOpenHashMapWritable();

		m2.put(1, 4.0f);
		m2.put(3, 5.0f);

		m1.plus(m2);

		assertEquals(3, m1.size(), 3);
		assertEquals(9.0f, m1.get(1), 10e-6);
		assertEquals(22.0f, m1.get(2), 10e-6);
		assertEquals(5.0f, m1.get(3), 10e-6);
	}

	@Test
	public void testLazyPlus() throws IOException {
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(1, 5.0f);
		m1.put(2, 22.0f);

		Int2FloatOpenHashMapWritable m2 = new Int2FloatOpenHashMapWritable();

		m2.put(1, 4.0f);
		m2.put(3, 5.0f);

		Int2FloatOpenHashMapWritable.setLazyDecodeFlag(true);
		Int2FloatOpenHashMapWritable m3 = Int2FloatOpenHashMapWritable.create(m2.serialize());

		assertEquals(0, m3.size());

		m1.lazyplus(m3);

		assertEquals(3, m1.size(), 3);
		assertEquals(9.0f, m1.get(1), 10e-6);
		assertEquals(22.0f, m1.get(2), 10e-6);
		assertEquals(5.0f, m1.get(3), 10e-6);
	}

	@Test
	public void testDot() throws IOException {
		Int2FloatOpenHashMapWritable m1 = new Int2FloatOpenHashMapWritable();

		m1.put(1, 2.0f);
		m1.put(2, 1.0f);
		m1.put(3, 3.0f);

		Int2FloatOpenHashMapWritable m2 = new Int2FloatOpenHashMapWritable();

		m2.put(1, 1.0f);
		m2.put(2, 4.0f);
		m2.put(4, 5.0f);

		float s = m1.dot(m2);

		assertEquals(6.0f, s, 10e-6);
	}

	@Test
	public void testSortedEntries1() {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

		m.put(1, 5.0f);
		m.put(2, 2.0f);
		m.put(3, 3.0f);
		m.put(4, 3.0f);
		m.put(5, 1.0f);

		Int2FloatMap.Entry[] e = m.getEntriesSortedByValue();
		assertEquals(5, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5.0f, e[0].getFloatValue(), 10e-6);

		assertEquals(3, e[1].getIntKey());
		assertEquals(3.0f, e[1].getFloatValue(), 10e-6);

		assertEquals(4, e[2].getIntKey());
		assertEquals(3.0f, e[2].getFloatValue(), 10e-6);

		assertEquals(2, e[3].getIntKey());
		assertEquals(2.0f, e[3].getFloatValue(), 10e-6);

		assertEquals(5, e[4].getIntKey());
		assertEquals(1.0f, e[4].getFloatValue(), 10e-6);
	}

	@Test
	public void testSortedEntries2() {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

		m.put(1, 5.0f);
		m.put(2, 2.0f);
		m.put(3, 3.0f);
		m.put(4, 3.0f);
		m.put(5, 1.0f);

		Int2FloatMap.Entry[] e = m.getEntriesSortedByValue(2);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5.0f, e[0].getFloatValue(), 10e-6);

		assertEquals(3, e[1].getIntKey());
		assertEquals(3.0f, e[1].getFloatValue(), 10e-6);
	}

	@Test
	public void testSortedEntries3() {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

		m.put(1, 5.0f);
		m.put(2, 2.0f);

		Int2FloatMap.Entry[] e = m.getEntriesSortedByValue(5);

		assertEquals(2, e.length);

		assertEquals(1, e[0].getIntKey());
		assertEquals(5.0f, e[0].getFloatValue(), 10e-6);

		assertEquals(2, e[1].getIntKey());
		assertEquals(2.0f, e[1].getFloatValue(), 10e-6);
	}

	@Test
	public void testSortedEntries4() {
		Int2FloatOpenHashMapWritable m = new Int2FloatOpenHashMapWritable();

		Int2FloatMap.Entry[] e = m.getEntriesSortedByValue();
		assertTrue(e == null);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Int2FloatOpenHashMapWritableTest.class);
	}
}
