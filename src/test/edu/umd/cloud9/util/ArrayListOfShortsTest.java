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

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class ArrayListOfShortsTest {

	@Test
	public void testBasic1() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i], v);
		}
	}

	@Test
	public void testRemove() {
		ArrayListOfShorts list = new ArrayListOfShorts();
		for ( int i=0; i<10; i++) {
			list.add((short) i);
		}

		list.remove(list.indexOf((short) 5));
		assertEquals(9, list.size());
		assertEquals(0, list.get(0));
		assertEquals(1, list.get(1));
		assertEquals(2, list.get(2));
		assertEquals(3, list.get(3));
		assertEquals(4, list.get(4));
		assertEquals(6, list.get(5));
		assertEquals(7, list.get(6));
		assertEquals(8, list.get(7));
		assertEquals(9, list.get(8));

		list.remove(list.indexOf((short) 9));
		assertEquals(8, list.size);
		assertEquals(0, list.get(0));
		assertEquals(1, list.get(1));
		assertEquals(2, list.get(2));
		assertEquals(3, list.get(3));
		assertEquals(4, list.get(4));
		assertEquals(6, list.get(5));
		assertEquals(7, list.get(6));
		assertEquals(8, list.get(7));
	}

	@Test
	public void testUpdate() {
		int size = 10000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			list.set(i, (short) (shorts[i] + 1));
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i] + 1, v);
		}

	}

	@Test
	public void testTrim1() {
		int size = 89;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(shorts[i], v);
		}

		short[] rawArray = list.getArray();
		int lenBefore = rawArray.length;

		list.trimToSize();
		short[] rawArrayAfter = list.getArray();
		int lenAfter = rawArrayAfter.length;

		assertEquals(89, lenAfter);
		assertTrue(lenBefore > lenAfter);
	}

	@Test
	public void testClone() {
		int size = 10000;
		Random r = new Random();
		int[] shorts = new int[size];

		ArrayListOfShorts list1 = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list1.add(k);
			shorts[i] = k;
		}

		ArrayListOfShorts list2 = list1.clone();

		assertEquals(size, list1.size());
		assertEquals(size, list2.size());

		for (int i = 0; i < size; i++) {
			list2.set(i, (short) (shorts[i] + 1));
		}

		// values in old list should not have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(shorts[i], list1.get(i));
		}

		// however, values in new list should have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(shorts[i] + 1, list2.get(i));
		}
	}

	@Test
	public void testToString() {
		int size = 10;
		Random r = new Random();

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			list.add((short) r.nextInt(30000));
		}

		String out = list.toString();
		for (int i = 0; i < size; i++) {
			short v = list.get(i);

			// Make sure the first 10 elements are printed out.
			assertTrue(out.indexOf(new Short(v).toString()) != -1);
		}

		for (int i = 0; i < size; i++) {
			list.add((short) r.nextInt(30000));
		}

		out = list.toString();
		for (int i = size; i < size+size; i++) {
			short v = list.get(i);

			// Make sure these elements are not printed out.
			assertTrue(out.indexOf(new Short(v).toString()) == -1);
		}

		assertTrue(out.indexOf(size + " more") != -1);
	}

	@Test
	public void testIterable() {
		int size = 1000;
		Random r = new Random();
		short[] shorts = new short[size];

		ArrayListOfShorts list = new ArrayListOfShorts();
		for (int i = 0; i < size; i++) {
			short k = (short) r.nextInt(size);
			list.add(k);
			shorts[i] = k;
		}

		int i=0;
		for ( Short v : list) {
			assertEquals(shorts[i++], (short) v);
		}

	}

	@Test
	public void testSetSize() {
		ArrayListOfShorts list = new ArrayListOfShorts();

		list.add((short) 5);
		assertEquals(1, list.size);
		assertEquals(5, list.get(0));

		list.setSize(5);
		assertEquals(5, list.size);
		assertEquals(0, list.get(1));
		assertEquals(0, list.get(2));
		assertEquals(0, list.get(3));
		assertEquals(0, list.get(4));

		list.add((short) 12);
		assertEquals(6, list.size);
		assertEquals(12, list.get(5));
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfShortsTest.class);
	}
}