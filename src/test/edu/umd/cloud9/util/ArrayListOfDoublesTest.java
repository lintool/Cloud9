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

public class ArrayListOfDoublesTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		double[] doubles = new double[size];

		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			double k = r.nextDouble();
			list.add(k);
			doubles[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(doubles[i], list.get(i), 10e-5);
		}
	}

	@Test
	public void testRemove() {
		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for ( int i=0; i<10; i++) {
			list.add((double) i);
		}

		list.remove(list.indexOf(5.0));
		assertEquals(9, list.size());
		assertEquals(0.0, list.get(0), 10e-6);
		assertEquals(1.0, list.get(1), 10e-6);
		assertEquals(2.0, list.get(2), 10e-6);
		assertEquals(3.0, list.get(3), 10e-6);
		assertEquals(4.0, list.get(4), 10e-6);
		assertEquals(6.0, list.get(5), 10e-6);
		assertEquals(7.0, list.get(6), 10e-6);
		assertEquals(8.0, list.get(7), 10e-6);
		assertEquals(9.0, list.get(8), 10e-6);

		list.remove(list.indexOf((short) 9));
		assertEquals(8, list.size);
		assertEquals(0.0, list.get(0), 10e-6);
		assertEquals(1.0, list.get(1), 10e-6);
		assertEquals(2.0, list.get(2), 10e-6);
		assertEquals(3.0, list.get(3), 10e-6);
		assertEquals(4.0, list.get(4), 10e-6);
		assertEquals(6.0, list.get(5), 10e-6);
		assertEquals(7.0, list.get(6), 10e-6);
		assertEquals(8.0, list.get(7), 10e-6);
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		double[] doubles = new double[size];

		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			double k = r.nextDouble();
			list.add(k);
			doubles[i] = k;
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			list.set(i, doubles[i] + 1);
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			assertEquals(doubles[i] + 1, list.get(i), 10e-5);
		}
	}

	@Test
	public void testTrim1() {
		int size = 89;
		Random r = new Random();
		double[] doubles = new double[size];

		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			double k = r.nextDouble();
			list.add(k);
			doubles[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(doubles[i], list.get(i), 10e-5);
		}

		double[] rawArray = list.getArray();
		int lenBefore = rawArray.length;

		list.trimToSize();
		double[] rawArrayAfter = list.getArray();
		int lenAfter = rawArrayAfter.length;

		assertEquals(89, lenAfter);
		assertTrue(lenBefore > lenAfter);
	}

	@Test
	public void testClone() {
		int size = 100000;
		Random r = new Random();
		double[] doubles = new double[size];

		ArrayListOfDoubles list1 = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			double k = r.nextDouble();
			list1.add(k);
			doubles[i] = k;
		}

		ArrayListOfDoubles list2 = list1.clone();

		assertEquals(size, list1.size());
		assertEquals(size, list2.size());

		for (int i = 0; i < size; i++) {
			list2.set(i, doubles[i] + 1);
		}

		// values in old list should not have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(doubles[i], list1.get(i), 10e-5);
		}

		// however, values in new list should have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(doubles[i] + 1, list2.get(i), 10e-5);
		}
	}

	@Test
	public void testToString() {
		int size = 10;
		Random r = new Random();

		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			list.add(r.nextDouble());
		}

		String out = list.toString();
		for (int i = 0; i < size; i++) {
			double v = list.get(i);

			// Make sure the first 10 elements are printed out.
			assertTrue(out.indexOf(new Double(v).toString()) != -1);
		}

		for (int i = 0; i < size; i++) {
			list.add(r.nextFloat());
		}

		out = list.toString();
		for (int i = size; i < size+size; i++) {
			double v = list.get(i);

			// Make sure these elements are not printed out.
			assertTrue(out.indexOf(new Double(v).toString()) == -1);
		}

		assertTrue(out.indexOf(size + " more") != -1);
	}

	@Test
	public void testIterable() {
		int size = 1000;
		Random r = new Random();
		double[] doubles = new double[size];

		ArrayListOfDoubles list = new ArrayListOfDoubles();
		for (int i = 0; i < size; i++) {
			double k = r.nextDouble();
			list.add(k);
			doubles[i] = k;
		}

		int i=0;
		for ( Double v : list) {
			assertEquals(doubles[i++], v, 10e-5);
		}

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfDoublesTest.class);
	}

}