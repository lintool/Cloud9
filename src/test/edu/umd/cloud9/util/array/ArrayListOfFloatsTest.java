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

package edu.umd.cloud9.util.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.array.ArrayListOfFloats;

public class ArrayListOfFloatsTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		ArrayListOfFloats list = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat();
			list.add(k);
			floats[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(floats[i], list.get(i), 10e-5);
		}
	}

  @Test
  public void testArrayConstructor() {
    float[] arr = new float[] { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
    assertEquals(5, arr.length);

    ArrayListOfFloats list = new ArrayListOfFloats(arr);
    list.remove(2);

    // Make sure the original array remains untouched.
    assertEquals(1.0f, arr[0], 10e-6);
    assertEquals(2.0f, arr[1], 10e-6);
    assertEquals(3.0f, arr[2], 10e-6);
    assertEquals(4.0f, arr[3], 10e-6);
    assertEquals(5.0f, arr[4], 10e-6);
  }

	@Test
	public void testRemove() {
		ArrayListOfFloats list = new ArrayListOfFloats();
		for ( int i=0; i<10; i++) {
			list.add((float) i);
		}

		list.remove(list.indexOf(5.0f));
		assertEquals(9, list.size());
		assertEquals(0.0f, list.get(0), 10e-6);
		assertEquals(1.0f, list.get(1), 10e-6);
		assertEquals(2.0f, list.get(2), 10e-6);
		assertEquals(3.0f, list.get(3), 10e-6);
		assertEquals(4.0f, list.get(4), 10e-6);
		assertEquals(6.0f, list.get(5), 10e-6);
		assertEquals(7.0f, list.get(6), 10e-6);
		assertEquals(8.0f, list.get(7), 10e-6);
		assertEquals(9.0f, list.get(8), 10e-6);

		list.remove(list.indexOf((short) 9));
		assertEquals(8, list.size);
		assertEquals(0.0f, list.get(0), 10e-6);
		assertEquals(1.0f, list.get(1), 10e-6);
		assertEquals(2.0f, list.get(2), 10e-6);
		assertEquals(3.0f, list.get(3), 10e-6);
		assertEquals(4.0f, list.get(4), 10e-6);
		assertEquals(6.0f, list.get(5), 10e-6);
		assertEquals(7.0f, list.get(6), 10e-6);
		assertEquals(8.0f, list.get(7), 10e-6);
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		ArrayListOfFloats list = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat();
			list.add(k);
			floats[i] = k;
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			list.set(i, floats[i] + 1);
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			assertEquals(floats[i] + 1, list.get(i), 10e-5);
		}

	}

	@Test
	public void testTrim1() {
		int size = 89;
		Random r = new Random();
		float[] floats = new float[size];

		ArrayListOfFloats list = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat();
			list.add(k);
			floats[i] = k;
		}

		for (int i = 0; i < size; i++) {
			assertEquals(floats[i], list.get(i), 10e-5);
		}

		float[] rawArray = list.getArray();
		int lenBefore = rawArray.length;

		list.trimToSize();
		float[] rawArrayAfter = list.getArray();
		int lenAfter = rawArrayAfter.length;

		assertEquals(89, lenAfter);
		assertTrue(lenBefore > lenAfter);
	}

	@Test
	public void testClone() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		ArrayListOfFloats list1 = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat();
			list1.add(k);
			floats[i] = k;
		}

		ArrayListOfFloats list2 = list1.clone();

		assertEquals(size, list1.size());
		assertEquals(size, list2.size());

		for (int i = 0; i < size; i++) {
			list2.set(i, floats[i] + 1);
		}

		// values in old list should not have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(floats[i], list1.get(i), 10e-5);
		}

		// however, values in new list should have changed
		assertEquals(size, list1.size());
		for (int i = 0; i < size; i++) {
			assertEquals(floats[i] + 1, list2.get(i), 10e-5);
		}
	}

	@Test
	public void testToString() {
		int size = 10;
		Random r = new Random();

		ArrayListOfFloats list = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			list.add(r.nextFloat());
		}

		String out = list.toString();
		for (int i = 0; i < size; i++) {
			float v = list.get(i);

			// Make sure the first 10 elements are printed out.
			assertTrue(out.indexOf(new Float(v).toString()) != -1);
		}

		for (int i = 0; i < size; i++) {
			list.add(r.nextFloat());
		}

		out = list.toString();
		for (int i = size; i < size+size; i++) {
			float v = list.get(i);

			// Make sure these elements are not printed out.
			assertTrue(out.indexOf(new Float(v).toString()) == -1);
		}

		assertTrue(out.indexOf(size + " more") != -1);
	}

	@Test
	public void testIterable() {
		int size = 1000;
		Random r = new Random();
		float[] floats = new float[size];

		ArrayListOfFloats list = new ArrayListOfFloats();
		for (int i = 0; i < size; i++) {
			float k = r.nextFloat();
			list.add(k);
			floats[i] = k;
		}

		int i=0;
		for ( Float v : list) {
			assertEquals(floats[i++], v, 10e-5);
		}
	}

	@Test
	public void testSetSize() {
		ArrayListOfFloats list = new ArrayListOfFloats();

		list.add(5.0f);
		assertEquals(1, list.size);
		assertEquals(5.0f, list.get(0), 10e-6);

		list.setSize(5);
		assertEquals(5, list.size);
		assertEquals(0.0f, list.get(1), 10e-6);
		assertEquals(0.0f, list.get(2), 10e-6);
		assertEquals(0.0f, list.get(3), 10e-6);
		assertEquals(0.0f, list.get(4), 10e-6);

		list.add(12.0f);
		assertEquals(6, list.size);
		assertEquals(12.0f, list.get(5), 10e-6);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfFloatsTest.class);
	}
}