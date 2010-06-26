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

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfFloatsTest.class);
	}

}