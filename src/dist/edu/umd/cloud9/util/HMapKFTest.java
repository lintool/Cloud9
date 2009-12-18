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

public class HMapKFTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		MapKF<Integer> map = new HMapKF<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k + 0.1f);
			floats[i] = k + 0.1f;
		}

		for (int i = 0; i < size; i++) {
			float v = map.get(i);

			assertEquals(floats[i], v, 0.0f);
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testBasic2() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];
		String[] strings = new String[size];

		MapKF<String> map = new HMapKF<String>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			String s = new Integer(k).toString();
			map.put(s, k + 0.1f);
			floats[i] = k + 0.1f;
			strings[i] = s;
		}

		for (int i = 0; i < size; i++) {
			float v = map.get(strings[i]);

			assertEquals(floats[i], v, 0.0f);
			assertTrue(map.containsKey(strings[i]));
		}
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		MapKF<Integer> map = new HMapKF<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k + 0.1f);
			floats[i] = k + 0.1f;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, floats[i] + 1.0f);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			float v = map.get(i);

			assertEquals(floats[i] + 1.0f, v, 0.0f);
			assertTrue(map.containsKey(i));
		}

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapKFTest.class);
	}

}