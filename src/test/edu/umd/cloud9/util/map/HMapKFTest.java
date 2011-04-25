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

import org.apache.hadoop.io.Text;
import org.junit.Test;

import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.MapKF;

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
	
	@Test
	public void testBasic() throws IOException {
		HMapKF<Text> m = new HMapKF<Text>();

		m.put(new Text("hi"), 5.0f);
		m.put(new Text("there"), 22.0f);

		Text key;
		float value;

		assertEquals(m.size(), 2);

		key = new Text("hi");
		value = m.get(key);
		assertTrue(value == 5.0f);

		value = m.remove(key);
		assertEquals(m.size(), 1);

		key = new Text("there");
		value = m.get(key);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testPlus() throws IOException {
		HMapKF<Text> m1 = new HMapKF<Text>();

		m1.put(new Text("hi"), 5.0f);
		m1.put(new Text("there"), 22.0f);

		HMapKF<Text> m2 = new HMapKF<Text>();

		m2.put(new Text("hi"), 4.0f);
		m2.put(new Text("test"), 5.0f);

		m1.plus(m2);

		assertEquals(m1.size(), 3);
		assertTrue(m1.get(new Text("hi")) == 9);
		assertTrue(m1.get(new Text("there")) == 22);
		assertTrue(m1.get(new Text("test")) == 5);
	}

	@Test
	public void testDot() throws IOException {
		HMapKF<Text> m1 = new HMapKF<Text>();

		m1.put(new Text("hi"), 2.3f);
		m1.put(new Text("there"), 1.9f);
		m1.put(new Text("empty"), 3.0f);

		HMapKF<Text> m2 = new HMapKF<Text>();

		m2.put(new Text("hi"), 1.2f);
		m2.put(new Text("there"), 4.3f);
		m2.put(new Text("test"), 5.0f);

		float s = m1.dot(m2);

		assertTrue(s == 10.93f);
	}

	@Test
	public void testLengthAndNormalize() throws IOException {
		HMapKF<Text> m1 = new HMapKF<Text>();

		m1.put(new Text("hi"), 2.3f);
		m1.put(new Text("there"), 1.9f);
		m1.put(new Text("empty"), 3.0f);

		assertEquals(m1.length(), 4.2308393, 10E-6);

		m1.normalize();

		assertEquals(m1.get(new Text("hi")), 0.5436274, 10E-6);
		assertEquals(m1.get(new Text("there")), 0.44908348, 10E-6);
		assertEquals(m1.get(new Text("empty")), 0.70907915, 10E-6);
		assertEquals(m1.length(), 1, 10E-6);

		HMapKF<Text> m2 = new HMapKF<Text>();

		m2.put(new Text("hi"), 1.2f);
		m2.put(new Text("there"), 4.3f);
		m2.put(new Text("test"), 5.0f);

		assertEquals(m2.length(), 6.7029843, 10E-6);

		m2.normalize();

		assertEquals(m2.get(new Text("hi")), 0.17902474, 10E-6);
		assertEquals(m2.get(new Text("there")), 0.64150536, 10E-6);
		assertEquals(m2.get(new Text("test")), 0.7459364, 10E-6);
		assertEquals(m2.length(), 1, 10E-6);
	}

	@Test
	public void testSortedEntries1() {
		HMapKF<Text> m = new HMapKF<Text>();

		m.put(new Text("a"), 5.0f);
		m.put(new Text("b"), 2.0f);
		m.put(new Text("c"), 3.0f);
		m.put(new Text("d"), 3.0f);
		m.put(new Text("e"), 1.0f);

		MapKF.Entry<Text>[] entries = m.getEntriesSortedByValue();
		MapKF.Entry<Text> e = null;

		assertEquals(5, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5.0f, e.getValue(), 10E-6);

		e = entries[1];
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3.0f, e.getValue(), 10E-6);

		e = entries[2];
		assertEquals(new Text("d"), e.getKey());
		assertEquals(3.0f, e.getValue(), 10E-6);

		e = entries[3];
		assertEquals(new Text("b"), e.getKey());
		assertEquals(2.0f, e.getValue(), 10E-6);

		e = entries[4];
		assertEquals(new Text("e"), e.getKey());
		assertEquals(1.0f, e.getValue(), 10E-6);

	}

	@Test
	public void testSortedEntries2() {
		HMapKF<Text> m = new HMapKF<Text>();

		m.put(new Text("a"), 5.0f);
		m.put(new Text("b"), 2.0f);
		m.put(new Text("c"), 3.0f);
		m.put(new Text("d"), 3.0f);
		m.put(new Text("e"), 1.0f);

		MapKF.Entry<Text>[] entries = m.getEntriesSortedByValue(2);
		MapKF.Entry<Text> e = null;

		assertEquals(2, entries.length);

		e = entries[0];
		assertEquals(new Text("a"), e.getKey());
		assertEquals(5.0f, e.getValue(), 10E-6);

		e = entries[1];
		assertEquals(new Text("c"), e.getKey());
		assertEquals(3.0f, e.getValue(), 10E-6);
	}

  @Test
  public void testIncrement() {
    HMapKF<String> m = new HMapKF<String>();
    assertEquals(0.0f, m.get("one"), 10E-6);

    m.increment("one", 0.5f);
    assertEquals(0.5f, m.get("one"), 10E-6);

    m.increment("one", 1.0f);
    m.increment("two", 0.0f);
    m.increment("three", -0.5f);
    
    assertEquals(1.5f, m.get("one"), 10E-6);
    assertEquals(0.0f, m.get("two"), 10E-6);
    assertEquals(-0.5f, m.get("three"), 10E-6);
  }

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapKFTest.class);
	}

}