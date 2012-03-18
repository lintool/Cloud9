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

import edu.umd.cloud9.util.map.HMapID;
import edu.umd.cloud9.util.map.MapID;
import edu.umd.cloud9.util.map.MapID.Entry;

public class HMapIDTest {

  @Test
  public void testBasic1() {
    int size = 100000;
    Random r = new Random();
    double[] doubles = new double[size];

    MapID map = new HMapID();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      map.put(i, k + 0.1);
      doubles[i] = k + 0.1;
    }

    for (int i = 0; i < size; i++) {
      double v = map.get(i);

      assertEquals(doubles[i], v, 0.0);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testUpdate() {
    int size = 100000;
    Random r = new Random();
    double[] doubles = new double[size];

    MapID map = new HMapID();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      map.put(i, k + 0.1);
      doubles[i] = k + 0.1;
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      map.put(i, doubles[i] + 1.0);
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      double v = map.get(i);

      assertEquals(doubles[i] + 1.0, v, 0.0);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testBasic() throws IOException {
    HMapID m = new HMapID();

    m.put(1, 5.0);
    m.put(2, 22.0);

    double value;

    assertEquals(m.size(), 2);

    value = m.get(1);
    assertTrue(value == 5.0);

    value = m.remove(1);
    assertEquals(m.size(), 1);

    value = m.get(2);
    assertTrue(value == 22.0);
  }

  @Test
  public void testPlus() throws IOException {
    HMapID m1 = new HMapID();

    m1.put(1, 5.0);
    m1.put(2, 22.0);

    HMapID m2 = new HMapID();

    m2.put(1, 4.0);
    m2.put(3, 5.0);

    m1.plus(m2);

    assertEquals(m1.size(), 3);
    assertTrue(m1.get(1) == 9);
    assertTrue(m1.get(2) == 22);
    assertTrue(m1.get(3) == 5);
  }

  @Test
  public void testDot() throws IOException {
    HMapID m1 = new HMapID();

    m1.put(1, 2.3);
    m1.put(2, 1.9);
    m1.put(3, 3.0);

    HMapID m2 = new HMapID();

    m2.put(1, 1.2);
    m2.put(2, 4.3);
    m2.put(4, 5.0);

    double s = m1.dot(m2);

    assertTrue(s == 10.93);
  }

  @Test
  public void testLengthAndNormalize() throws IOException {
    HMapID m1 = new HMapID();

    m1.put(1, 2.3);
    m1.put(2, 1.9);
    m1.put(3, 3.0);

    assertEquals(m1.length(), 4.2308393, 10E-6);

    m1.normalize();

    assertEquals(m1.get(1), 0.5436274, 10E-6);
    assertEquals(m1.get(2), 0.44908348, 10E-6);
    assertEquals(m1.get(3), 0.70907915, 10E-6);
    assertEquals(m1.length(), 1, 10E-6);

    HMapID m2 = new HMapID();

    m2.put(1, 1.2);
    m2.put(2, 4.3);
    m2.put(3, 5.0);

    assertEquals(m2.length(), 6.7029843, 10E-6);

    m2.normalize();

    assertEquals(m2.get(1), 0.17902474, 10E-6);
    assertEquals(m2.get(2), 0.64150536, 10E-6);
    assertEquals(m2.get(3), 0.7459364, 10E-6);
    assertEquals(m2.length(), 1, 10E-6);
  }

  @Test
  public void testSortedEntries1() {
    HMapID m = new HMapID();

    m.put(1, 5.0);
    m.put(2, 2.0);
    m.put(3, 3.0);
    m.put(4, 3.0);
    m.put(5, 1.0);

    Entry[] e = m.getEntriesSortedByValue();
    assertEquals(5, e.length);

    assertEquals(1, e[0].getKey());
    assertEquals(5.0, e[0].getValue(), 10E-6);

    assertEquals(3, e[1].getKey());
    assertEquals(3.0, e[1].getValue(), 10E-6);

    assertEquals(4, e[2].getKey());
    assertEquals(3.0, e[2].getValue(), 10E-6);

    assertEquals(2, e[3].getKey());
    assertEquals(2.0, e[3].getValue(), 10E-6);

    assertEquals(5, e[4].getKey());
    assertEquals(1.0, e[4].getValue(), 10E-6);
  }

  @Test
  public void testSortedEntries2() {
    HMapID m = new HMapID();

    m.put(1, 5.0);
    m.put(2, 2.0);
    m.put(3, 3.0);
    m.put(4, 3.0);
    m.put(5, 1.0);

    Entry[] e = m.getEntriesSortedByValue(2);

    assertEquals(2, e.length);

    assertEquals(1, e[0].getKey());
    assertEquals(5.0, e[0].getValue(), 10E-6);

    assertEquals(3, e[1].getKey());
    assertEquals(3.0, e[1].getValue(), 10E-6);
  }

  @Test
  public void testSortedEntries3() {
    HMapID m = new HMapID();

    m.put(1, 5.0);
    m.put(2, 2.0);

    Entry[] e = m.getEntriesSortedByValue(5);

    assertEquals(2, e.length);

    assertEquals(1, e[0].getKey());
    assertEquals(5.0, e[0].getValue(), 10E-6);

    assertEquals(2, e[1].getKey());
    assertEquals(2.0, e[1].getValue(), 10E-6);
  }

  @Test
  public void testSortedEntries4() {
    HMapID m = new HMapID();

    Entry[] e = m.getEntriesSortedByValue();
    assertTrue(e == null);
  }

  @Test
  public void testIncrement() {
    HMapID m = new HMapID();
    assertEquals(0.0, m.get(1), 10E-6);

    m.increment(1, 0.5);
    assertEquals(0.5, m.get(1), 10E-6);

    m.increment(1, 1.0);
    m.increment(2, 0.0);
    m.increment(3, -0.5);

    assertEquals(1.5, m.get(1), 10E-6);
    assertEquals(0.0, m.get(2), 10E-6);
    assertEquals(-0.5, m.get(3), 10E-6);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(HMapIDTest.class);
  }
}