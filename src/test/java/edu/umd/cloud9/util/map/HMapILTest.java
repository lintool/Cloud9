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

import edu.umd.cloud9.util.map.HMapIL;
import edu.umd.cloud9.util.map.MapIL;
import edu.umd.cloud9.util.map.MapIL.Entry;

public class HMapILTest {

  @Test
  public void testBasic1() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    MapIL map = new HMapIL();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      map.put(i, k * 2);
      longs[i] = k * 2;
    }

    for (int i = 0; i < size; i++) {
      long v = map.get(i);

      assertEquals(longs[i], v);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testUpdate() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    MapIL map = new HMapIL();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      map.put(i, k + 10L);
      longs[i] = k + 10L;
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      map.put(i, longs[i] + 10L);
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      long v = map.get(i);

      assertEquals(longs[i] + 10L, v);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testBasic() throws IOException {
    HMapIL m = new HMapIL();

    m.put(1, 5L);
    m.put(2, 22L);

    long value;

    assertEquals(2, m.size());

    value = m.get(1);
    assertEquals(5L, value);

    value = m.remove(1);
    assertEquals(m.size(), 1);

    value = m.get(2);
    assertEquals(22L, value);
  }

  @Test
  public void testPlus() throws IOException {
    HMapIL m1 = new HMapIL();

    m1.put(1, 5L);
    m1.put(2, 22L);

    HMapIL m2 = new HMapIL();

    m2.put(1, 4L);
    m2.put(3, 5L);

    m1.plus(m2);

    assertEquals(m1.size(), 3);
    assertTrue(m1.get(1) == 9L);
    assertTrue(m1.get(2) == 22L);
    assertTrue(m1.get(3) == 5L);
  }

  @Test
  public void testDot() throws IOException {
    HMapIL m1 = new HMapIL();

    m1.put(1, 2L);
    m1.put(2, 1L);
    m1.put(3, 3L);

    HMapIL m2 = new HMapIL();

    m2.put(1, 1L);
    m2.put(2, 4L);
    m2.put(4, 5L);

    long s = m1.dot(m2);

    assertEquals(6L, s);
  }

  @Test
  public void testSortedEntries1() {
    HMapIL m = new HMapIL();

    m.put(1, 5L);
    m.put(2, 2L);
    m.put(3, 3L);
    m.put(4, 3L);
    m.put(5, 1L);

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
    HMapIL m = new HMapIL();

    m.put(1, 5L);
    m.put(2, 2L);
    m.put(3, 3L);
    m.put(4, 3L);
    m.put(5, 1L);

    Entry[] e = m.getEntriesSortedByValue(2);

    assertEquals(2, e.length);

    assertEquals(1, e[0].getKey());
    assertEquals(5L, e[0].getValue());

    assertEquals(3, e[1].getKey());
    assertEquals(3L, e[1].getValue());
  }

  @Test
  public void testSortedEntries3() {
    HMapIL m = new HMapIL();

    m.put(1, 5L);
    m.put(2, 2L);

    Entry[] e = m.getEntriesSortedByValue(5);

    assertEquals(2, e.length);

    assertEquals(1, e[0].getKey());
    assertEquals(5L, e[0].getValue());

    assertEquals(2, e[1].getKey());
    assertEquals(2L, e[1].getValue());
  }

  @Test
  public void testSortedEntries4() {
    HMapIL m = new HMapIL();

    Entry[] e = m.getEntriesSortedByValue();
    assertTrue(e == null);
  }

  @Test
  public void testIncrement() {
    HMapIL m = new HMapIL();
    assertEquals(0, m.get(1));

    m.increment(1, 1);
    assertEquals(1, m.get(1));

    m.increment(1, 1);
    m.increment(2, 0);
    m.increment(3, -1);

    assertEquals(2, m.get(1));
    assertEquals(0, m.get(2));
    assertEquals(-1, m.get(3));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(HMapILTest.class);
  }
}