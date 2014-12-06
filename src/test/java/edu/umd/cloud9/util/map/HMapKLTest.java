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

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.util.map.HMapKL;
import edu.umd.cloud9.util.map.MapKL;

public class HMapKLTest {

  @Test
  public void testBasic1() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    MapKL<Integer> map = new HMapKL<Integer>();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      map.put(i, k);
      longs[i] = k;
    }

    for (int i = 0; i < size; i++) {
      long v = map.get(i);

      assertEquals(longs[i], v);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testBasic2() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];
    String[] strings = new String[size];

    MapKL<String> map = new HMapKL<String>();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      String s = new Integer(k).toString();
      map.put(s, k);
      longs[i] = k;
      strings[i] = s;
    }

    for (int i = 0; i < size; i++) {
      long v = map.get(strings[i]);

      assertEquals(longs[i], v);
      assertTrue(map.containsKey(strings[i]));
    }
  }

  @Test
  public void testUpdate() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    MapKL<Integer> map = new HMapKL<Integer>();
    for (int i = 0; i < size; i++) {
      int k = r.nextInt(size);
      map.put(i, k);
      longs[i] = k;
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      map.put(i, longs[i] + 1);
    }

    assertEquals(size, map.size());

    for (int i = 0; i < size; i++) {
      long v = map.get(i);

      assertEquals(longs[i] + 1, v);
      assertTrue(map.containsKey(i));
    }
  }

  @Test
  public void testIncrement() {
    HMapKL<String> m = new HMapKL<String>();
    assertEquals(0, m.get("one"));

    m.increment("one", 1);
    assertEquals(1, m.get("one"));

    m.increment("one", 1);
    m.increment("two", 0);
    m.increment("three", -1);

    assertEquals(2, m.get("one"));
    assertEquals(0, m.get("two"));
    assertEquals(-1, m.get("three"));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(HMapKLTest.class);
  }
}