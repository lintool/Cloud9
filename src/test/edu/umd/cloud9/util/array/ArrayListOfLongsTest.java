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

public class ArrayListOfLongsTest {

  @Test
  public void testBasic1() {
    int size = 100000;
    Random r = new Random();
    long[] ints = new long[size];

    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      list.add(k);
      ints[i] = k;
    }

    for (int i = 0; i < size; i++) {
      long v = list.get(i);

      assertEquals(ints[i], v);
    }
  }

  @Test
  public void testArrayConstructor() {
    long[] arr = new long[] { 1, 2, 3, 4, 5 };
    assertEquals(5, arr.length);

    ArrayListOfLongs list = new ArrayListOfLongs(arr);
    list.remove(2);

    // Make sure the original array remains untouched.
    assertEquals(1, arr[0]);
    assertEquals(2, arr[1]);
    assertEquals(3, arr[2]);
    assertEquals(4, arr[3]);
    assertEquals(5, arr[4]);
  }

  @Test
  public void testRemove() {
    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < 10; i++) {
      list.add(i);
    }

    list.remove(list.indexOf(5));
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

    list.remove(list.indexOf(9));
    assertEquals(8, list.size());
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
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      list.add(k);
      longs[i] = k;
    }

    assertEquals(size, list.size());

    for (int i = 0; i < size; i++) {
      list.set(i, longs[i] + 1);
    }

    assertEquals(size, list.size());

    for (int i = 0; i < size; i++) {
      long v = list.get(i);

      assertEquals(longs[i] + 1, v);
    }
  }

  @Test
  public void testTrim1() {
    int size = 89;
    Random r = new Random();
    long[] longs = new long[size];

    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      list.add(k);
      longs[i] = k;
    }

    for (int i = 0; i < size; i++) {
      long v = list.get(i);

      assertEquals(longs[i], v);
    }

    long[] rawArray = list.getArray();
    int lenBefore = rawArray.length;

    list.trimToSize();
    long[] rawArrayAfter = list.getArray();
    int lenAfter = rawArrayAfter.length;

    assertEquals(89, lenAfter);
    assertTrue(lenBefore > lenAfter);
  }

  @Test
  public void testClone() {
    int size = 100000;
    Random r = new Random();
    long[] longs = new long[size];

    ArrayListOfLongs list1 = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      list1.add(k);
      longs[i] = k;
    }

    ArrayListOfLongs list2 = list1.clone();

    assertEquals(size, list1.size());
    assertEquals(size, list2.size());

    for (int i = 0; i < size; i++) {
      list2.set(i, longs[i] + 1);
    }

    // values in old list should not have changed
    assertEquals(size, list1.size());
    for (int i = 0; i < size; i++) {
      assertEquals(longs[i], list1.get(i));
    }

    // however, values in new list should have changed
    assertEquals(size, list1.size());
    for (int i = 0; i < size; i++) {
      assertEquals(longs[i] + 1, list2.get(i));
    }
  }

  @Test
  public void testToString1() {
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", new ArrayListOfLongs(1, 11)
        .toString());
    assertEquals("[1, 2, 3, 4, 5 ... (5 more) ]", new ArrayListOfLongs(1, 11)
        .toString(5));

    assertEquals("[1, 2, 3, 4, 5]", new ArrayListOfLongs(1, 6).toString());
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]", new ArrayListOfLongs(1,
        12).toString(11));

    assertEquals("[]", new ArrayListOfLongs().toString());
  }

  @Test
  public void testToString2() {
    int size = 10;
    Random r = new Random();

    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      list.add(r.nextLong());
    }

    String out = list.toString();
    for (int i = 0; i < size; i++) {
      long v = list.get(i);

      // Make sure the first 10 elements are printed out.
      assertTrue(out.indexOf(new Long(v).toString()) != -1);
    }

    for (int i = 0; i < size; i++) {
      list.add(r.nextInt(100000));
    }

    out = list.toString();
    for (int i = size; i < size + size; i++) {
      long v = list.get(i);

      // Make sure these elements are not printed out.
      assertTrue(out.indexOf(new Long(v).toString()) == -1);
    }

    assertTrue(out.indexOf(size + " more") != -1);
  }

  @Test
  public void testIterable() {
    int size = 1000;
    Random r = new Random();
    long[] longs = new long[size];

    ArrayListOfLongs list = new ArrayListOfLongs();
    for (int i = 0; i < size; i++) {
      long k = r.nextLong();
      list.add(k);
      longs[i] = k;
    }

    int i = 0;
    for (Long v : list) {
      assertEquals(longs[i++], (long) v);
    }

  }

  @Test
  public void testSetSize() {
    ArrayListOfLongs list = new ArrayListOfLongs();

    list.add(5L);
    assertEquals(1, list.size);
    assertEquals(5L, list.get(0));

    list.setSize(5);
    assertEquals(5L, list.size);
    assertEquals(0, list.get(1));
    assertEquals(0, list.get(2));
    assertEquals(0, list.get(3));
    assertEquals(0, list.get(4));

    list.add(12L);
    assertEquals(6, list.size);
    assertEquals(12L, list.get(5));
  }

  @Test
  public void testSort() {
    ArrayListOfLongs a = new ArrayListOfLongs();
    assertEquals(0, a.size());

    a.add(5).add(6).add(1).add(4);
    assertEquals(4, a.size());

    a.sort();
    assertEquals(4, a.size());

    assertEquals(1, a.get(0));
    assertEquals(4, a.get(1));
    assertEquals(5, a.get(2));
    assertEquals(6, a.get(3));
  }

  @Test
  public void testIntersection1() {
    ArrayListOfLongs a = new ArrayListOfLongs();
    a.add(5).add(3).add(1);

    a.sort();

    ArrayListOfLongs b = new ArrayListOfLongs();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfLongs c = a.intersection(b);

    assertEquals(1, c.get(0));
    assertEquals(3, c.get(1));
    assertEquals(2, c.size());
  }

  @Test
  public void testIntersection2() {
    ArrayListOfLongs a = new ArrayListOfLongs();
    a.add(5);

    ArrayListOfLongs b = new ArrayListOfLongs();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfLongs c = a.intersection(b);
    assertTrue(c.size() == 0);
  }

  @Test
  public void testIntersection3() {
    ArrayListOfLongs a = new ArrayListOfLongs();
    a.add(3).add(5).add(7).add(8).add(9);

    ArrayListOfLongs b = new ArrayListOfLongs();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfLongs c = a.intersection(b);

    assertEquals(3, c.get(0));
    assertEquals(1, c.size());
  }

  @Test
  public void testIntersection4() {
    ArrayListOfLongs a = new ArrayListOfLongs();
    a.add(3);

    ArrayListOfLongs b = new ArrayListOfLongs();
    b.add(0);

    ArrayListOfLongs c = a.intersection(b);

    assertEquals(0, c.size());
  }

  @Test
  public void testSubList() {
    ArrayListOfLongs a = new ArrayListOfLongs(new long[] { 1, 2, 3, 4, 5, 6, 7 });
    ArrayListOfLongs b = a.subList(1, 5);
    assertEquals(5, b.size());
    assertEquals(2, b.get(0));
    assertEquals(3, b.get(1));
    assertEquals(4, b.get(2));
    assertEquals(5, b.get(3));
    assertEquals(6, b.get(4));

    a.clear();
    // Make sure b is a new object.
    assertEquals(5, b.size());
    assertEquals(2, b.get(0));
    assertEquals(3, b.get(1));
    assertEquals(4, b.get(2));
    assertEquals(5, b.get(3));
    assertEquals(6, b.get(4));
  }

  @Test
  public void testAddUnique() {
    ArrayListOfLongs a = new ArrayListOfLongs(new long[] { 1, 2, 3, 4, 5, 6, 7 });
    a.addUnique(new long[] { 8, 0, 2, 5, -1, 11, 9 });
    assertEquals(12, a.size());
    assertEquals(0, a.get(8));
    assertEquals(-1, a.get(9));
    assertEquals(11, a.get(10));
    assertEquals(9, a.get(11));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ArrayListOfLongsTest.class);
  }
}