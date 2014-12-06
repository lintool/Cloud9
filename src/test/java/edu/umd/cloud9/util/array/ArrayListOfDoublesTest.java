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
import edu.umd.cloud9.util.array.ArrayListOfDoubles;

public class ArrayListOfDoublesTest {
  double neg_one=-1, zero=0, one=1, two=2, three=3, four=4, five=5, six=6, seven=7, nine=9;

  @Test
  public void testRemoveWithinBounds(){
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(one).add(three).add(five).add(seven);

    assertTrue(one == a.remove(0));

    assertTrue(three == a.get(0));
    assertTrue(five == a.get(1));

    assertTrue(five == a.remove(1));
    assertTrue(seven == a.get(2));  
  }

  @Test (expected=ArrayIndexOutOfBoundsException.class)
  public void testRemoveOutOfBounds(){
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(one).add(three).add(five).add(seven);

    a.remove(4);
  }

  @Test (expected=ArrayIndexOutOfBoundsException.class)
  public void testRemoveOutOfBounds2(){
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(neg_one);
    a.remove(-1);
  }

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
  public void testArrayConstructor() {
    double[] arr = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };
    assertEquals(5, arr.length);

    ArrayListOfDoubles list = new ArrayListOfDoubles(arr);
    list.remove(2);

    // Make sure the original array remains untouched.
    assertEquals(1.0, arr[0], 10e-6);
    assertEquals(2.0, arr[1], 10e-6);
    assertEquals(3.0, arr[2], 10e-6);
    assertEquals(4.0, arr[3], 10e-6);
    assertEquals(5.0, arr[4], 10e-6);
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

  @Test
  public void testSetSize() {
    ArrayListOfDoubles list = new ArrayListOfDoubles();

    list.add(5);
    assertEquals(1, list.size);
    assertEquals(5, list.get(0), 10e-6);

    list.setSize(5);
    assertEquals(5, list.size);
    assertEquals(0, list.get(1), 10e-6);
    assertEquals(0, list.get(2), 10e-6);
    assertEquals(0, list.get(3), 10e-6);
    assertEquals(0, list.get(4), 10e-6);

    list.add(12);
    assertEquals(6, list.size);
    assertEquals(12, list.get(5), 10e-6);
  }

  @Test
  public void testSort() {
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    assertEquals(0, a.size());

    a.add(5.2f).add(6f).add(5.9f).add(4.1f);
    assertEquals(4, a.size());

    a.sort();
    assertEquals(4, a.size());

    assertTrue(Math.abs(a.get(0)-4.1) < 0.0001);
    assertTrue(Math.abs(a.get(1)-5.2) < 0.0001);
    assertTrue(Math.abs(a.get(2)-5.9) < 0.0001);
    assertTrue(Math.abs(a.get(3)-6) < 0.0001);
  }

  @Test
  public void testIntersection1() {
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(5).add(3).add(1);

    a.sort();

    ArrayListOfDoubles b = new ArrayListOfDoubles();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfDoubles c = a.intersection(b);

    assertTrue(Math.abs(1 - c.get(0)) < 0.0001);
    assertTrue(Math.abs(3 - c.get(1)) < 0.0001);
    assertTrue(Math.abs(2 - c.size()) < 0.0001);
  }

  @Test
  public void testIntersection2() {
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(5);

    ArrayListOfDoubles b = new ArrayListOfDoubles();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfDoubles c = a.intersection(b);
    assertTrue(c.size() == 0);
  }

  @Test
  public void testIntersection3() {
    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(3).add(5).add(7).add(8).add(9);

    ArrayListOfDoubles b = new ArrayListOfDoubles();
    b.add(0).add(1).add(2).add(3);

    ArrayListOfDoubles c = a.intersection(b);

    assertTrue(Math.abs(3 - c.get(0)) < 0.0001);
    assertEquals(1, c.size());
  }

  @Test
  public void testMerge1() {
    //CASE: interleaved

    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(3);
    a.add(7);
    a.add(10);

    ArrayListOfDoubles b = new ArrayListOfDoubles();
    b.add(0);
    b.add(4);
    b.add(9);

    ArrayListOfDoubles c = a.merge(b);

    assertEquals(6, c.size());
    assertTrue(Math.abs(0 - c.get(0)) < 0.0001);
    assertTrue(Math.abs(3 - c.get(1)) < 0.0001);
    assertTrue(Math.abs(4 - c.get(2)) < 0.0001);
    assertTrue(Math.abs(7 - c.get(3)) < 0.0001);
    assertTrue(Math.abs(9 - c.get(4)) < 0.0001);
    assertTrue(Math.abs(10 - c.get(5)) < 0.0001);

    // c should be same as c2
    ArrayListOfDoubles c2 = b.merge(a); 
    assertEquals(c, c2);
  }

  @Test
  public void testMerge2() {
    //CASE: append

    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(3);
    a.add(7);
    a.add(10);

    ArrayListOfDoubles b = new ArrayListOfDoubles();
    b.add(11);
    b.add(19);
    b.add(21);

    ArrayListOfDoubles c = a.merge(b);

    assertEquals(6, c.size());
    assertTrue(Math.abs(3 - c.get(0)) < 0.0001);
    assertTrue(Math.abs(7 - c.get(1)) < 0.0001);
    assertTrue(Math.abs(10 - c.get(2)) < 0.0001);
    assertTrue(Math.abs(11 - c.get(3)) < 0.0001);
    assertTrue(Math.abs(19 - c.get(4)) < 0.0001);
    assertTrue(Math.abs(21 - c.get(5)) < 0.0001);

    ArrayListOfDoubles c2 = b.merge(a);
    assertEquals(c, c2);
  }

  @Test
  public void testMerge3() {
    //CASE: one of the lists are empty

    ArrayListOfDoubles a = new ArrayListOfDoubles();
    a.add(3);
    a.add(7);
    a.add(10);

    ArrayListOfDoubles b = new ArrayListOfDoubles();

    ArrayListOfDoubles c = a.merge(b);
    assertEquals(c, a);

    ArrayListOfDoubles c2 = b.merge(a);
    assertEquals(c, c2);   
  }

  @Test
  public void testSubList() {
    ArrayListOfDoubles a = new ArrayListOfDoubles(new double[] {1, 2, 3, 4, 5, 6, 7});
    ArrayListOfDoubles c = a.subList(1, 5);
    assertEquals(5, c.size());
    assertTrue(Math.abs(2 - c.get(0)) < 0.0001);
    assertTrue(Math.abs(3 - c.get(1)) < 0.0001);
    assertTrue(Math.abs(4 - c.get(2)) < 0.0001);
    assertTrue(Math.abs(5 - c.get(3)) < 0.0001);
    assertTrue(Math.abs(6 - c.get(4)) < 0.0001);

    a.clear();
    // Make sure b is a new object.
    assertEquals(5, c.size());
    assertTrue(Math.abs(2 - c.get(0)) < 0.0001);
    assertTrue(Math.abs(3 - c.get(1)) < 0.0001);
    assertTrue(Math.abs(4 - c.get(2)) < 0.0001);
    assertTrue(Math.abs(5 - c.get(3)) < 0.0001);
    assertTrue(Math.abs(6 - c.get(4)) < 0.0001);   
  }

  @Test
  public void testAddUnique() {
    ArrayListOfDoubles a = new ArrayListOfDoubles(new double[] {1, 2, 3, 4, 5, 6, 7});
    a.addUnique(new int[] {8, 0, 2, 5, -1, 11, 9});

    assertEquals(12, a.size());
    assertTrue(Math.abs(0 - a.get(8)) < 0.0001);
    assertTrue(Math.abs(-1 - a.get(9)) < 0.0001);
    assertTrue(Math.abs(11 - a.get(10)) < 0.0001);
    assertTrue(Math.abs(9 - a.get(11)) < 0.0001);
  }

  @Test
  public void testShift() {
    int size = 100;
    int shift = 10;

    ArrayListOfDoubles list = new ArrayListOfDoubles();
    for (int i = 0; i < size; i++)
      list.add(i);
    list.shiftLastNToTop(shift);

    for (int i = 0; i < list.size(); i++) {
      assertTrue(Math.abs(size - shift + i - list.get(i)) < 0.001);
    }
    list.add(size);
    assertEquals(shift + 1, list.size());
    assertTrue(Math.abs(size - list.get(shift)) < 0.001);

  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ArrayListOfDoublesTest.class);
  }
}