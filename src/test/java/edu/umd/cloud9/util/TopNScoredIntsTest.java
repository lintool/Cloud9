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
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.pair.PairOfIntFloat;

public class TopNScoredIntsTest {

  @Test
  public void testBasic1() {
    TopNScoredInts set = new TopNScoredInts(5);

    set.add(1, 0);
    set.add(2, 5);
    set.add(3, 4);
    set.add(4, 6);
    set.add(5, 1);
    set.add(6, 199);
    set.add(7, -31);

    PairOfIntFloat[] arr = set.extractAll();

    assertEquals(5, arr.length);
    assertEquals(6, arr[0].getLeftElement());
    assertEquals(199, arr[0].getRightElement(), 10e-6);
    assertEquals(4, arr[1].getLeftElement());
    assertEquals(6, arr[1].getRightElement(), 10e-6);
    assertEquals(2, arr[2].getLeftElement());
    assertEquals(5, arr[2].getRightElement(), 10e-6);
    assertEquals(3, arr[3].getLeftElement());
    assertEquals(4, arr[3].getRightElement(), 10e-6);
    assertEquals(5, arr[4].getLeftElement());
    assertEquals(1, arr[4].getRightElement(), 10e-6);
  }

  @Test
  public void testBasic2() {
    TopNScoredInts set = new TopNScoredInts(5);

    set.add(1, 5);
    set.add(2, 5);
    set.add(3, 4);
    set.add(4, 6);
    set.add(5, 1);
    set.add(6, 1);
    // 6 should get preserved.

    PairOfIntFloat[] arr = set.extractAll();

    assertEquals(5, arr.length);
    assertEquals(4, arr[0].getLeftElement());
    assertEquals(6, arr[0].getRightElement(), 10e-6);
    assertEquals(1, arr[1].getLeftElement());
    assertEquals(5, arr[1].getRightElement(), 10e-6);
    assertEquals(2, arr[2].getLeftElement());
    assertEquals(5, arr[2].getRightElement(), 10e-6);
    assertEquals(3, arr[3].getLeftElement());
    assertEquals(4, arr[3].getRightElement(), 10e-6);
    assertEquals(6, arr[4].getLeftElement());
    assertEquals(1, arr[4].getRightElement(), 10e-6);
  }

  @Test
  public void testBasic3() {
    TopNScoredInts set = new TopNScoredInts(5);
    // What if # objects is less than size of queue?
    set.add(1, 5);
    set.add(2, 4);
    set.add(3, 6);

    PairOfIntFloat[] arr = set.extractAll();

    assertEquals(3, arr.length);
    assertEquals(3, arr[0].getLeftElement());
    assertEquals(6, arr[0].getRightElement(), 10e-6);
    assertEquals(1, arr[1].getLeftElement());
    assertEquals(5, arr[1].getRightElement(), 10e-6);
    assertEquals(2, arr[2].getLeftElement());
    assertEquals(4, arr[2].getRightElement(), 10e-6);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TopNScoredIntsTest.class);
  }
}